import csv
import io
import json
import time
from datetime import date
from pathlib import Path

from recovery.errors import RecoveryError
from recovery.source import FIXTURES, load_sources, snapshot_inputs
from recovery.store import Store
from recovery.transform import broken, diagnose, recover


def validate_partition(partition):
    try:
        parsed = date.fromisoformat(partition)
        if parsed.isoformat() != partition:
            raise ValueError("Use YYYY-MM-DD.")
    except (ValueError, TypeError) as exc:
        raise RecoveryError("invalid_partition", "Use one explicit YYYY-MM-DD partition.") from exc
    return partition


def safe_cell(value):
    text = "" if value is None else str(value)
    return "'" + text if text.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")) else text


def report_csv(report):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(["Run ID", "Source record", "Order", "Line", "Disposition", "Reasons"])
    for row in report.get("result", {}).get("outcomes", []):
        writer.writerow(
            [
                safe_cell(value)
                for value in [
                    report["run_id"],
                    row["source_record"],
                    row["order_id"],
                    row["line_id"],
                    row["disposition"],
                    "; ".join(row["reasons"]),
                ]
            ]
        )
    return stream.getvalue()


class Engine:
    def __init__(self, store: Store, spark_factory=None):
        self.store = store
        self._spark = None
        if spark_factory is None:
            from recovery.runtime import create_spark

            spark_factory = create_spark
        self.spark_factory = spark_factory

    @property
    def spark(self):
        if self._spark is None:
            self._spark = self.spark_factory()
        return self._spark

    def close(self):
        if self._spark is not None:
            self._spark.stop()

    def run(
        self,
        action="recover",
        partition="2026-11-18",
        scenario="schema",
        directory=FIXTURES,
        strict=False,
    ):
        validate_partition(partition)
        if action not in {"broken", "diagnose", "recover"} or scenario not in {
            "schema",
            "cast",
            "join",
        }:
            raise RecoveryError("invalid_action", "Choose a supported lab action and scenario.")
        run_id = self.store.start(action, partition, scenario if action == "broken" else None)
        return self.execute(run_id, Path(directory), strict)

    def execute(self, run_id, directory=FIXTURES, strict=False):
        job = self.store.run(run_id)
        started = time.perf_counter()
        report = {
            "schema_version": "1.0",
            "run_id": run_id,
            "action": job["action"],
            "scenario": job["scenario"],
            "partition_date": job["partition_date"],
            "started_at": job["started_at"],
            "data_scope": "Authored synthetic ETL incident. Not a customer outage or production recovery.",
            "contract_version": "1.0",
            "strict": strict,
            "state": "failed",
            "publication": None,
        }
        publish = False
        try:
            self.store.event(
                run_id,
                "runtime",
                "Starting the local Spark session; native Spark SQL expressions perform the transformation.",
            )
            spark = self.spark
            report["runtime"] = {
                "spark_version": spark.version,
                "master": spark.sparkContext.master,
                "timezone": spark.conf.get("spark.sql.session.timeZone"),
                "ansi": spark.conf.get("spark.sql.ansi.enabled"),
            }
            frozen = snapshot_inputs(
                Path(directory), self.store.path.parent / "input_snapshots" / run_id
            )
            report["inputs"] = load_sources(spark, frozen)
            self.store.event(
                run_id,
                "extract",
                "CSV structure checked and explicit string schemas loaded.",
                {name: profile["rows"] for name, profile in report["inputs"].items()},
            )
            if job["action"] == "broken":
                rows = broken(spark, job["scenario"])
                if job["scenario"] == "join":
                    total = self.store.legacy_append(run_id, [row.asDict() for row in rows])
                    report["legacy"] = {
                        "joined_rows_this_run": len(rows),
                        "source_rows": report["inputs"]["order_lines.csv"]["rows"],
                        "append_target_rows": total,
                        "target": "isolated legacy_lines; curated order_lines untouched",
                    }
                    report["error"] = {
                        "code": "legacy_quality_failure",
                        "message": "Historical join fan-out and append replay produce an unvalidated legacy target.",
                    }
                    report["state"] = "failed_validation"
                else:
                    report["error"] = {
                        "code": "failure_not_reproduced",
                        "message": "The chosen input did not reproduce the expected legacy exception. No curated publication was attempted.",
                    }
            else:
                report["diagnosis"] = diagnose(spark, report["inputs"])
                self.store.event(
                    run_id,
                    "diagnose",
                    "Schema aliases, unsafe casts and join cardinality profiled using Spark SQL.",
                )
                if job["action"] == "diagnose":
                    report["state"] = "succeeded"
                else:
                    report["result"] = recover(spark, job["partition_date"], strict)
                    report["result"]["checks"].append(
                        {
                            "name": "snapshot_row_count",
                            "passed": report["result"]["stats"]["source_rows"]
                            == report["inputs"]["order_lines.csv"]["rows"],
                        }
                    )
                    self.store.event(
                        run_id,
                        "validate",
                        "Every source row classified; partition and totals checked.",
                        report["result"]["stats"],
                    )
                    if not all(check["passed"] for check in report["result"]["checks"]):
                        report["error"] = {
                            "code": "quality_gate",
                            "message": "Validation failed. The previous published partition is preserved.",
                        }
                    else:
                        publish = True
                        report["state"] = (
                            "succeeded_with_review"
                            if report["result"]["stats"]["rejected"]
                            else "succeeded"
                        )
        except Exception as exc:
            code = getattr(exc, "code", None)
            condition = getattr(exc, "getCondition", None)
            if not code and callable(condition):
                code = condition()
            report["error"] = {
                "code": code or type(exc).__name__,
                "type": type(exc).__name__,
                "message": str(exc)[:1600],
            }
            self.store.event(
                run_id,
                "error",
                "Execution failed before curated publication.",
                {"code": report["error"]["code"]},
                level="error",
            )
        report["duration_ms"] = round((time.perf_counter() - started) * 1000)
        try:
            return self.store.finish(report, publish=publish)
        except Exception as exc:
            report["state"] = "failed"
            report["publication"] = None
            report["error"] = {
                "code": getattr(exc, "code", "publish_failed"),
                "message": str(exc)[:800],
            }
            return self.store.finish(report, publish=False)


def export_run(run: dict, directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    payloads = {
        "report.json": json.dumps(run["report"], indent=2, ensure_ascii=False),
        "logs.json": json.dumps(run["events"], indent=2, ensure_ascii=False),
        "outcomes.csv": report_csv(run["report"]),
    }
    for name, text in payloads.items():
        target = directory / name
        temp = target.with_suffix(target.suffix + ".tmp")
        temp.write_text(text if text.endswith("\n") else text + "\n", encoding="utf-8", newline="")
        temp.replace(target)
