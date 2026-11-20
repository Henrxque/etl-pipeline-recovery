import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from recovery.errors import RecoveryError


def now():
    return datetime.now(UTC).isoformat()


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class Store:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        setup = sqlite3.connect(self.path, timeout=30)
        try:
            setup.execute("PRAGMA journal_mode=WAL")
        finally:
            setup.close()
        with self.connect() as db:
            db.executescript("""
              CREATE TABLE IF NOT EXISTS runs(
                run_id TEXT PRIMARY KEY,action TEXT NOT NULL,scenario TEXT,partition_date TEXT NOT NULL,
                state TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT,report_json TEXT);
              CREATE TABLE IF NOT EXISTS lease(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                run_id TEXT NOT NULL REFERENCES runs(run_id),expires_at REAL NOT NULL);
              CREATE TABLE IF NOT EXISTS events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES runs(run_id),occurred_at TEXT NOT NULL,stage TEXT NOT NULL,
                level TEXT NOT NULL,message TEXT NOT NULL,context_json TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS order_lines(
                order_date TEXT NOT NULL,order_id TEXT NOT NULL,line_id TEXT NOT NULL,
                source_record TEXT NOT NULL,customer_id TEXT NOT NULL,customer_name TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK(quantity>0),unit_price_minor INTEGER NOT NULL CHECK(unit_price_minor>=0),
                gross_minor INTEGER NOT NULL CHECK(gross_minor=quantity*unit_price_minor),
                currency TEXT NOT NULL CHECK(currency='BRL'),updated_at TEXT NOT NULL,
                PRIMARY KEY(order_date,order_id,line_id));
              CREATE TABLE IF NOT EXISTS daily_sales(order_date TEXT NOT NULL,currency TEXT NOT NULL,
                line_count INTEGER NOT NULL,order_count INTEGER NOT NULL,gross_minor INTEGER NOT NULL,
                PRIMARY KEY(order_date,currency));
              CREATE TABLE IF NOT EXISTS published_partitions(order_date TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,run_id TEXT NOT NULL REFERENCES runs(run_id),published_at TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS quarantine(run_id TEXT NOT NULL REFERENCES runs(run_id),
                source_record TEXT NOT NULL,reasons_json TEXT NOT NULL,raw_json TEXT NOT NULL,
                PRIMARY KEY(run_id,source_record));
              CREATE TABLE IF NOT EXISTS legacy_lines(row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES runs(run_id),record_json TEXT NOT NULL);
            """)

    @contextmanager
    def connect(self, write=False):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def start(self, action, partition, scenario=None):
        run_id = uuid4().hex
        with self.connect(write=True) as db:
            active = db.execute("SELECT * FROM lease WHERE singleton=1").fetchone()
            if active and active["expires_at"] > time.time():
                raise RecoveryError(
                    "job_active", "An ETL run already owns the local execution lease."
                )
            if active:
                failed = {
                    "run_id": active["run_id"],
                    "state": "failed",
                    "error": {
                        "code": "worker_lost",
                        "message": "The worker lease expired before a terminal report was saved.",
                    },
                }
                db.execute(
                    "UPDATE runs SET state='failed',finished_at=?,report_json=? WHERE run_id=?",
                    (now(), json.dumps(failed), active["run_id"]),
                )
                db.execute("DELETE FROM lease")
            db.execute(
                "INSERT INTO runs VALUES(?,?,?,?,?,?,?,?)",
                (run_id, action, scenario, partition, "running", now(), None, None),
            )
            db.execute("INSERT INTO lease VALUES(1,?,?)", (run_id, time.time() + 600))
        self.event(
            run_id,
            "start",
            "Run claimed; published data remains unchanged until validation completes.",
            {"action": action, "scenario": scenario},
        )
        return run_id

    @staticmethod
    def owner(db, run_id):
        row = db.execute("SELECT run_id,expires_at FROM lease WHERE singleton=1").fetchone()
        if not row or row["run_id"] != run_id or row["expires_at"] < time.time():
            raise RecoveryError("lease_lost", "This worker no longer owns the execution lease.")

    def event(self, run_id, stage, message, context=None, level="info"):
        with self.connect(write=True) as db:
            db.execute(
                "INSERT INTO events(run_id,occurred_at,stage,level,message,context_json) VALUES(?,?,?,?,?,?)",
                (run_id, now(), stage, level, message, json.dumps(context or {})),
            )

    def legacy_append(self, run_id, rows):
        with self.connect(write=True) as db:
            self.owner(db, run_id)
            db.executemany(
                "INSERT INTO legacy_lines(run_id,record_json) VALUES(?,?)",
                [(run_id, json.dumps(row, default=str)) for row in rows],
            )
            return db.execute("SELECT COUNT(*) FROM legacy_lines").fetchone()[0]

    def finish(self, report: dict, publish=False):
        run_id = report["run_id"]
        with self.connect(write=True) as db:
            self.owner(db, run_id)
            if publish:
                result = report["result"]
                if not all(check["passed"] for check in result["checks"]):
                    raise RecoveryError("quality_gate", "Failed validation blocks publication.")
                lines = result["lines"]
                partition = report["partition_date"]
                if not lines or any(row["order_date"] != partition for row in lines):
                    raise RecoveryError(
                        "partition_scope",
                        "Only a nonempty, explicitly scoped partition can be published.",
                    )
                columns = [
                    "order_date",
                    "order_id",
                    "line_id",
                    "source_record",
                    "customer_id",
                    "customer_name",
                    "quantity",
                    "unit_price_minor",
                    "gross_minor",
                    "currency",
                    "updated_at",
                ]
                canonical = [{key: row[key] for key in columns} for row in lines]
                canonical.sort(key=lambda row: (row["order_id"], row["line_id"]))
                content_hash = digest(canonical)
                old = db.execute(
                    "SELECT content_hash FROM published_partitions WHERE order_date=?", (partition,)
                ).fetchone()
                current = [
                    dict(row)
                    for row in db.execute(
                        "SELECT * FROM order_lines WHERE order_date=? ORDER BY order_id,line_id",
                        (partition,),
                    )
                ]
                same = (
                    old is not None and old[0] == content_hash and digest(current) == content_hash
                )
                if not same:
                    db.execute("DELETE FROM order_lines WHERE order_date=?", (partition,))
                    db.execute("DELETE FROM daily_sales WHERE order_date=?", (partition,))
                    db.executemany(
                        f"INSERT INTO order_lines({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                        [tuple(row[key] for key in columns) for row in canonical],
                    )
                    db.executemany(
                        "INSERT INTO daily_sales VALUES(?,?,?,?,?)",
                        [
                            (
                                row["order_date"],
                                row["currency"],
                                row["line_count"],
                                row["order_count"],
                                row["gross_minor"],
                            )
                            for row in result["sales"]
                        ],
                    )
                    db.execute(
                        "INSERT INTO published_partitions VALUES(?,?,?,?) ON CONFLICT(order_date) DO UPDATE SET content_hash=excluded.content_hash,run_id=excluded.run_id,published_at=excluded.published_at",
                        (partition, content_hash, run_id, now()),
                    )
                # Independent SQL checks execute inside the same transaction as publication.
                actual = db.execute(
                    "SELECT count(*) AS lines,count(DISTINCT order_id) AS orders,coalesce(sum(gross_minor),0) AS gross FROM order_lines WHERE order_date=?",
                    (partition,),
                ).fetchone()
                aggregate = db.execute(
                    "SELECT coalesce(sum(line_count),0),coalesce(sum(order_count),0),coalesce(sum(gross_minor),0) FROM daily_sales WHERE order_date=?",
                    (partition,),
                ).fetchone()
                expected = (
                    len(lines),
                    len({row["order_id"] for row in lines}),
                    sum(row["gross_minor"] for row in lines),
                )
                if tuple(actual) != expected or tuple(aggregate) != expected:
                    raise RecoveryError(
                        "target_reconciliation",
                        "SQL target reconciliation failed. The transaction was rolled back.",
                    )
                result["checks"].append({"name": "sqlite_target_reconciliation", "passed": True})
                report["publication"] = {
                    "action": "unchanged" if same else "replaced_partition",
                    "content_hash": content_hash,
                    "lines": actual["lines"],
                    "orders": actual["orders"],
                    "gross_minor": actual["gross"],
                    "currency": "BRL",
                }
            for row in report.get("result", {}).get("outcomes", []):
                if row["disposition"] == "rejected":
                    db.execute(
                        "INSERT INTO quarantine VALUES(?,?,?,?)",
                        (run_id, row["source_record"], json.dumps(row["reasons"]), row["raw_json"]),
                    )
            report["finished_at"] = now()
            db.execute(
                "UPDATE runs SET state=?,finished_at=?,report_json=? WHERE run_id=?",
                (report["state"], report["finished_at"], json.dumps(report), run_id),
            )
            db.execute(
                "INSERT INTO events(run_id,occurred_at,stage,level,message,context_json) VALUES(?,?,?,?,?,?)",
                (
                    run_id,
                    now(),
                    "finish",
                    "info" if report["state"].startswith("succeeded") else "error",
                    "Terminal report saved; execution lease released.",
                    json.dumps(
                        {"state": report["state"], "publication": report.get("publication")}
                    ),
                ),
            )
            db.execute("DELETE FROM lease WHERE run_id=?", (run_id,))
        return self.run(run_id, detail=True)

    def run(self, run_id, detail=False):
        with self.connect() as db:
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                raise RecoveryError("run_not_found", "Run was not found.")
            run = dict(row)
            raw = run.pop("report_json")
            run["report"] = json.loads(raw) if raw else None
            if detail:
                run["events"] = [
                    {**dict(row), "context": json.loads(row["context_json"])}
                    for row in db.execute(
                        "SELECT * FROM events WHERE run_id=? ORDER BY event_id", (run_id,)
                    )
                ]
                for event in run["events"]:
                    event.pop("context_json")
        return run

    def summary(self):
        with self.connect() as db:
            ids = [
                row[0]
                for row in db.execute("SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 20")
            ]
            published = [
                dict(row)
                for row in db.execute(
                    "SELECT p.*,s.line_count,s.order_count,s.gross_minor,s.currency FROM published_partitions p JOIN daily_sales s USING(order_date) ORDER BY order_date DESC"
                )
            ]
            lines = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM order_lines ORDER BY order_date,order_id,line_id LIMIT 30"
                )
            ]
            legacy = db.execute("SELECT count(*) FROM legacy_lines").fetchone()[0]
        return {
            "runs": [self.run(run_id) for run_id in ids],
            "published": published,
            "lines": lines,
            "legacy_rows": legacy,
        }
