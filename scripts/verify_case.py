"""Execute the full incident and assert independent, observable acceptance criteria."""

import argparse
import json
from pathlib import Path
from uuid import uuid4

from recovery.engine import Engine, export_run
from recovery.source import FIXTURES
from recovery.store import Store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path)
    parser.add_argument("--output", type=Path, default=Path("artifacts/verified-case"))
    args = parser.parse_args()
    database = args.database or Path("tmp") / f"verified-{uuid4().hex[:8]}.sqlite3"
    if database.exists():
        raise SystemExit("Use a new database path to keep the baseline reproducible.")
    oracle = json.loads((FIXTURES / "expected.json").read_text(encoding="utf-8"))
    store = Store(database)
    engine = Engine(store)
    runs = {}
    try:
        for label, action, scenario, strict in [
            ("broken-schema", "broken", "schema", False),
            ("broken-cast", "broken", "cast", False),
            ("broken-join", "broken", "join", False),
            ("broken-join-replay", "broken", "join", False),
            ("diagnosis", "diagnose", "schema", False),
            ("recovered", "recover", "schema", False),
            ("replay", "recover", "schema", False),
            ("strict-blocked", "recover", "schema", True),
        ]:
            before = store.summary()["published"]
            run = engine.run(action, scenario=scenario, strict=strict)
            runs[label] = run
            export_run(run, args.output / label)
            if label == "strict-blocked":
                assert store.summary()["published"] == before, (
                    "Strict gate changed the published partition"
                )
            print(f"{label}: {run['state']}", flush=True)
        assert "UNRESOLVED_COLUMN" in runs["broken-schema"]["report"]["error"]["code"]
        assert "CAST_INVALID_INPUT" in runs["broken-cast"]["report"]["error"]["code"]
        joined = runs["broken-join"]["report"]["legacy"]["joined_rows_this_run"]
        assert joined > oracle["source_rows"]
        assert runs["broken-join-replay"]["report"]["legacy"]["append_target_rows"] == joined * 2
        recovered = runs["recovered"]["report"]
        assert recovered["publication"]["lines"] == oracle["accepted_lines"]
        assert recovered["publication"]["gross_minor"] == oracle["gross_minor"]
        assert {
            f"{row['order_id']}/{row['line_id']}": row["gross_minor"]
            for row in recovered["result"]["lines"]
        } == oracle["lines"]
        assert all(check["passed"] for check in recovered["result"]["checks"])
        assert runs["replay"]["report"]["publication"]["action"] == "unchanged"
        assert runs["strict-blocked"]["report"]["error"]["code"] == "quality_gate"
        manifest = {
            "data_scope": "Authored synthetic incident; real local Spark execution.",
            "runtime": recovered["runtime"],
            "cases": {
                name: {
                    "run_id": run["run_id"],
                    "state": run["state"],
                    "duration_ms": run["report"]["duration_ms"],
                }
                for name, run in runs.items()
            },
            "observed": {
                "source_rows": oracle["source_rows"],
                "legacy_joined_rows": joined,
                "legacy_rows_after_replay": joined * 2,
                "accepted_lines": recovered["publication"]["lines"],
                "orders": recovered["publication"]["orders"],
                "gross_minor": recovered["publication"]["gross_minor"],
                "currency": "BRL",
                "replay": "unchanged",
                "strict_gate_preserved_partition": True,
            },
        }
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        print("Verified full incident:", args.output / "manifest.json", flush=True)
    finally:
        engine.close()


if __name__ == "__main__":
    main()
