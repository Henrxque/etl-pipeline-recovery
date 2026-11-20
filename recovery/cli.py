import argparse
import json
import sys
from pathlib import Path

from recovery.engine import Engine, export_run
from recovery.errors import RecoveryError
from recovery.source import FIXTURES
from recovery.store import Store


def main():
    parser = argparse.ArgumentParser(
        description="Reproduce, diagnose and recover a synthetic PySpark ETL incident."
    )
    parser.add_argument("--database", type=Path, default=Path("data/recovery.sqlite3"))
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ["broken", "diagnose", "recover"]:
        command = sub.add_parser(action)
        command.add_argument("--input-dir", type=Path, default=FIXTURES)
        command.add_argument("--partition", default="2026-11-18")
        command.add_argument("--output-dir", type=Path)
        if action == "broken":
            command.add_argument("--scenario", choices=["schema", "cast", "join"], default="schema")
        if action == "recover":
            command.add_argument("--strict", action="store_true")
    sub.add_parser("inspect")
    args = parser.parse_args()
    engine = None
    try:
        store = Store(args.database)
        if args.action == "inspect":
            print(json.dumps(store.summary(), indent=2))
            return
        engine = Engine(store)
        run = engine.run(
            args.action,
            args.partition,
            getattr(args, "scenario", "schema"),
            args.input_dir,
            getattr(args, "strict", False),
        )
        destination = args.output_dir or Path("artifacts") / run["run_id"]
        export_run(run, destination)
        print(
            json.dumps(
                {
                    "run_id": run["run_id"],
                    "state": run["state"],
                    "report": str(destination / "report.json"),
                    "stats": (run["report"].get("result") or {}).get("stats"),
                    "publication": run["report"].get("publication"),
                    "error": run["report"].get("error"),
                },
                indent=2,
            )
        )
        if not run["state"].startswith("succeeded"):
            sys.exit(1)
    except (RecoveryError, OSError) as exc:
        print(
            json.dumps(
                {"error": {"code": getattr(exc, "code", "command_failed"), "message": str(exc)}}
            ),
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        if engine:
            engine.close()


if __name__ == "__main__":
    main()
