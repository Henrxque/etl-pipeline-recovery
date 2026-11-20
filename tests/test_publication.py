import copy
import csv
import json
import sqlite3

import pytest

from recovery.engine import Engine, export_run
from recovery.errors import RecoveryError


def terminal_report(store, plan, partition="2026-11-18"):
    run_id = store.start("recover", partition)
    return {
        "run_id": run_id,
        "action": "recover",
        "partition_date": partition,
        "state": "succeeded_with_review",
        "result": copy.deepcopy(plan),
    }


def test_publish_and_replay_keep_partition_identity(store, golden_plan, oracle):
    first = store.finish(terminal_report(store, golden_plan), publish=True)
    before = store.summary()["published"]
    second = store.finish(terminal_report(store, golden_plan), publish=True)
    assert first["report"]["publication"]["action"] == "replaced_partition"
    assert second["report"]["publication"]["action"] == "unchanged"
    assert store.summary()["published"] == before
    assert len(store.summary()["lines"]) == 7
    assert second["report"]["publication"]["gross_minor"] == oracle["gross_minor"]
    with store.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0] == 16


def test_failed_gate_does_not_replace_known_good_partition(store, golden_plan):
    store.finish(terminal_report(store, golden_plan), publish=True)
    before = store.summary()["lines"]
    report = terminal_report(store, golden_plan)
    report["result"]["checks"].append({"name": "strict_zero_quarantine", "passed": False})
    with pytest.raises(RecoveryError, match="Failed validation"):
        store.finish(report, publish=True)
    report["state"] = "failed"
    store.finish(report, publish=False)
    assert store.summary()["lines"] == before


def test_mid_insert_database_error_rolls_back_entire_partition(store, golden_plan):
    store.finish(terminal_report(store, golden_plan), publish=True)
    before = store.summary()
    report = terminal_report(store, golden_plan)
    row = report["result"]["lines"][0]
    row["unit_price_minor"] += 10
    row["gross_minor"] += row["quantity"] * 10
    report["result"]["sales"][0]["gross_minor"] += row["quantity"] * 10
    with store.connect(write=True) as db:
        db.execute(
            "CREATE TRIGGER fail_mid_batch BEFORE INSERT ON order_lines WHEN NEW.order_id='A1003' BEGIN SELECT RAISE(ABORT,'injected write failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        store.finish(report, publish=True)
    after = store.summary()
    assert after["lines"] == before["lines"]
    assert after["published"] == before["published"]
    report["state"] = "failed"
    report["publication"] = None
    store.finish(report, publish=False)


def test_unrelated_partition_survives_recovery(store, golden_plan):
    other = copy.deepcopy(golden_plan)
    for row in other["lines"]:
        row["order_date"] = "2026-11-17"
    for row in other["sales"]:
        row["order_date"] = "2026-11-17"
    store.finish(terminal_report(store, other, "2026-11-17"), publish=True)
    store.finish(terminal_report(store, golden_plan), publish=True)
    with store.connect() as db:
        assert dict(
            db.execute("SELECT order_date,COUNT(*) FROM order_lines GROUP BY order_date").fetchall()
        ) == {"2026-11-17": 7, "2026-11-18": 7}


def test_target_corruption_is_not_misreported_as_an_unchanged_replay(store, golden_plan):
    store.finish(terminal_report(store, golden_plan), publish=True)
    with store.connect(write=True) as db:
        db.execute(
            "UPDATE order_lines SET unit_price_minor=unit_price_minor+100,gross_minor=gross_minor+quantity*100 WHERE order_id='A1001' AND line_id='1'"
        )
    repaired = store.finish(terminal_report(store, golden_plan), publish=True)
    assert repaired["report"]["publication"]["action"] == "replaced_partition"
    assert repaired["report"]["publication"]["gross_minor"] == 51120


def test_concurrent_claim_and_expired_worker_fencing(store):
    old = store.start("recover", "2026-11-18")
    with pytest.raises(RecoveryError, match="lease"):
        store.start("recover", "2026-11-18")
    with store.connect(write=True) as db:
        db.execute("UPDATE lease SET expires_at=0")
    new = store.start("diagnose", "2026-11-18")
    assert store.run(old)["state"] == "failed"
    with pytest.raises(RecoveryError, match="lease"):
        store.finish({"run_id": old, "state": "succeeded"})
    store.finish({"run_id": new, "state": "succeeded"})


def test_spark_startup_failure_is_persisted_without_fallback(store):
    def unavailable():
        raise RecoveryError("java_missing", "Java is unavailable.")

    run = Engine(store, unavailable).run()
    assert run["state"] == "failed"
    assert run["report"]["error"]["code"] == "java_missing"
    assert store.summary()["lines"] == []


def test_engine_real_recovery_replay_and_blocking_gate(engine, tmp_path):
    first = engine.run()
    assert first["state"] == "succeeded_with_review"
    assert first["report"]["publication"]["action"] == "replaced_partition"
    replay = engine.run()
    assert replay["report"]["publication"]["action"] == "unchanged"
    before = engine.store.summary()["published"]
    blocked = engine.run(strict=True)
    assert blocked["state"] == "failed"
    assert blocked["report"]["error"]["code"] == "quality_gate"
    assert engine.store.summary()["published"] == before
    export_run(replay, tmp_path / "exports")
    exported_csv = tmp_path / "exports/outcomes.csv"
    assert b"\r\r\n" not in exported_csv.read_bytes()
    with exported_csv.open(encoding="utf-8", newline="") as handle:
        assert len(list(csv.DictReader(handle))) == 17
    assert (
        json.loads((tmp_path / "exports/report.json").read_text(encoding="utf-8"))["publication"][
            "action"
        ]
        == "unchanged"
    )
    assert len(list((engine.store.path.parent / "input_snapshots").iterdir())) == 3


def test_broken_join_append_is_isolated_from_curated_output(engine):
    first = engine.run("broken", scenario="join")
    second = engine.run("broken", scenario="join")
    assert first["state"] == "failed_validation"
    assert (
        second["report"]["legacy"]["append_target_rows"]
        == 2 * first["report"]["legacy"]["joined_rows_this_run"]
    )
    assert engine.store.summary()["lines"] == []
