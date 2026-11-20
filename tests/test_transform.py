import csv

import pytest
from pyspark.sql.types import DecimalType

from recovery.errors import RecoveryError
from recovery.source import FIXTURES, load_sources
from recovery.transform import broken, diagnose, recover


def change_rows(directory, filename, modify):
    path = directory / filename
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames
        rows = list(reader)
    rows = modify(rows)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, headers)
        writer.writeheader()
        writer.writerows(rows)


def test_recovery_matches_hand_authored_business_oracle(golden_plan, oracle):
    assert golden_plan["stats"] == {
        "source_rows": 17,
        "accepted": 7,
        "rejected": 8,
        "duplicates": 1,
        "superseded": 1,
    }
    assert {
        f"{r['order_id']}/{r['line_id']}": r["gross_minor"] for r in golden_plan["lines"]
    } == oracle["lines"]
    assert golden_plan["sales"] == [
        {
            "order_date": oracle["order_date"],
            "currency": oracle["currency"],
            "line_count": 7,
            "order_count": 6,
            "gross_minor": oracle["gross_minor"],
        }
    ]
    assert all(c["passed"] for c in golden_plan["checks"])
    assert all(not r["customer_name"].endswith("Old") for r in golden_plan["lines"])
    assert (
        next(r for r in golden_plan["lines"] if r["order_id"] == "A1002")["unit_price_minor"]
        == 8490
    )


def test_ledger_preserves_duplicates_bad_values_and_conflicts(golden_plan):
    ledger = {r["source_record"]: r for r in golden_plan["outcomes"]}
    assert ledger["R02"]["disposition"] == "duplicate"
    assert ledger["R04"]["disposition"] == "superseded"
    expected = {
        "R10": "invalid_price",
        "R11": "invalid_quantity",
        "R12": "missing_customer",
        "R13": "invalid_date",
        "R14": "invalid_quantity",
        "R15": "invalid_price",
        "R16": "conflicting_same_version",
        "R17": "conflicting_same_version",
    }
    for row, reason in expected.items():
        assert ledger[row]["disposition"] == "rejected"
        assert reason in ledger[row]["reasons"]


def test_diagnosis_profiles_real_loaded_data(spark):
    profile = load_sources(spark, FIXTURES)
    result = diagnose(spark, profile)
    assert result["schema_aliases"] == [
        {"expected": "order_date", "observed": "ordered_at"},
        {"expected": "unit_price", "observed": "price"},
    ]
    assert {r["customer_id"] for r in result["customer_history_keys_with_multiple_versions"]} == {
        "C001",
        "C003",
    }
    assert set(result["unsafe_direct_cast_records"]) == {"R06", "R10", "R11"}
    assert {r["order_id"] for r in result["repeated_business_keys"]} == {"A1001", "A1002", "A1013"}


@pytest.mark.parametrize(
    "scenario,condition", [("schema", "UNRESOLVED_COLUMN"), ("cast", "CAST_INVALID_INPUT")]
)
def test_legacy_query_reproduces_actual_spark_error(spark, scenario, condition):
    load_sources(spark, FIXTURES)
    with pytest.raises(Exception) as exc:
        broken(spark, scenario)
    assert condition in str(exc.value)


def test_legacy_join_has_observable_fanout(spark):
    load_sources(spark, FIXTURES)
    rows = broken(spark, "join")
    assert len(rows) > 17
    assert len({(r.order_id, r.line_id) for r in rows}) < len(rows)


def test_invalid_newer_version_cannot_resurrect_an_old_value(spark, inputs):
    def mutate(rows):
        next(r for r in rows if r["source_record"] == "R05")["price"] = "BAD"
        return rows

    change_rows(inputs, "order_lines.csv", mutate)
    load_sources(spark, inputs)
    plan = recover(spark, "2026-11-18")
    assert not any(r["order_id"] == "A1002" for r in plan["lines"])
    old = next(r for r in plan["outcomes"] if r["source_record"] == "R04")
    assert old["disposition"] == "rejected" and old["reasons"] == ["blocked_by_invalid_version"]
    assert (
        sum(plan["stats"][key] for key in ["accepted", "rejected", "duplicates", "superseded"])
        == 17
    )


def test_customer_conflict_blocks_publication(spark, inputs):
    def mutate(rows):
        rows.append({**rows[-1], "customer_name": "Conflicting customer"})
        return rows

    change_rows(inputs, "customer_history.csv", mutate)
    load_sources(spark, inputs)
    with pytest.raises(RecoveryError, match="conflicting"):
        recover(spark, "2026-11-18")


def test_unscoped_date_and_strict_review_gate_fail(spark):
    load_sources(spark, FIXTURES)
    plan = recover(spark, "2026-11-19", strict=True)
    checks = {c["name"]: c["passed"] for c in plan["checks"]}
    assert checks["explicit_partition_scope"] is False
    assert checks["strict_zero_quarantine"] is False
    assert spark.table("recovery_ledger").schema["unit_price"].dataType == DecimalType(12, 2)
