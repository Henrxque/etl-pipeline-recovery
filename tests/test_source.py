import csv
import json

import pytest

from recovery.engine import report_csv, safe_cell, validate_partition
from recovery.errors import RecoveryError
from recovery.source import ORDER_COLUMNS, inspect_csv, snapshot_inputs


def rewrite(path, headers, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def test_controlled_aliases_and_snapshot_hash(inputs, tmp_path):
    frozen = snapshot_inputs(inputs, tmp_path / "frozen")
    profile = inspect_csv(frozen / "order_lines.csv", ORDER_COLUMNS)
    assert profile["rows"] == 17
    assert profile["mapping"]["unit_price"] == "price"
    assert profile["mapping"]["order_date"] == "ordered_at"
    (inputs / "order_lines.csv").write_text("changed after snapshot", encoding="utf-8")
    assert inspect_csv(frozen / "order_lines.csv", ORDER_COLUMNS)["sha256"] == profile["sha256"]


@pytest.mark.parametrize(
    "variant,code",
    [
        ("missing", "schema_contract"),
        ("ambiguous", "schema_contract"),
        ("unknown", "unexpected_columns"),
        ("duplicate_header", "duplicate_headers"),
        ("malformed", "malformed_csv"),
        ("empty", "empty_input"),
        ("duplicate_identity", "source_identity"),
        ("missing_identity", "source_identity"),
    ],
)
def test_bad_structure_is_rejected(inputs, variant, code):
    path = inputs / "order_lines.csv"
    with path.open(encoding="utf-8", newline="") as h:
        rows = list(csv.reader(h))
    headers, data = rows[0], rows[1:]
    if variant == "missing":
        headers[6] = "unknown_price"
    if variant == "ambiguous":
        headers.append("unit_price")
        data = [r + ["1.00"] for r in data]
    if variant == "unknown":
        headers.append("secret_extra")
        data = [r + ["value"] for r in data]
    if variant == "duplicate_header":
        headers[-1] = "order_id"
    if variant == "malformed":
        data[0] = data[0][:-1]
    if variant == "empty":
        data = []
    if variant == "duplicate_identity":
        data[1][0] = data[0][0]
    if variant == "missing_identity":
        data[0][0] = ""
    rewrite(path, headers, data)
    with pytest.raises(RecoveryError) as exc:
        inspect_csv(path, ORDER_COLUMNS)
    assert exc.value.code == code


def test_bounded_input_and_missing_file(inputs, tmp_path):
    path = inputs / "order_lines.csv"
    path.write_bytes(b"x" * 5_000_001)
    with pytest.raises(RecoveryError, match="5 MB"):
        snapshot_inputs(inputs, tmp_path / "big")
    with pytest.raises(RecoveryError, match="snapshot"):
        snapshot_inputs(tmp_path / "absent", tmp_path / "missing")


@pytest.mark.parametrize(
    "value", ["2026-02-30", "20261118", "18/11/2026", "2026-11-18T00:00:00Z", "", None]
)
def test_partition_is_explicit_and_strict(value):
    with pytest.raises(RecoveryError):
        validate_partition(value)


@pytest.mark.parametrize("value", ["=CMD()", " +SUM(A1:A2)", "\t@NOW()", "-1+1"])
def test_export_text_protects_spreadsheet_formulas(value):
    assert safe_cell(value).startswith("'")


def test_csv_report_can_be_reopened(golden_plan):
    text = report_csv({"run_id": "test-run", "result": golden_plan})
    rows = list(csv.DictReader(text.splitlines()))
    assert len(rows) == 17
    assert next(r for r in rows if r["Source record"] == "R12")["Reasons"] == "missing_customer"
    assert (
        json.loads(
            next(o for o in golden_plan["outcomes"] if o["source_record"] == "R06")["raw_json"]
        )["unit_price"]
        == "19,90"
    )
