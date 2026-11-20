import csv
import hashlib
from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType

from recovery.errors import RecoveryError

FIXTURES = Path(__file__).parent / "fixtures"
ORDER_COLUMNS = {
    "source_record": ["source_record"],
    "order_id": ["order_id"],
    "line_id": ["line_id"],
    "customer_id": ["customer_id"],
    "order_date": ["order_date", "ordered_at"],
    "quantity": ["quantity"],
    "unit_price": ["unit_price", "price"],
    "currency": ["currency"],
    "updated_at": ["updated_at"],
}
CUSTOMER_COLUMNS = {key: [key] for key in ["customer_id", "customer_name", "updated_at"]}


def snapshot_inputs(directory: Path, destination: Path):
    """Freeze the bytes once before Spark's lazy evaluation reads any source files."""
    destination.mkdir(parents=True, exist_ok=False)
    try:
        for name in ["order_lines.csv", "customer_history.csv"]:
            with (directory / name).open("rb") as handle:
                payload = handle.read(5_000_001)
            if len(payload) > 5_000_000:
                raise RecoveryError(
                    "input_too_large", "Local demo inputs are limited to 5 MB per file."
                )
            (destination / name).write_bytes(payload)
    except OSError as exc:
        raise RecoveryError(
            "input_unreadable", "Could not snapshot both required source CSV files."
        ) from exc
    return destination


def inspect_csv(path: Path, required: dict) -> dict:
    try:
        if path.stat().st_size > 5_000_000:
            raise RecoveryError(
                "input_too_large", "Local demo inputs are limited to 5 MB per file."
            )
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, strict=True)
            headers = next(reader, [])
            if len(headers) != len(set(headers)):
                raise RecoveryError("duplicate_headers", "CSV column names must be unique.")
            mapping = {}
            for canonical, aliases in required.items():
                found = [name for name in aliases if name in headers]
                if len(found) != 1:
                    raise RecoveryError(
                        "schema_contract", f"Expected exactly one of {aliases}; found {found}."
                    )
                mapping[canonical] = found[0]
            # Only declared fields are allowed: never interpolate unknown headers into SQL.
            known = {alias for aliases in required.values() for alias in aliases}
            if set(headers) - known:
                raise RecoveryError(
                    "unexpected_columns", "The CSV has columns outside the declared contract."
                )
            count = 0
            identifiers = set()
            for row in reader:
                count += 1
                if len(row) != len(headers):
                    raise RecoveryError(
                        "malformed_csv", f"CSV record {count} has the wrong field count."
                    )
                if "source_record" in mapping:
                    key = row[headers.index("source_record")].strip()
                    if not key or key in identifiers:
                        raise RecoveryError(
                            "source_identity", "Every source_record must be nonempty and unique."
                        )
                    identifiers.add(key)
                if count > 5000:
                    raise RecoveryError(
                        "input_too_large",
                        "This local SQLite adapter accepts at most 5000 records per input.",
                    )
            if count == 0:
                raise RecoveryError(
                    "empty_input", "An empty input cannot replace a valid published partition."
                )
    except RecoveryError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise RecoveryError(
            "input_unreadable", "Could not read the expected UTF-8 CSV input."
        ) from exc
    return {
        "file": path.name,
        "headers": headers,
        "mapping": mapping,
        "rows": count,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def load_sources(spark, directory: Path):
    profiles = {}
    for name, contract, view in [
        ("order_lines.csv", ORDER_COLUMNS, "source_orders"),
        ("customer_history.csv", CUSTOMER_COLUMNS, "source_customers"),
    ]:
        path = directory / name
        profile = inspect_csv(path, contract)
        schema = StructType([StructField(key, StringType(), True) for key in profile["headers"]])
        frame = (
            spark.read.schema(schema)
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(str(path.resolve()))
        )
        frame.createOrReplaceTempView(view)
        profiles[name] = profile
    mapping = profiles["order_lines.csv"]["mapping"]
    columns = ", ".join(f"`{actual}` AS `{canonical}`" for canonical, actual in mapping.items())
    spark.sql(f"SELECT {columns} FROM source_orders").createOrReplaceTempView("canonical_source")
    return profiles
