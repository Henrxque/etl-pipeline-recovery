import json
from pathlib import Path

from recovery.errors import RecoveryError

SQL = Path(__file__).parent / "sql"


def prepare_customers(spark):
    spark.sql("""
      SELECT upper(trim(customer_id)) AS customer_id,
             trim(customer_name) AS customer_name,
             try_cast(trim(updated_at) AS TIMESTAMP) AS updated_at
      FROM source_customers
    """).createOrReplaceTempView("customer_versions")
    invalid = (
        spark.sql("""
      SELECT count(*) AS n FROM customer_versions
      WHERE customer_id IS NULL OR NOT customer_id RLIKE '^[A-Z0-9][A-Z0-9_-]{0,39}$'
         OR customer_name IS NULL OR length(customer_name)=0 OR updated_at IS NULL
    """)
        .first()
        .n
    )
    conflict = (
        spark.sql("""
      SELECT count(*) AS n FROM (
        SELECT customer_id,updated_at FROM customer_versions GROUP BY customer_id,updated_at
        HAVING count(DISTINCT customer_name)>1
      )
    """)
        .first()
        .n
    )
    if invalid or conflict:
        raise RecoveryError(
            "customer_contract",
            "Customer history contains invalid or conflicting versions; publication is blocked.",
        )
    spark.sql("""
      SELECT customer_id,customer_name,updated_at FROM (
        SELECT *,row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC,customer_name) AS rank
        FROM customer_versions
      ) WHERE rank=1
    """).createOrReplaceTempView("current_customers")


def diagnose(spark, profiles):
    history = spark.sql("""
      SELECT customer_id,count(*) AS versions FROM source_customers
      GROUP BY customer_id HAVING count(*)>1 ORDER BY customer_id
    """).collect()
    repeated = spark.sql("""
      SELECT order_id,line_id,count(*) AS rows FROM canonical_source
      GROUP BY order_id,line_id HAVING count(*)>1 ORDER BY order_id,line_id
    """).collect()
    invalid_casts = spark.sql("""
      SELECT source_record FROM canonical_source
      WHERE try_cast(unit_price AS DECIMAL(12,2)) IS NULL
         OR try_cast(quantity AS INT) IS NULL ORDER BY source_record
    """).collect()
    mapping = profiles["order_lines.csv"]["mapping"]
    return {
        "schema_aliases": [{"expected": k, "observed": v} for k, v in mapping.items() if k != v],
        "customer_history_keys_with_multiple_versions": [row.asDict() for row in history],
        "repeated_business_keys": [row.asDict() for row in repeated],
        "unsafe_direct_cast_records": [row.source_record for row in invalid_casts],
        "findings": [
            {
                "code": "schema_drift",
                "cause": "Legacy SQL expects unit_price/order_date; the source supplies price/ordered_at.",
                "fix": "Explicit, versioned aliases; reject missing, ambiguous or unexpected columns.",
            },
            {
                "code": "unsafe_cast",
                "cause": "Direct ANSI casts fail on malformed values and decimal-comma input.",
                "fix": "Documented decimal contract, try_cast, strict format/range checks and a review ledger.",
            },
            {
                "code": "join_fanout",
                "cause": "Joining all customer history versions multiplies order lines.",
                "fix": "Resolve one current customer per key before joining; assert output business-key uniqueness.",
            },
            {
                "code": "append_replay",
                "cause": "An append-only target accumulates the same snapshot on rerun.",
                "fix": "Validate first, publish one explicit partition transactionally, and skip an unchanged content hash.",
            },
        ],
    }


def recover(spark, partition: str, strict: bool = False):
    prepare_customers(spark)
    ledger = spark.sql((SQL / "normalize.sql").read_text(encoding="utf-8")).cache()
    try:
        ledger.createOrReplaceTempView("recovery_ledger")
        stats = spark.sql((SQL / "reconcile.sql").read_text(encoding="utf-8")).first().asDict()
        spark.sql("""
          SELECT source_record,order_id,line_id,customer_id,customer_name,order_date,
                 quantity,unit_price_minor,gross_minor,currency,updated_at
          FROM recovery_ledger WHERE disposition='accepted'
        """).createOrReplaceTempView("accepted_lines")
        lines = [
            row.asDict()
            for row in spark.sql("SELECT * FROM accepted_lines ORDER BY order_id,line_id").collect()
        ]
        sales = [
            row.asDict()
            for row in spark.sql((SQL / "daily_sales.sql").read_text(encoding="utf-8")).collect()
        ]
        outcomes = [
            row.asDict()
            for row in spark.sql("""
          SELECT source_record,order_id,line_id,disposition,reasons,raw_json
          FROM recovery_ledger ORDER BY source_record
        """).collect()
        ]
        checks = [
            {
                "name": "source_row_conservation",
                "passed": stats["source_rows"]
                == sum(stats[k] for k in ["accepted", "rejected", "duplicates", "superseded"]),
            },
            {"name": "nonempty_accepted_partition", "passed": len(lines) > 0},
            {
                "name": "unique_business_keys",
                "passed": len({(r["order_id"], r["line_id"]) for r in lines}) == len(lines),
            },
            {
                "name": "explicit_partition_scope",
                "passed": all(str(r["order_date"]) == partition for r in lines),
            },
            {
                "name": "line_to_aggregate_reconciliation",
                "passed": sum(r["gross_minor"] for r in lines)
                == sum(r["gross_minor"] for r in sales),
            },
            {"name": "all_rows_accounted", "passed": stats["source_rows"] == len(outcomes)},
        ]
        if strict:
            checks.append({"name": "strict_zero_quarantine", "passed": stats["rejected"] == 0})
        return json.loads(
            json.dumps(
                {
                    "stats": stats,
                    "lines": lines,
                    "sales": sales,
                    "outcomes": outcomes,
                    "checks": checks,
                },
                default=str,
            )
        )
    finally:
        ledger.unpersist()


def broken(spark, scenario: str):
    if scenario == "schema":
        return spark.sql(
            "SELECT order_id,line_id,unit_price,order_date FROM source_orders"
        ).collect()
    if scenario == "cast":
        return spark.sql(
            "SELECT sum(cast(unit_price AS DECIMAL(12,2))*cast(quantity AS INT)) AS gross FROM canonical_source"
        ).collect()
    # Reproduce permissive casts, historical fan-out, and an unfiltered target.
    return spark.sql("""
      SELECT o.source_record,o.order_id,o.line_id,o.customer_id,c.customer_name,
             try_cast(replace(o.unit_price,',','.') AS DOUBLE) AS unit_price,
             try_cast(o.quantity AS INT) AS quantity,o.order_date
      FROM canonical_source o JOIN source_customers c ON o.customer_id=c.customer_id
      ORDER BY o.source_record,c.updated_at
    """).collect()
