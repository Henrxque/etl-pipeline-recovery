# Local operating guide

## Start

Install the pinned dependencies, make Java 17+ available, and run the README server command on localhost port 8002. The first action initializes Spark. Cold startup took tens of seconds on the verification machine; the subsequent jobs reuse the JVM. There is no external cluster.

The server runs one local background job at a time. A persisted SQLite lease also serializes CLI/web runs that point at the same database. Configure the web database using `RECOVERY_DB_PATH`; set CLI `--database` explicitly to share a nondefault file. Avoid network filesystems for SQLite.

## Follow an incident

- Run the schema scenario first and inspect the actual Spark error.
- Run diagnosis to profile the source and connect each failure with its correction.
- Run recovery with the default quarantine policy; inspect all passed gates and row outcomes.
- Repeat recovery to verify `publication.action = unchanged`.
- Enable strict mode to require zero rejected rows. This fixture intentionally fails that gate and preserves the previously published partition.
- Inspect the CSV/JSON report and execution logs for each run. A completed-with-review run is not a zero-defect source.

The historical join scenario appends to `legacy_lines` only. Repeating it demonstrates the bad replay behavior without damaging `order_lines`.

## Failure states

| State or error | Meaning and action |
| --- | --- |
| `running` | Real local worker execution. Inspect persisted events; allow time for JVM startup. |
| `failed` with Spark error class | Query could not complete. Use the diagnostic action and source contract to identify the cause. |
| `failed_validation` | Isolated legacy query completed but produced unvalidated fan-out/append output. |
| `succeeded_with_review` | Valid lines published, rejected source rows retained with reasons. |
| `quality_gate` | Failed gate blocked publication; previous partition remains. |
| HTTP 409 / `job_active` | Another job owns the lease. Wait rather than dispatching overlapping work. |
| `java_missing` | Set `JAVA_HOME` to a valid JDK 17+ and retry. No non-Spark fallback is used. |
| `customer_contract` | Fix the invalid or conflicting customer-history versions before retrying. |
| `target_reconciliation` | Target rows/aggregates did not match the validated candidate; the publication transaction rolled back. Investigate target integrity. |
| `worker_lost` | A later claim detected an expired ten-minute worker lease and fenced the old run. |

An empty source cannot erase a published partition. Invalid versions block their entire line key. A different partition date does not authorize replacing unrelated dates. A table snapshot is authoritative only for the date explicitly selected.

## Stop, interruption and backup

Use Ctrl+C for a graceful server shutdown; the server waits for the active job before stopping Spark. If a worker is killed, the SQLite lease remains until expiry. After ten minutes, a new claim marks that run failed and fences its run ID from publication. The lease is not a job timeout or an automatic restart service.

Use SQLite's backup API for a consistent online copy; copying only the `.sqlite3` file while WAL is active may omit recent writes:

```python
import sqlite3
with sqlite3.connect("data/recovery.sqlite3") as source:
    with sqlite3.connect("tmp/recovery-backup.sqlite3") as target:
        source.backup(target)
```

Data, raw snapshots and operational artifacts are ignored by Git. Committed `docs/evidence` contains explicitly authored synthetic records. Runs, snapshots and quarantine have no automatic expiry; define retention before adapting to real data.

On Windows, Spark may warn that `winutils.exe` and native Hadoop libraries are absent. The verified CSV-read/Spark-SQL/SQLite path ran successfully using the built-in Java fallback; this project does not require downloading unofficial Hadoop binaries. The runner may also print process-cleanup messages when the local JVM exits. Check the actual command exit status and report state rather than treating all stderr output as a failed recovery.

## Inspection and verification

[inspection.sql](inspection.sql) provides read-only target queries. `scripts/verify_case.py` requires a fresh database and produces the full incident proof. `scripts/verify_http.py` requires the existing localhost server and dispatches real API jobs; it does not perform visual browser testing.

The API/CLI are local development tools without authentication. Do not expose this port publicly as a production service. Production storage outages may also prevent saving a terminal audit record; investigate storage health before replaying and preserve backups.
