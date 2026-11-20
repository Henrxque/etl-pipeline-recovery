# Incident report: ETL pipeline recovery

**Independent synthetic incident · Henrique Ramos · 20 November 2026**

## Problem

A commerce back-office ETL was deliberately written with brittle assumptions: fixed column names, direct casts, an unqualified customer-history join and append-only output. The task was to reproduce the defects, identify their causes, implement the correction, and verify the published result.

This is an authored lab, not a real customer incident. The fixtures are small enough for a reviewer to audit every source row independently.

## Failure evidence

1. **Schema:** `unit_price` and `order_date` do not exist in the evolved source. Executing the legacy query produces `UNRESOLVED_COLUMN.WITH_SUGGESTION`. [Actual report](evidence/broken-schema/report.json).
2. **Cast:** after controlled aliases make the old field names accessible, direct ANSI decimal conversion fails on `19,90`; other rows also contain malformed quantity/price data. The actual error is `CAST_INVALID_INPUT`. [Actual report](evidence/broken-cast/report.json).
3. **Join:** a permissive legacy query joins every historical customer version. It emits 25 rows from 17 input records and drops the unknown customer through its inner join. This is not a valid grain-preserving transform. [Actual report](evidence/broken-join/report.json).
4. **Replay:** append writes add another 25 rows, leaving 50 in the isolated legacy table. [Actual replay report](evidence/broken-join-replay/report.json).

Schema/cast scenarios stop before publication. Join scenarios intentionally write only to `legacy_lines`, separate from curated `order_lines`.

## Diagnosis and correction

The [diagnostic report](evidence/diagnosis/report.json) profiles loaded source columns, direct-cast failures, repeated line keys and multi-version customer keys. The correction uses explicit alias rules and rejects ambiguous or unknown schemas. Input files are frozen and hashed before Spark's lazy reads.

Prices follow a declared contract: dot or comma decimal separator, at most two decimal places, no thousands separator, nonnegative and bounded. Malformed or unsupported values are quarantined rather than silently rounded into valid output. Spark uses decimal expressions and SQL window functions; no pandas replacement or Python UDF is involved.

Customer history is resolved to one current version per ID before joining. Conflicting same-time customer versions block publication. Order-line versions are classified deterministically: exact repeats are duplicates, older valid versions are superseded, and conflicting same-version rows are rejected. An invalid version blocks its whole order-line key so an older value cannot be silently resurrected.

All 17 source records remain represented in a disposition ledger. The accepted set has seven lines, six orders and BRL 511.20 gross fixture value. The exact accepted amounts are independently specified in the hand-authored test oracle; the engine does not use that file to generate output.

## Publication and recovery proof

Publication happens after validation, inside one SQLite transaction containing the selected partition, its aggregate, quarantine audit and terminal report. Independent SQL queries compare target line count, distinct orders and gross total with Spark output. A failed transaction restores the previous state.

The [recovered run](evidence/recovered/report.json) passed eight gates. Its [replay](evidence/replay/report.json) left the content hash and publication identity unchanged. A subsequent [strict run](evidence/strict-blocked/report.json) required zero rejected rows, failed as expected and preserved the previously validated partition.

Automated tests inject a mid-insert database error to verify actual transaction rollback. They also verify unrelated-partition preservation, stale-worker fencing, invalid-source contracts and blocked older-version fallback.

## Business relevance and boundaries

This case demonstrates a repair process a client can review: reproduce, diagnose, correct, reconcile and protect the last good output. There is no claim of measured downtime reduction, client ROI, production-store recovery or large-scale Spark performance. The output is an authoritative snapshot of one partition, not incremental CDC.

The local monitor and CLI expose the evidence. Cloud deployment, authentication, a production scheduler and real source access are outside the demonstration. Desktop recovery dispatch, report inspection and outcome tabs were verified and [captured](images); actual HTTP dispatch, polling, downloads and data results were checked separately. Responsive layout remains unverified.
