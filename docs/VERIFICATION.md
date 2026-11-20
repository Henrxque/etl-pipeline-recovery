# Verification record

**Date:** 20 November 2026. **Local environment:** Windows, Python 3.14.3, Eclipse Temurin Java 17.0.21.1, PySpark 4.3.0, Spark `local[2]`, ANSI mode enabled, session timezone UTC.

## Automated tests

`pytest -q`: **39 passed in 89.85 seconds** on the initial complete local run. The suite executes real Spark SQL rather than mocking the transformation engine. It includes the hand-authored acceptance oracle, actual legacy exceptions, source contracts, immutable input snapshots, customer/version conflicts, source-row accounting, strict blocking, unchanged replay, injected mid-insert rollback, unrelated partitions and expired worker fencing.

CI runs the suite and the full incident script on Linux with Java 17 and Python 3.11/3.14. Check the repository Actions run for its recorded outcome; local evidence files below are not fabricated CI output.

## Complete incident execution

The [manifest](evidence/manifest.json) records eight real runs:

| Case | Observed result |
| --- | --- |
| Schema baseline | Actual `UNRESOLVED_COLUMN.WITH_SUGGESTION` |
| Cast baseline | Actual `CAST_INVALID_INPUT` |
| Historical join | 17 source rows → 25 joined rows in an isolated legacy target |
| Legacy append replay | 50 accumulated legacy rows |
| Diagnosis | Two controlled aliases, unsafe direct casts, repeated keys and historical-customer cardinality |
| Recovery | 7 accepted, 8 rejected, 1 duplicate, 1 superseded; 6 orders; 51,120 BRL minor units |
| Corrected replay | Same published content hash; publication action `unchanged` |
| Strict gate | Failed zero-quarantine requirement; previous publication preserved |

The [expected-result file](../recovery/fixtures/expected.json) is hand-authored and is read by tests/verification only. The engine does not use it to construct accepted rows, amounts or its publication decision.

## HTTP checks

The local server is exercised through actual HTTP requests by [verify_http.py](../scripts/verify_http.py): asynchronous dispatch, persisted polling, concurrent request HTTP 409, diagnosis, recovery/replay, strict blocking, and saved JSON/CSV/log downloads reopened by the verifier. Its [recorded results](evidence/http-verification.json) are separate from the isolated CLI case.

## Desktop browser verification

The browser service was initially unavailable and became accessible during a follow-up on 20 November 2026. The actual desktop UI dispatched recovery run `5319238888` (prefix), displayed its eight passed checks and unchanged replay, opened the saved schema-failure report, and displayed accepted/rejected source-row outcomes. The full run identifier is recorded in [browser-capture.json](evidence/browser-capture.json). No browser console errors were observed.

Actual JPEG captures are in [images](images): [cover](images/01-cover.jpg), [original failure](images/02-original-failure.jpg), [validated recovery](images/03-recovery.jpg), and [row outcomes](images/04-row-outcomes.jpg). These capture the real application with synthetic fixture data; the displayed results were not inserted into the page for the screenshot.

**Responsive visual layout remains unverified.** The desktop browser check covers recovery dispatch and persisted-run/tab inspection; API download verification is documented separately above.

## Practical limits

The dataset is intentionally small and synthetic. No large-volume performance test, customer incident, financial recovery, measured downtime reduction, hosted uptime, cluster operation or production security claim is implied. Processing times include cold JVM startup where applicable and are observations on a development machine.
