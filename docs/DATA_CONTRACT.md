# Data contract v1.0

The local adapter accepts UTF-8 CSV input, at most **5 MB and 5,000 records per file**. Both files must be nonempty. Unknown/duplicate headers, missing required fields, ambiguous aliases, malformed field counts and duplicate source-row identities block the job. Source bytes are frozen once per run and their hashes are stored in its report.

## Order lines

| Canonical field | Accepted source header | Rule |
| --- | --- | --- |
| `source_record` | same | Unique, nonempty provenance ID within the source file |
| `order_id` | same | Trimmed, uppercase; 1–40 letters/digits/underscore/hyphen, starts alphanumeric |
| `line_id` | same | Positive 1–6 digit string; no leading zero |
| `customer_id` | same | Same ID format as order; reference must exist in current customer lookup |
| `order_date` | `order_date` OR `ordered_at` | Exact `YYYY-MM-DD`, valid date |
| `quantity` | same | Digit-only integer, 1–10,000 |
| `unit_price` | `unit_price` OR `price` | Nonnegative, up to 9,999,999.99; dot or comma decimal separator; 0–2 fractional digits; no grouping separators |
| `currency` | same | Trimmed/uppercase `BRL`, explicitly provided by the fixture |
| `updated_at` | same | Valid UTC `YYYY-MM-DDTHH:MM:SSZ` timestamp |

Both alias columns being present is an error; the engine does not choose one silently. Parsing starts from explicit string schemas rather than inferred numeric types. `try_cast` permits classification, while format/range rules decide acceptance. Casting a three-decimal number to Decimal does not make it valid: the lexical check still rejects it.

Spark computes `DECIMAL(12,2)` amounts, then stores integer hundredths. `gross_minor = quantity × unit_price_minor`. There is no currency inference or FX conversion.

## Customer history

Columns: `customer_id`, `customer_name`, `updated_at`. IDs follow the trimmed uppercase ID rule. Names must be nonempty; timestamps must be parseable and are interpreted in the Spark session's explicit UTC timezone. Inputs should supply UTC timestamps. For each ID, the newest timestamp provides the current customer. Conflicting names at the same ID/timestamp block the catalogue. Identical historical copies are harmless because one current row is selected before joining.

## Business grain and dispositions

The selected output grain is `(order_date, order_id, line_id)`. Input version selection uses `(order_id, line_id)` and requires the resulting accepted set to match one explicit selected date. The source is an authoritative snapshot for that partition; multi-date accepted inputs fail the scope gate rather than overwriting several dates.

Canonical row hashes include business values and source update time; source provenance ID is excluded from duplicate detection. Conflicting normalized values at the same key/update time are rejected. Exact repeated rows are marked `duplicate`; older versions are `superseded`. If any version of a key is invalid or conflicting, otherwise-valid versions of that key are `blocked_by_invalid_version`. This conservative policy avoids falling back to an older value after a newer record becomes invalid.

Every source row has one disposition: `accepted`, `rejected`, `duplicate` or `superseded`. Rejected rows retain original JSON and reason codes. The default demo permits explicit quarantine and reports `succeeded_with_review`; strict mode additionally requires zero rejected rows.

## Publication gates

1. Source-row conservation across all four dispositions.
2. At least one accepted row.
3. Unique accepted order/line keys.
4. Every accepted date matches the requested partition.
5. Spark line-to-aggregate amount equality.
6. Complete row outcome accounting.
7. Spark row count matches the frozen CSV snapshot.
8. SQLite line count, distinct orders and amount match Spark output and the saved aggregate inside the publication transaction.

Strict mode adds a zero-quarantine gate. A failure saves an inspectable failure report when storage is available and does not replace the previous curated partition.
