-- Read-only queries for data/recovery.sqlite3.
SELECT run_id, action, scenario, state, started_at, finished_at FROM runs ORDER BY started_at DESC;

SELECT order_date, COUNT(*) AS lines, COUNT(DISTINCT order_id) AS orders,
       SUM(gross_minor) AS gross_minor FROM order_lines GROUP BY order_date;

SELECT * FROM daily_sales;
SELECT * FROM published_partitions;

SELECT run_id, source_record, reasons_json, raw_json FROM quarantine ORDER BY run_id,source_record;

SELECT COUNT(*) AS isolated_legacy_rows FROM legacy_lines;

SELECT run_id, stage, level, message, context_json FROM events ORDER BY event_id;

-- Replace :run_id with a parameter in your SQLite client.
SELECT json_extract(report_json,'$.result.stats') AS dispositions,
       json_extract(report_json,'$.publication') AS publication
FROM runs WHERE run_id=:run_id;
