SELECT order_date, currency, count(*) AS line_count,
       count(DISTINCT order_id) AS order_count,
       sum(gross_minor) AS gross_minor
FROM accepted_lines GROUP BY order_date, currency ORDER BY order_date, currency
