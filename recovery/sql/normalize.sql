WITH cleaned AS (
    SELECT source_record,
           upper(trim(order_id)) AS order_id, trim(line_id) AS line_id,
           upper(trim(customer_id)) AS customer_id,
           trim(order_date) AS raw_date, trim(quantity) AS raw_quantity,
           trim(unit_price) AS raw_price, upper(trim(currency)) AS currency,
           trim(updated_at) AS raw_updated_at,
           try_cast(trim(order_date) AS DATE) AS order_date,
           try_cast(trim(quantity) AS INT) AS quantity,
           try_cast(replace(trim(unit_price), ',', '.') AS DECIMAL(12,2)) AS unit_price,
           try_cast(trim(updated_at) AS TIMESTAMP) AS updated_at,
           to_json(struct(source_record, order_id, line_id, customer_id, order_date,
                          quantity, unit_price, currency, updated_at)) AS raw_json
    FROM canonical_source
), checked AS (
    SELECT c.*, d.customer_name,
           filter(array(
              CASE WHEN c.order_id IS NULL OR NOT c.order_id RLIKE '^[A-Z0-9][A-Z0-9_-]{0,39}$' THEN 'invalid_order_id' END,
              CASE WHEN c.line_id IS NULL OR NOT c.line_id RLIKE '^[1-9][0-9]{0,5}$' THEN 'invalid_line_id' END,
              CASE WHEN c.customer_id IS NULL OR NOT c.customer_id RLIKE '^[A-Z0-9][A-Z0-9_-]{0,39}$' THEN 'invalid_customer_id' END,
              CASE WHEN c.order_date IS NULL OR NOT c.raw_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 'invalid_date' END,
              CASE WHEN c.quantity IS NULL OR c.quantity < 1 OR c.quantity > 10000 OR NOT c.raw_quantity RLIKE '^[0-9]+$' THEN 'invalid_quantity' END,
              CASE WHEN c.unit_price IS NULL OR c.unit_price < 0 OR c.unit_price > 9999999.99 OR NOT c.raw_price RLIKE '^[0-9]+([.,][0-9]{1,2})?$' THEN 'invalid_price' END,
              CASE WHEN c.currency IS NULL OR c.currency <> 'BRL' THEN 'unsupported_currency' END,
              CASE WHEN c.updated_at IS NULL OR NOT c.raw_updated_at RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$' THEN 'invalid_update_timestamp' END,
              CASE WHEN d.customer_id IS NULL THEN 'missing_customer' END
           ), x -> x IS NOT NULL) AS errors,
           sha2(to_json(struct(c.order_id, c.line_id, c.customer_id, c.order_date,
                   c.quantity, c.unit_price, c.currency, c.updated_at)), 256) AS row_hash
    FROM cleaned c LEFT JOIN current_customers d ON c.customer_id = d.customer_id
), conflicts AS (
    SELECT order_id, line_id, updated_at, count(DISTINCT row_hash) AS versions
    FROM checked GROUP BY order_id, line_id, updated_at
), ranked AS (
    SELECT c.*, x.versions,
           max(CASE WHEN size(c.errors) > 0 OR x.versions > 1 THEN 1 ELSE 0 END)
               OVER (PARTITION BY c.order_id, c.line_id) AS blocked_key,
           row_number() OVER (PARTITION BY c.order_id, c.line_id, c.row_hash ORDER BY c.source_record) AS exact_rank,
           max(c.updated_at) OVER (PARTITION BY c.order_id, c.line_id) AS latest_update
    FROM checked c LEFT JOIN conflicts x
      ON c.order_id <=> x.order_id AND c.line_id <=> x.line_id AND c.updated_at <=> x.updated_at
)
SELECT *,
       CASE WHEN size(errors) > 0 OR versions > 1 OR blocked_key = 1 THEN 'rejected'
            WHEN exact_rank > 1 THEN 'duplicate'
            WHEN updated_at < latest_update THEN 'superseded'
            ELSE 'accepted' END AS disposition,
       CASE WHEN size(errors) > 0 THEN errors
            WHEN versions > 1 THEN array('conflicting_same_version')
            WHEN blocked_key = 1 THEN array('blocked_by_invalid_version')
            ELSE cast(array() AS ARRAY<STRING>) END AS reasons,
       cast(unit_price * 100 AS BIGINT) AS unit_price_minor,
       cast(unit_price * quantity * 100 AS BIGINT) AS gross_minor
FROM ranked
