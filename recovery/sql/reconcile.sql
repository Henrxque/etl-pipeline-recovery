SELECT count(*) AS source_rows,
       sum(CASE WHEN disposition = 'accepted' THEN 1 ELSE 0 END) AS accepted,
       sum(CASE WHEN disposition = 'rejected' THEN 1 ELSE 0 END) AS rejected,
       sum(CASE WHEN disposition = 'duplicate' THEN 1 ELSE 0 END) AS duplicates,
       sum(CASE WHEN disposition = 'superseded' THEN 1 ELSE 0 END) AS superseded
FROM recovery_ledger
