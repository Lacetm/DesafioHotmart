-- GMV diário por subsidiária (registros correntes)
SELECT
    reference_date,
    subsidiary,
    gmv
FROM gmv_daily_snapshot
WHERE is_current = TRUE
ORDER BY reference_date, subsidiary;


-- GMV diário por subsidiária em uma data de snapshot específica
SELECT
    snapshot_date,
    reference_date,
    subsidiary,
    gmv
FROM gmv_daily_snapshot
WHERE is_current = TRUE
  AND snapshot_date = '2023-03-31'
ORDER BY reference_date, subsidiary;


-- GMV acumulado por subsidiária (corrente)
SELECT
    subsidiary,
    SUM(gmv) AS gmv_total
FROM gmv_daily_snapshot
WHERE is_current = TRUE
GROUP BY subsidiary
ORDER BY gmv_total DESC;