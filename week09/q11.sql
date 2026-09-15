-- q11.sql: ตรวจสอบความถูกต้องก่อนและหลัง JOIN (จำนวนแถวและยอดรวม)
SELECT
    'fact_sales (before JOIN)' AS source,
    COUNT(*) AS row_count,
    SUM(quantity * unit_price) AS total_amount
FROM fact_sales
UNION ALL
SELECT
    'sales (after JOIN)' AS source,
    COUNT(*) AS row_count,
    SUM(amount) AS total_amount
FROM sales;
