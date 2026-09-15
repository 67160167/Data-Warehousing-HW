-- q09.sql: ยอดขายรายเดือนพร้อมแถวรวม ALL โดยใช้ UNION ALL
SELECT
    month,
    SUM(amount) AS revenue
FROM sales
GROUP BY month
UNION ALL
SELECT
    'ALL' AS month,
    SUM(amount) AS revenue
FROM sales;
