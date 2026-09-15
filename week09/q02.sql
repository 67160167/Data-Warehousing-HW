-- q02.sql: Roll-up ยอดขายรายเดือน เรียงตามเดือน
SELECT
    month,
    SUM(amount) AS revenue
FROM sales
GROUP BY month
ORDER BY month;
