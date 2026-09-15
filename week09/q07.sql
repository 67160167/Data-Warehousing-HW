-- q07.sql: หาจังหวัดที่ยอดรวมกันยายนเกิน 400 บาท เรียงยอดมากไปน้อย
SELECT
    province,
    SUM(amount) AS revenue
FROM sales
WHERE month = '2026-09'
GROUP BY province
HAVING SUM(amount) > 400
ORDER BY revenue DESC;
