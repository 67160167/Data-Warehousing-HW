-- q06.sql: Dice กันยายน หมวด Drink และจังหวัด Bangkok หรือ Chonburi
SELECT
    month,
    category,
    province,
    SUM(amount) AS revenue
FROM sales
WHERE month = '2026-09'
  AND category = 'Drink'
  AND province IN ('Bangkok', 'Chonburi')
GROUP BY month, category, province
ORDER BY province;
