-- orders shipped per country
SELECT c.country, COUNT(*)
FROM customers c JOIN orders o ON o.customer_id = c.id
WHERE o.status = 'shipped'
GROUP BY c.country;
