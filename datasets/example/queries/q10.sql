SELECT MIN(r.body)
FROM reviews r
  JOIN customers c ON r.customer_id = c.id
  JOIN orders o ON o.customer_id = c.id
WHERE r.body LIKE '%great%'
  AND o.status <> 'cancelled';
