SELECT DISTINCT c.country
FROM customers c, orders o
WHERE c.id = o.customer_id
  AND o.status IN ('returned', 'cancelled');
