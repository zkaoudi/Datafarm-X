SELECT MAX(o.order_date)
FROM orders o, customers c
WHERE o.customer_id = c.id
  AND c.country = 'DK'
  AND o.note IS NOT NULL;
