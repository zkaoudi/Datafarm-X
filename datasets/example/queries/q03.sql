SELECT o.id, SUM(i.quantity)
FROM orders o, order_items i
WHERE i.order_id = o.id
  AND i.quantity > 3
GROUP BY o.id
ORDER BY o.id;
