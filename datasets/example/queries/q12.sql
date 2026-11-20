SELECT MIN(o.id)
FROM orders o, order_items i, products p, reviews r
WHERE o.id = i.order_id
  AND i.product_id = p.id
  AND r.product_id = p.id
  AND r.rating = 1
  AND o.status = 'delivered';
