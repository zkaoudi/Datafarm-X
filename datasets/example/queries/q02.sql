SELECT MIN(p.name)
FROM products p, order_items i, orders o
WHERE p.id = i.product_id
  AND i.order_id = o.id
  AND p.category IN ('books', 'games')
  AND o.order_date >= DATE '2024-01-01';
