SELECT p.category, AVG(r.rating)
FROM reviews r, products p, customers c
WHERE r.product_id = p.id
  AND r.customer_id = c.id
  AND r.rating >= 4
GROUP BY p.category;
