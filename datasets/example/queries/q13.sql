-- reviews of 2024 with a low rating
SELECT p.category, COUNT(*)
FROM reviews r, products p
WHERE r.product_id = p.id
  AND r.created_at >= TIMESTAMP '2024-01-01 00:00:00'
  AND r.rating <= 2
  AND p.price BETWEEN 10 AND 50
GROUP BY p.category;
