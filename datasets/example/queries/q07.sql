SELECT COUNT(*)
FROM order_items i, products p
WHERE i.product_id = p.id
  AND (p.category = 'toys' OR p.category = 'garden')
  AND i.discount_code IS NULL;
