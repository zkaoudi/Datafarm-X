-- price is a DECIMAL, which this version reads as text: that filter is reported and not used
SELECT MIN(c.name), MIN(p.name)
FROM customers c, orders o, order_items i, products p
WHERE c.id = o.customer_id
  AND o.id = i.order_id
  AND i.product_id = p.id
  AND p.price < 20
  AND c.signup_date BETWEEN DATE '2022-01-01' AND DATE '2022-12-31';
