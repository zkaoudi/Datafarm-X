SELECT c.name
FROM customers c
WHERE c.segment = 'business'
  AND c.name LIKE 'A%'
ORDER BY c.name;
