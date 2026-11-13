-- Solo lectura. Ejecutar en connexa_platform_test.
-- No modifica productos, barcodes ni relaciones.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '10s';
SET LOCAL lock_timeout = '1s';

SELECT current_database(), current_user, clock_timestamp();

SELECT id, ext_code
FROM inventory.inv_product
WHERE ext_code IS NULL OR ext_code ~ '^[[:space:]]*$';

SELECT b.id AS barcode_id, b.barcode, p.id AS product_id,
       p.ext_code AS product_code, bt.ext_code AS barcode_type_code
FROM inventory.inv_barcode b
JOIN inventory.inv_product p ON p.id = b.product_id
JOIN inventory.inv_barcode_type bt ON bt.id = b.barcode_type_id
WHERE p.ext_code IS NULL OR p.ext_code ~ '^[[:space:]]*$'
ORDER BY b.barcode;

SELECT ps.id AS relation_id, ps.active, ps.product_id, ps.supplier_id,
       p.ext_code AS product_code, s.ext_code AS supplier_code,
       ps.supplier_product_code
FROM inventory.inv_product_supplier ps
JOIN inventory.inv_product p ON p.id = ps.product_id
JOIN inventory.inv_supplier s ON s.id = ps.supplier_id
WHERE p.ext_code IS NULL OR p.ext_code ~ '^[[:space:]]*$'
   OR s.ext_code IS NULL OR s.ext_code ~ '^[[:space:]]*$';

-- Duplicados por codigo almacenado: el diagnostico Python adicional utilizo
-- la normalizacion del propio sincronizador (p. ej. ceros a la izquierda).
SELECT p.ext_code AS product_code, s.ext_code AS supplier_code,
       count(*) AS relation_count,
       array_agg(ps.id ORDER BY ps.id) AS relation_ids,
       count(*) FILTER (WHERE ps.active) AS active_count
FROM inventory.inv_product_supplier ps
JOIN inventory.inv_product p ON p.id = ps.product_id
JOIN inventory.inv_supplier s ON s.id = ps.supplier_id
GROUP BY p.ext_code, s.ext_code
HAVING count(*) > 1
ORDER BY relation_count DESC, p.ext_code, s.ext_code;

ROLLBACK;
