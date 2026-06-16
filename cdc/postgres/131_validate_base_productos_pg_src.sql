-- Validacion operativa para comparar base_productos_vigentes
-- entre el modo hybrid_src y el nuevo modo pg_src.
--
-- Requiere haber generado previamente estas dos tablas snapshot:
--   - src.base_productos_vigentes_cmp_hybrid_src
--   - src.base_productos_vigentes_cmp_pg_src

-- 1. Presencia de tablas snapshot
SELECT
    to_regclass('src.base_productos_vigentes_cmp_hybrid_src') AS tabla_hybrid_src,
    to_regclass('src.base_productos_vigentes_cmp_pg_src') AS tabla_pg_src;

-- 2. Conteo total y claves distintas por modo
SELECT
    modo,
    total_filas,
    total_claves_distintas,
    total_sucursales,
    total_articulos,
    min_fecha_extraccion,
    max_fecha_extraccion
FROM (
    SELECT
        'hybrid_src' AS modo,
        COUNT(*) AS total_filas,
        COUNT(DISTINCT (c_sucu_empr, c_articulo, c_proveedor_primario)) AS total_claves_distintas,
        COUNT(DISTINCT c_sucu_empr) AS total_sucursales,
        COUNT(DISTINCT c_articulo) AS total_articulos,
        MIN(fecha_extraccion) AS min_fecha_extraccion,
        MAX(fecha_extraccion) AS max_fecha_extraccion
    FROM src.base_productos_vigentes_cmp_hybrid_src

    UNION ALL

    SELECT
        'pg_src' AS modo,
        COUNT(*) AS total_filas,
        COUNT(DISTINCT (c_sucu_empr, c_articulo, c_proveedor_primario)) AS total_claves_distintas,
        COUNT(DISTINCT c_sucu_empr) AS total_sucursales,
        COUNT(DISTINCT c_articulo) AS total_articulos,
        MIN(fecha_extraccion) AS min_fecha_extraccion,
        MAX(fecha_extraccion) AS max_fecha_extraccion
    FROM src.base_productos_vigentes_cmp_pg_src
) q
ORDER BY modo;

-- 3. Duplicados por clave operativa
WITH duplicados AS (
    SELECT
        'hybrid_src' AS modo,
        c_sucu_empr,
        c_articulo,
        c_proveedor_primario,
        COUNT(*) AS repeticiones
    FROM src.base_productos_vigentes_cmp_hybrid_src
    GROUP BY c_sucu_empr, c_articulo, c_proveedor_primario
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'pg_src' AS modo,
        c_sucu_empr,
        c_articulo,
        c_proveedor_primario,
        COUNT(*) AS repeticiones
    FROM src.base_productos_vigentes_cmp_pg_src
    GROUP BY c_sucu_empr, c_articulo, c_proveedor_primario
    HAVING COUNT(*) > 1
)
SELECT
    modo,
    COUNT(*) AS grupos_duplicados,
    COALESCE(SUM(repeticiones), 0) AS filas_en_grupos_duplicados,
    COALESCE(SUM(repeticiones - 1), 0) AS filas_duplicadas_sobrantes
FROM duplicados
GROUP BY modo
ORDER BY modo;

-- 4. Diferencia de cobertura por clave operativa
WITH hy_keys AS (
    SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_hybrid_src
),
pg_keys AS (
    SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_pg_src
)
SELECT
    origen,
    COUNT(*) AS total_claves
FROM (
    SELECT
        'solo_hybrid_src' AS origen,
        hy.c_sucu_empr,
        hy.c_articulo,
        hy.c_proveedor_primario
    FROM hy_keys hy
    LEFT JOIN pg_keys pg
      ON pg.c_sucu_empr = hy.c_sucu_empr
     AND pg.c_articulo = hy.c_articulo
     AND pg.c_proveedor_primario IS NOT DISTINCT FROM hy.c_proveedor_primario
    WHERE pg.c_sucu_empr IS NULL

    UNION ALL

    SELECT
        'solo_pg_src' AS origen,
        pg.c_sucu_empr,
        pg.c_articulo,
        pg.c_proveedor_primario
    FROM pg_keys pg
    LEFT JOIN hy_keys hy
      ON hy.c_sucu_empr = pg.c_sucu_empr
     AND hy.c_articulo = pg.c_articulo
     AND hy.c_proveedor_primario IS NOT DISTINCT FROM pg.c_proveedor_primario
    WHERE hy.c_sucu_empr IS NULL
) q
GROUP BY origen
ORDER BY origen;

-- 5. Muestra de claves presentes solo en uno de los modos
WITH hy_keys AS (
    SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_hybrid_src
),
pg_keys AS (
    SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_pg_src
)
SELECT *
FROM (
    SELECT
        'solo_hybrid_src' AS origen,
        hy.c_sucu_empr,
        hy.c_articulo,
        hy.c_proveedor_primario
    FROM hy_keys hy
    LEFT JOIN pg_keys pg
      ON pg.c_sucu_empr = hy.c_sucu_empr
     AND pg.c_articulo = hy.c_articulo
     AND pg.c_proveedor_primario IS NOT DISTINCT FROM hy.c_proveedor_primario
    WHERE pg.c_sucu_empr IS NULL

    UNION ALL

    SELECT
        'solo_pg_src' AS origen,
        pg.c_sucu_empr,
        pg.c_articulo,
        pg.c_proveedor_primario
    FROM pg_keys pg
    LEFT JOIN hy_keys hy
      ON hy.c_sucu_empr = pg.c_sucu_empr
     AND hy.c_articulo = pg.c_articulo
     AND hy.c_proveedor_primario IS NOT DISTINCT FROM pg.c_proveedor_primario
    WHERE hy.c_sucu_empr IS NULL
) q
ORDER BY origen, c_sucu_empr, c_articulo, c_proveedor_primario
LIMIT 50;

-- 6. Diferencias de atributos sobre claves compartidas
WITH shared_rows AS (
    SELECT
        hy.c_sucu_empr,
        hy.c_articulo,
        hy.c_proveedor_primario,
        hy.abastecimiento AS hy_abastecimiento,
        pg.abastecimiento AS pg_abastecimiento,
        hy.cod_cd AS hy_cod_cd,
        pg.cod_cd AS pg_cod_cd,
        hy.habilitado AS hy_habilitado,
        pg.habilitado AS pg_habilitado,
        hy.fecha_registro AS hy_fecha_registro,
        pg.fecha_registro AS pg_fecha_registro,
        hy.fecha_baja AS hy_fecha_baja,
        pg.fecha_baja AS pg_fecha_baja,
        hy.promocion AS hy_promocion,
        pg.promocion AS pg_promocion,
        hy.active_for_purchase AS hy_active_for_purchase,
        pg.active_for_purchase AS pg_active_for_purchase,
        hy.active_for_sale AS hy_active_for_sale,
        pg.active_for_sale AS pg_active_for_sale,
        hy.active_on_mix AS hy_active_on_mix,
        pg.active_on_mix AS pg_active_on_mix,
        hy.delivered_id AS hy_delivered_id,
        pg.delivered_id AS pg_delivered_id,
        hy.q_factor_compra AS hy_q_factor_compra,
        pg.q_factor_compra AS pg_q_factor_compra,
        hy.full_capacity_pallet AS hy_full_capacity_pallet,
        pg.full_capacity_pallet AS pg_full_capacity_pallet,
        hy.number_of_layers AS hy_number_of_layers,
        pg.number_of_layers AS pg_number_of_layers,
        hy.number_of_boxes_per_layer AS hy_number_of_boxes_per_layer,
        pg.number_of_boxes_per_layer AS pg_number_of_boxes_per_layer
    FROM src.base_productos_vigentes_cmp_hybrid_src hy
    INNER JOIN src.base_productos_vigentes_cmp_pg_src pg
      ON pg.c_sucu_empr = hy.c_sucu_empr
     AND pg.c_articulo = hy.c_articulo
     AND pg.c_proveedor_primario IS NOT DISTINCT FROM hy.c_proveedor_primario
)
SELECT *
FROM (
    SELECT 'abastecimiento' AS campo, COUNT(*) FILTER (WHERE hy_abastecimiento IS DISTINCT FROM pg_abastecimiento) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'cod_cd' AS campo, COUNT(*) FILTER (WHERE hy_cod_cd IS DISTINCT FROM pg_cod_cd) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'habilitado' AS campo, COUNT(*) FILTER (WHERE hy_habilitado IS DISTINCT FROM pg_habilitado) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'fecha_registro' AS campo, COUNT(*) FILTER (WHERE hy_fecha_registro IS DISTINCT FROM pg_fecha_registro) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'fecha_baja' AS campo, COUNT(*) FILTER (WHERE hy_fecha_baja IS DISTINCT FROM pg_fecha_baja) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'promocion' AS campo, COUNT(*) FILTER (WHERE hy_promocion IS DISTINCT FROM pg_promocion) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'active_for_purchase' AS campo, COUNT(*) FILTER (WHERE hy_active_for_purchase IS DISTINCT FROM pg_active_for_purchase) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'active_for_sale' AS campo, COUNT(*) FILTER (WHERE hy_active_for_sale IS DISTINCT FROM pg_active_for_sale) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'active_on_mix' AS campo, COUNT(*) FILTER (WHERE hy_active_on_mix IS DISTINCT FROM pg_active_on_mix) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'delivered_id' AS campo, COUNT(*) FILTER (WHERE hy_delivered_id IS DISTINCT FROM pg_delivered_id) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'q_factor_compra' AS campo, COUNT(*) FILTER (WHERE hy_q_factor_compra IS DISTINCT FROM pg_q_factor_compra) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'full_capacity_pallet' AS campo, COUNT(*) FILTER (WHERE hy_full_capacity_pallet IS DISTINCT FROM pg_full_capacity_pallet) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'number_of_layers' AS campo, COUNT(*) FILTER (WHERE hy_number_of_layers IS DISTINCT FROM pg_number_of_layers) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'number_of_boxes_per_layer' AS campo, COUNT(*) FILTER (WHERE hy_number_of_boxes_per_layer IS DISTINCT FROM pg_number_of_boxes_per_layer) AS filas_distintas FROM shared_rows
) q
ORDER BY filas_distintas DESC, campo;
