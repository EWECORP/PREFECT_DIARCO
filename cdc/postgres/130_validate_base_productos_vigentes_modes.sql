-- Validacion operativa para comparar base_productos_vigentes
-- entre el modo sqlserver_sp y el modo hybrid_src.
--
-- Requiere haber generado previamente estas dos tablas snapshot:
--   - src.base_productos_vigentes_cmp_sqlserver_sp
--   - src.base_productos_vigentes_cmp_hybrid_src
--
-- Importante:
-- las dos capturas deben ejecutarse lo mas juntas posible.
-- Si entre un modo y el otro pasan minutos u horas, las diferencias de cobertura
-- pueden reflejar cambios reales del negocio y no necesariamente drift de logica.

-- 1. Presencia de tablas snapshot
SELECT
    to_regclass('src.base_productos_vigentes_cmp_sqlserver_sp') AS tabla_sqlserver_sp,
    to_regclass('src.base_productos_vigentes_cmp_hybrid_src') AS tabla_hybrid_src;

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
        'sqlserver_sp' AS modo,
        COUNT(*) AS total_filas,
        COUNT(DISTINCT (c_sucu_empr, c_articulo, c_proveedor_primario)) AS total_claves_distintas,
        COUNT(DISTINCT c_sucu_empr) AS total_sucursales,
        COUNT(DISTINCT c_articulo) AS total_articulos,
        MIN(fecha_extraccion) AS min_fecha_extraccion,
        MAX(fecha_extraccion) AS max_fecha_extraccion
    FROM src.base_productos_vigentes_cmp_sqlserver_sp

    UNION ALL

    SELECT
        'hybrid_src' AS modo,
        COUNT(*) AS total_filas,
        COUNT(DISTINCT (c_sucu_empr, c_articulo, c_proveedor_primario)) AS total_claves_distintas,
        COUNT(DISTINCT c_sucu_empr) AS total_sucursales,
        COUNT(DISTINCT c_articulo) AS total_articulos,
        MIN(fecha_extraccion) AS min_fecha_extraccion,
        MAX(fecha_extraccion) AS max_fecha_extraccion
    FROM src.base_productos_vigentes_cmp_hybrid_src
) q
ORDER BY modo;

-- 2b. Diferencia temporal entre snapshots
WITH ts AS (
    SELECT
        'sqlserver_sp' AS modo,
        MIN(fecha_extraccion) AS min_fecha_extraccion,
        MAX(fecha_extraccion) AS max_fecha_extraccion
    FROM src.base_productos_vigentes_cmp_sqlserver_sp

    UNION ALL

    SELECT
        'hybrid_src' AS modo,
        MIN(fecha_extraccion) AS min_fecha_extraccion,
        MAX(fecha_extraccion) AS max_fecha_extraccion
    FROM src.base_productos_vigentes_cmp_hybrid_src
)
SELECT
    sp.min_fecha_extraccion AS sp_min_fecha_extraccion,
    sp.max_fecha_extraccion AS sp_max_fecha_extraccion,
    hy.min_fecha_extraccion AS hy_min_fecha_extraccion,
    hy.max_fecha_extraccion AS hy_max_fecha_extraccion,
    EXTRACT(EPOCH FROM (hy.max_fecha_extraccion - sp.max_fecha_extraccion)) / 60.0 AS minutos_entre_maximos,
    CASE
        WHEN ABS(EXTRACT(EPOCH FROM (hy.max_fecha_extraccion - sp.max_fecha_extraccion)) / 60.0) > 10
            THEN 'ADVERTENCIA: snapshots no contemporaneos'
        ELSE 'OK: snapshots cercanos'
    END AS diagnostico_tiempo
FROM ts sp
JOIN ts hy
  ON sp.modo = 'sqlserver_sp'
 AND hy.modo = 'hybrid_src';

-- 3. Duplicados por clave operativa en cada modo
WITH duplicados AS (
    SELECT
        'sqlserver_sp' AS modo,
        c_sucu_empr,
        c_articulo,
        c_proveedor_primario,
        COUNT(*) AS repeticiones
    FROM src.base_productos_vigentes_cmp_sqlserver_sp
    GROUP BY c_sucu_empr, c_articulo, c_proveedor_primario
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'hybrid_src' AS modo,
        c_sucu_empr,
        c_articulo,
        c_proveedor_primario,
        COUNT(*) AS repeticiones
    FROM src.base_productos_vigentes_cmp_hybrid_src
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

-- 4. Muestra de duplicados si existieran
WITH duplicados AS (
    SELECT
        'sqlserver_sp' AS modo,
        c_sucu_empr,
        c_articulo,
        c_proveedor_primario,
        COUNT(*) AS repeticiones
    FROM src.base_productos_vigentes_cmp_sqlserver_sp
    GROUP BY c_sucu_empr, c_articulo, c_proveedor_primario
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'hybrid_src' AS modo,
        c_sucu_empr,
        c_articulo,
        c_proveedor_primario,
        COUNT(*) AS repeticiones
    FROM src.base_productos_vigentes_cmp_hybrid_src
    GROUP BY c_sucu_empr, c_articulo, c_proveedor_primario
    HAVING COUNT(*) > 1
)
SELECT *
FROM duplicados
ORDER BY modo, repeticiones DESC, c_sucu_empr, c_articulo, c_proveedor_primario
LIMIT 50;

-- 5. Diferencia de cobertura por clave operativa
WITH sp_keys AS (
    SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_sqlserver_sp
),
hy_keys AS (
    SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_hybrid_src
)
SELECT
    origen,
    COUNT(*) AS total_claves
FROM (
    SELECT
        'solo_sqlserver_sp' AS origen,
        sp.c_sucu_empr,
        sp.c_articulo,
        sp.c_proveedor_primario
    FROM sp_keys sp
    LEFT JOIN hy_keys hy
      ON hy.c_sucu_empr = sp.c_sucu_empr
     AND hy.c_articulo = sp.c_articulo
     AND hy.c_proveedor_primario IS NOT DISTINCT FROM sp.c_proveedor_primario
    WHERE hy.c_sucu_empr IS NULL

    UNION ALL

    SELECT
        'solo_hybrid_src' AS origen,
        hy.c_sucu_empr,
        hy.c_articulo,
        hy.c_proveedor_primario
    FROM hy_keys hy
    LEFT JOIN sp_keys sp
      ON sp.c_sucu_empr = hy.c_sucu_empr
     AND sp.c_articulo = hy.c_articulo
     AND sp.c_proveedor_primario IS NOT DISTINCT FROM hy.c_proveedor_primario
    WHERE sp.c_sucu_empr IS NULL
) q
GROUP BY origen
ORDER BY origen;

-- 6. Muestra de claves presentes solo en uno de los modos
WITH sp_keys AS (
    SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_sqlserver_sp
),
hy_keys AS (
    SELECT DISTINCT c_sucu_empr, c_articulo, c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_hybrid_src
)
SELECT *
FROM (
    SELECT
        'solo_sqlserver_sp' AS origen,
        sp.c_sucu_empr,
        sp.c_articulo,
        sp.c_proveedor_primario
    FROM sp_keys sp
    LEFT JOIN hy_keys hy
      ON hy.c_sucu_empr = sp.c_sucu_empr
     AND hy.c_articulo = sp.c_articulo
     AND hy.c_proveedor_primario IS NOT DISTINCT FROM sp.c_proveedor_primario
    WHERE hy.c_sucu_empr IS NULL

    UNION ALL

    SELECT
        'solo_hybrid_src' AS origen,
        hy.c_sucu_empr,
        hy.c_articulo,
        hy.c_proveedor_primario
    FROM hy_keys hy
    LEFT JOIN sp_keys sp
      ON sp.c_sucu_empr = hy.c_sucu_empr
     AND sp.c_articulo = hy.c_articulo
     AND sp.c_proveedor_primario IS NOT DISTINCT FROM hy.c_proveedor_primario
    WHERE sp.c_sucu_empr IS NULL
) q
ORDER BY origen, c_sucu_empr, c_articulo, c_proveedor_primario
LIMIT 50;

-- 7. Conteo de diferencias de atributos sobre claves compartidas
WITH shared_rows AS (
    SELECT
        sp.c_sucu_empr,
        sp.c_articulo,
        sp.c_proveedor_primario,
        sp.abastecimiento AS sp_abastecimiento,
        hy.abastecimiento AS hy_abastecimiento,
        sp.cod_cd AS sp_cod_cd,
        hy.cod_cd AS hy_cod_cd,
        sp.habilitado AS sp_habilitado,
        hy.habilitado AS hy_habilitado,
        sp.fecha_registro AS sp_fecha_registro,
        hy.fecha_registro AS hy_fecha_registro,
        sp.fecha_baja AS sp_fecha_baja,
        hy.fecha_baja AS hy_fecha_baja,
        sp.promocion AS sp_promocion,
        hy.promocion AS hy_promocion,
        sp.active_for_purchase AS sp_active_for_purchase,
        hy.active_for_purchase AS hy_active_for_purchase,
        sp.active_for_sale AS sp_active_for_sale,
        hy.active_for_sale AS hy_active_for_sale,
        sp.active_on_mix AS sp_active_on_mix,
        hy.active_on_mix AS hy_active_on_mix,
        sp.delivered_id AS sp_delivered_id,
        hy.delivered_id AS hy_delivered_id,
        sp.q_factor_compra AS sp_q_factor_compra,
        hy.q_factor_compra AS hy_q_factor_compra,
        sp.full_capacity_pallet AS sp_full_capacity_pallet,
        hy.full_capacity_pallet AS hy_full_capacity_pallet,
        sp.number_of_layers AS sp_number_of_layers,
        hy.number_of_layers AS hy_number_of_layers,
        sp.number_of_boxes_per_layer AS sp_number_of_boxes_per_layer,
        hy.number_of_boxes_per_layer AS hy_number_of_boxes_per_layer
    FROM src.base_productos_vigentes_cmp_sqlserver_sp sp
    INNER JOIN src.base_productos_vigentes_cmp_hybrid_src hy
      ON hy.c_sucu_empr = sp.c_sucu_empr
     AND hy.c_articulo = sp.c_articulo
     AND hy.c_proveedor_primario IS NOT DISTINCT FROM sp.c_proveedor_primario
)
SELECT *
FROM (
    SELECT 'abastecimiento' AS campo, COUNT(*) FILTER (WHERE sp_abastecimiento IS DISTINCT FROM hy_abastecimiento) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'cod_cd' AS campo, COUNT(*) FILTER (WHERE sp_cod_cd IS DISTINCT FROM hy_cod_cd) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'habilitado' AS campo, COUNT(*) FILTER (WHERE sp_habilitado IS DISTINCT FROM hy_habilitado) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'fecha_registro' AS campo, COUNT(*) FILTER (WHERE sp_fecha_registro IS DISTINCT FROM hy_fecha_registro) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'fecha_baja' AS campo, COUNT(*) FILTER (WHERE sp_fecha_baja IS DISTINCT FROM hy_fecha_baja) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'promocion' AS campo, COUNT(*) FILTER (WHERE sp_promocion IS DISTINCT FROM hy_promocion) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'active_for_purchase' AS campo, COUNT(*) FILTER (WHERE sp_active_for_purchase IS DISTINCT FROM hy_active_for_purchase) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'active_for_sale' AS campo, COUNT(*) FILTER (WHERE sp_active_for_sale IS DISTINCT FROM hy_active_for_sale) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'active_on_mix' AS campo, COUNT(*) FILTER (WHERE sp_active_on_mix IS DISTINCT FROM hy_active_on_mix) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'delivered_id' AS campo, COUNT(*) FILTER (WHERE sp_delivered_id IS DISTINCT FROM hy_delivered_id) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'q_factor_compra' AS campo, COUNT(*) FILTER (WHERE sp_q_factor_compra IS DISTINCT FROM hy_q_factor_compra) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'full_capacity_pallet' AS campo, COUNT(*) FILTER (WHERE sp_full_capacity_pallet IS DISTINCT FROM hy_full_capacity_pallet) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'number_of_layers' AS campo, COUNT(*) FILTER (WHERE sp_number_of_layers IS DISTINCT FROM hy_number_of_layers) AS filas_distintas FROM shared_rows
    UNION ALL
    SELECT 'number_of_boxes_per_layer' AS campo, COUNT(*) FILTER (WHERE sp_number_of_boxes_per_layer IS DISTINCT FROM hy_number_of_boxes_per_layer) AS filas_distintas FROM shared_rows
) q
ORDER BY filas_distintas DESC, campo;

-- 8. Testigos automaticos: muestra de sucursales/articulos compartidos
WITH shared_keys AS (
    SELECT DISTINCT
        sp.c_sucu_empr,
        sp.c_articulo,
        sp.c_proveedor_primario
    FROM src.base_productos_vigentes_cmp_sqlserver_sp sp
    INNER JOIN src.base_productos_vigentes_cmp_hybrid_src hy
      ON hy.c_sucu_empr = sp.c_sucu_empr
     AND hy.c_articulo = sp.c_articulo
     AND hy.c_proveedor_primario IS NOT DISTINCT FROM sp.c_proveedor_primario
),
testigos AS (
    SELECT
        c_sucu_empr,
        c_articulo,
        c_proveedor_primario
    FROM (
        SELECT
            sk.*,
            ROW_NUMBER() OVER (
                PARTITION BY c_sucu_empr
                ORDER BY c_articulo, c_proveedor_primario
            ) AS rn
        FROM shared_keys sk
    ) q
    WHERE rn <= 3
    ORDER BY c_sucu_empr, c_articulo, c_proveedor_primario
    LIMIT 30
)
SELECT
    t.c_sucu_empr,
    t.c_articulo,
    t.c_proveedor_primario,
    sp.cod_cd AS sp_cod_cd,
    hy.cod_cd AS hy_cod_cd,
    sp.habilitado AS sp_habilitado,
    hy.habilitado AS hy_habilitado,
    sp.active_for_purchase AS sp_active_for_purchase,
    hy.active_for_purchase AS hy_active_for_purchase,
    sp.active_for_sale AS sp_active_for_sale,
    hy.active_for_sale AS hy_active_for_sale,
    sp.delivered_id AS sp_delivered_id,
    hy.delivered_id AS hy_delivered_id,
    sp.fecha_registro AS sp_fecha_registro,
    hy.fecha_registro AS hy_fecha_registro,
    sp.fecha_baja AS sp_fecha_baja,
    hy.fecha_baja AS hy_fecha_baja
FROM testigos t
LEFT JOIN src.base_productos_vigentes_cmp_sqlserver_sp sp
  ON sp.c_sucu_empr = t.c_sucu_empr
 AND sp.c_articulo = t.c_articulo
 AND sp.c_proveedor_primario IS NOT DISTINCT FROM t.c_proveedor_primario
LEFT JOIN src.base_productos_vigentes_cmp_hybrid_src hy
  ON hy.c_sucu_empr = t.c_sucu_empr
 AND hy.c_articulo = t.c_articulo
 AND hy.c_proveedor_primario IS NOT DISTINCT FROM t.c_proveedor_primario
ORDER BY t.c_sucu_empr, t.c_articulo, t.c_proveedor_primario;

-- 9. Testigos manuales
-- Reemplazar los VALUES por sucursales/articulos/proveedores conocidos del negocio.
WITH testigos(c_sucu_empr, c_articulo, c_proveedor_primario) AS (
    VALUES
        (41, 0, 0),
        (82, 0, 0),
        (1, 0, 0)
)
SELECT
    t.c_sucu_empr,
    t.c_articulo,
    t.c_proveedor_primario,
    sp.cod_cd AS sp_cod_cd,
    hy.cod_cd AS hy_cod_cd,
    sp.habilitado AS sp_habilitado,
    hy.habilitado AS hy_habilitado,
    sp.active_for_purchase AS sp_active_for_purchase,
    hy.active_for_purchase AS hy_active_for_purchase,
    sp.active_for_sale AS sp_active_for_sale,
    hy.active_for_sale AS hy_active_for_sale,
    sp.delivered_id AS sp_delivered_id,
    hy.delivered_id AS hy_delivered_id,
    sp.fecha_registro AS sp_fecha_registro,
    hy.fecha_registro AS hy_fecha_registro,
    sp.fecha_baja AS sp_fecha_baja,
    hy.fecha_baja AS hy_fecha_baja
FROM testigos t
LEFT JOIN src.base_productos_vigentes_cmp_sqlserver_sp sp
  ON sp.c_sucu_empr = t.c_sucu_empr
 AND sp.c_articulo = t.c_articulo
 AND sp.c_proveedor_primario IS NOT DISTINCT FROM t.c_proveedor_primario
LEFT JOIN src.base_productos_vigentes_cmp_hybrid_src hy
  ON hy.c_sucu_empr = t.c_sucu_empr
 AND hy.c_articulo = t.c_articulo
 AND hy.c_proveedor_primario IS NOT DISTINCT FROM t.c_proveedor_primario
WHERE t.c_articulo <> 0
ORDER BY t.c_sucu_empr, t.c_articulo, t.c_proveedor_primario;
