-- Validacion puntual: registros tocados hoy por CDC en src.t051_articulos_sucursal

-- 1. Resumen de filas tocadas hoy
SELECT
    current_date AS fecha_consulta,
    COUNT(*) AS filas_tocadas_hoy,
    COUNT(DISTINCT c_sucu_empr) AS sucursales_tocadas_hoy,
    COUNT(DISTINCT c_articulo) AS articulos_tocados_hoy,
    MIN(fecha_extraccion) AS primera_fecha_extraccion_hoy,
    MAX(fecha_extraccion) AS ultima_fecha_extraccion_hoy
FROM src.t051_articulos_sucursal
WHERE fuente_origen = (
    SELECT source_server || '.' || source_database || '.' || source_schema || '.' || source_table
    FROM etl.cdc_table_config
    WHERE config_name = 'pilot_t051_articulos_sucursal'
)
  AND fecha_extraccion >= current_date
  AND fecha_extraccion < current_date + INTERVAL '1 day';

-- 2. Ultimos registros tocados hoy por CDC
SELECT
    c_sucu_empr,
    c_articulo,
    fuente_origen,
    fecha_extraccion,
    encode(cdc_lsn, 'hex') AS cdc_lsn_hex,
    estado_sincronizacion
FROM src.t051_articulos_sucursal
WHERE fuente_origen = (
    SELECT source_server || '.' || source_database || '.' || source_schema || '.' || source_table
    FROM etl.cdc_table_config
    WHERE config_name = 'pilot_t051_articulos_sucursal'
)
  AND fecha_extraccion >= current_date
  AND fecha_extraccion < current_date + INTERVAL '1 day'
ORDER BY fecha_extraccion DESC, c_sucu_empr, c_articulo
LIMIT 100;

-- 3. Distribucion por hora de aplicacion
SELECT
    date_trunc('hour', fecha_extraccion) AS hora_extraccion,
    COUNT(*) AS filas_tocadas
FROM src.t051_articulos_sucursal
WHERE fuente_origen = (
    SELECT source_server || '.' || source_database || '.' || source_schema || '.' || source_table
    FROM etl.cdc_table_config
    WHERE config_name = 'pilot_t051_articulos_sucursal'
)
  AND fecha_extraccion >= current_date
  AND fecha_extraccion < current_date + INTERVAL '1 day'
GROUP BY 1
ORDER BY 1 DESC;

-- 4. Testigos manuales
-- Reemplazar los valores por combinaciones conocidas del negocio si hace falta.
WITH testigos(c_sucu_empr, c_articulo) AS (
    VALUES
        (1, 37),
        (1, 40),
        (1, 44)
)
SELECT
    t.c_sucu_empr,
    t.c_articulo,
    src.fecha_extraccion,
    src.fuente_origen,
    encode(src.cdc_lsn, 'hex') AS cdc_lsn_hex,
    src.estado_sincronizacion
FROM testigos t
LEFT JOIN src.t051_articulos_sucursal src
  ON src.c_sucu_empr = t.c_sucu_empr
 AND src.c_articulo = t.c_articulo
ORDER BY t.c_sucu_empr, t.c_articulo;
