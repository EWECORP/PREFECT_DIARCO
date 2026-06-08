-- Validacion operativa del piloto CDC para src.t050_articulos

-- 1. Estado actual del piloto
SELECT
    config_name,
    encode(last_start_lsn, 'hex') AS last_start_lsn_hex,
    encode(last_end_lsn, 'hex') AS last_end_lsn_hex,
    last_status,
    last_rowcount,
    last_error,
    last_started_at,
    last_finished_at,
    updated_at
FROM etl.cdc_state
WHERE config_name = 'pilot_t050_articulos';

-- 2. Ultimas corridas registradas
SELECT
    run_id,
    status,
    rows_read,
    rows_upserted,
    rows_deleted,
    encode(from_lsn, 'hex') AS from_lsn_hex,
    encode(to_lsn, 'hex') AS to_lsn_hex,
    duration_ms,
    error_text,
    created_at
FROM etl.cdc_run_log
WHERE config_name = 'pilot_t050_articulos'
ORDER BY run_id DESC
LIMIT 10;

-- 3. Ultimos registros tocados por el piloto
SELECT
    c_articulo,
    n_articulo,
    f_modif,
    fuente_origen,
    fecha_extraccion,
    encode(cdc_lsn, 'hex') AS cdc_lsn_hex,
    estado_sincronizacion
FROM src.t050_articulos
WHERE fuente_origen = (
    SELECT source_server || '.' || source_database || '.' || source_schema || '.' || source_table
    FROM etl.cdc_table_config
    WHERE config_name = 'pilot_t050_articulos'
)
ORDER BY fecha_extraccion DESC NULLS LAST, c_articulo
LIMIT 50;

-- 4. Conteo de registros con metadata CDC
SELECT
    COUNT(*) AS total_con_metadata_cdc
FROM src.t050_articulos
WHERE cdc_lsn IS NOT NULL;
