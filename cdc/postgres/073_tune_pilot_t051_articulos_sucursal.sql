UPDATE etl.cdc_table_config
SET
    batch_size = 5000,
    updated_at = now(),
    notes = 'Septima tabla piloto CDC SQL Server -> PostgreSQL para T051_ARTICULOS_SUCURSAL | tuned batch_size=5000 por volumen'
WHERE config_name = 'pilot_t051_articulos_sucursal';

SELECT
    config_name,
    poll_seconds,
    batch_size,
    notes,
    updated_at
FROM etl.cdc_table_config
WHERE config_name = 'pilot_t051_articulos_sucursal';




--- Informar NEVER RUN para volver a empezar el proceso desde el inicio (LSN NULL) y validar que funciona correctamente con la nueva configuración de batch_size
UPDATE etl.cdc_state
SET
    last_start_lsn = NULL,
    last_end_lsn = NULL,
    last_status = 'never_run',
    last_rowcount = 0,
    last_error = NULL,
    updated_at = now()
WHERE config_name = 'pilot_t051_articulos_sucursal';