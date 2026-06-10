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
