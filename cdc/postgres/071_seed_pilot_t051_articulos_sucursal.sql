INSERT INTO etl.cdc_table_config (
    config_name,
    source_server,
    source_database,
    source_schema,
    source_table,
    capture_instance,
    source_driver_env,
    source_port_env,
    source_user_env,
    source_password_env,
    target_schema,
    target_table,
    pk_columns,
    enabled,
    mode,
    poll_seconds,
    batch_size,
    notes
)
VALUES (
    'pilot_t051_articulos_sucursal',
    '192.168.0.11',
    'DiarcoP',
    'dbo',
    'T051_ARTICULOS_SUCURSAL',
    'dbo_T051_ARTICULOS_SUCURSAL',
    'SQLP_DRIVER',
    'SQLP_PORT',
    'SQLP_USER',
    'SQLP_PASSWORD',
    'src',
    't051_articulos_sucursal',
    ARRAY['c_sucu_empr', 'c_articulo'],
    true,
    'cdc',
    600,
    10000,
    'Septima tabla piloto CDC SQL Server -> PostgreSQL para T051_ARTICULOS_SUCURSAL'
)
ON CONFLICT (config_name) DO UPDATE
SET source_server = EXCLUDED.source_server,
    source_database = EXCLUDED.source_database,
    source_schema = EXCLUDED.source_schema,
    source_table = EXCLUDED.source_table,
    capture_instance = EXCLUDED.capture_instance,
    source_driver_env = EXCLUDED.source_driver_env,
    source_port_env = EXCLUDED.source_port_env,
    source_user_env = EXCLUDED.source_user_env,
    source_password_env = EXCLUDED.source_password_env,
    target_schema = EXCLUDED.target_schema,
    target_table = EXCLUDED.target_table,
    pk_columns = EXCLUDED.pk_columns,
    enabled = EXCLUDED.enabled,
    mode = EXCLUDED.mode,
    poll_seconds = EXCLUDED.poll_seconds,
    batch_size = EXCLUDED.batch_size,
    notes = EXCLUDED.notes,
    updated_at = now();

INSERT INTO etl.cdc_state (
    config_name,
    last_status,
    updated_at
)
VALUES (
    'pilot_t051_articulos_sucursal',
    'never_run',
    now()
)
ON CONFLICT (config_name) DO NOTHING;
