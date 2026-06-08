ALTER TABLE etl.cdc_table_config
    ADD COLUMN IF NOT EXISTS source_port_env varchar(128);
