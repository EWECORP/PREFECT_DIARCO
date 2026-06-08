CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.cdc_table_config (
    config_name text PRIMARY KEY,
    source_server varchar(255) NOT NULL,
    source_database varchar(255) NOT NULL,
    source_schema varchar(128) NOT NULL,
    source_table varchar(255) NOT NULL,
    capture_instance varchar(255) NOT NULL,
    source_driver_env varchar(128) NOT NULL DEFAULT 'SQL_DRIVER',
    source_port_env varchar(128),
    source_user_env varchar(128) NOT NULL DEFAULT 'SQL_USER',
    source_password_env varchar(128) NOT NULL DEFAULT 'SQL_PASSWORD',
    target_schema varchar(128) NOT NULL,
    target_table varchar(255) NOT NULL,
    pk_columns text[] NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    mode varchar(20) NOT NULL DEFAULT 'cdc',
    poll_seconds integer NOT NULL DEFAULT 300,
    batch_size integer NOT NULL DEFAULT 5000,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT cdc_table_config_mode_chk CHECK (mode IN ('cdc', 'batch', 'hybrid'))
);

CREATE UNIQUE INDEX IF NOT EXISTS cdc_table_config_target_uq
    ON etl.cdc_table_config (target_schema, target_table);

CREATE TABLE IF NOT EXISTS etl.cdc_state (
    config_name text PRIMARY KEY
        REFERENCES etl.cdc_table_config(config_name)
        ON DELETE CASCADE,
    last_start_lsn bytea,
    last_end_lsn bytea,
    last_commit_time timestamptz,
    last_status varchar(30) NOT NULL DEFAULT 'never_run',
    last_rowcount integer NOT NULL DEFAULT 0,
    last_error text,
    last_started_at timestamptz,
    last_finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl.cdc_run_log (
    run_id bigserial PRIMARY KEY,
    config_name text NOT NULL
        REFERENCES etl.cdc_table_config(config_name)
        ON DELETE CASCADE,
    source_table text NOT NULL,
    from_lsn bytea,
    to_lsn bytea,
    rows_read integer NOT NULL DEFAULT 0,
    rows_upserted integer NOT NULL DEFAULT 0,
    rows_deleted integer NOT NULL DEFAULT 0,
    status varchar(30) NOT NULL,
    duration_ms bigint,
    error_text text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cdc_run_log_config_created_idx
    ON etl.cdc_run_log (config_name, created_at DESC);
