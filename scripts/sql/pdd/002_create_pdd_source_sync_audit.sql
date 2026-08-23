-- PDD - Auditoria de sincronizacion de fuentes diarco_data v1.0
-- PostgreSQL 14+
-- Base objetivo: diarco_data
--
-- Ejecucion sugerida:
-- PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" \
--   -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 \
--   -f scripts/sql/pdd/002_create_pdd_source_sync_audit.sql

BEGIN;

DO $preconditions$
BEGIN
    IF current_database() <> 'diarco_data' THEN
        RAISE EXCEPTION
            'Base incorrecta: se esperaba diarco_data y se recibio %',
            current_database();
    END IF;

    IF to_regclass('audit.pdd_source_sync_run') IS NOT NULL
       OR to_regclass('audit.pdd_source_sync_detail') IS NOT NULL THEN
        RAISE EXCEPTION
            'La auditoria PDD de fuentes ya existe; aplicar una migracion incremental';
    END IF;
END
$preconditions$;

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE audit.pdd_source_sync_run (
    source_sync_run_uuid uuid PRIMARY KEY,
    business_date date NOT NULL,
    cutoff_date date NOT NULL,
    status varchar(20) NOT NULL,
    refresh_mode varchar(20) NOT NULL,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    common_closed_date date,
    recommended_business_date date,
    source_count integer NOT NULL DEFAULT 0,
    ready_count integer NOT NULL DEFAULT 0,
    warning_count integer NOT NULL DEFAULT 0,
    blocker_count integer NOT NULL DEFAULT 0,
    refresh_options jsonb NOT NULL DEFAULT '{}'::jsonb,
    summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text,
    created_by varchar(120) NOT NULL,
    CONSTRAINT ck_pdd_source_sync_run_dates
        CHECK (cutoff_date = business_date - 1),
    CONSTRAINT ck_pdd_source_sync_run_status
        CHECK (status IN ('RUNNING', 'READY', 'BLOCKED', 'FAILED')),
    CONSTRAINT ck_pdd_source_sync_run_mode
        CHECK (refresh_mode IN ('FULL', 'VALIDATE_ONLY')),
    CONSTRAINT ck_pdd_source_sync_run_counts
        CHECK (
            source_count >= 0 AND ready_count >= 0
            AND warning_count >= 0 AND blocker_count >= 0
        )
);

CREATE TABLE audit.pdd_source_sync_detail (
    source_sync_run_uuid uuid NOT NULL,
    source_code varchar(50) NOT NULL,
    physical_relation varchar(160) NOT NULL,
    is_required boolean NOT NULL DEFAULT true,
    status varchar(20) NOT NULL,
    max_business_date date,
    as_of_ts timestamptz,
    row_count bigint,
    blocker_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    warning_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_sync_run_uuid, source_code),
    CONSTRAINT fk_pdd_source_sync_detail_run
        FOREIGN KEY (source_sync_run_uuid)
        REFERENCES audit.pdd_source_sync_run (source_sync_run_uuid)
        ON DELETE CASCADE,
    CONSTRAINT ck_pdd_source_sync_detail_status
        CHECK (status IN ('READY', 'WARN', 'BLOCKED')),
    CONSTRAINT ck_pdd_source_sync_detail_row_count
        CHECK (row_count IS NULL OR row_count >= 0)
);

CREATE INDEX ix_pdd_source_sync_run_business_date
    ON audit.pdd_source_sync_run (business_date DESC, started_at DESC);

CREATE INDEX ix_pdd_source_sync_run_status
    ON audit.pdd_source_sync_run (status, started_at DESC);

CREATE INDEX ix_pdd_source_sync_detail_status
    ON audit.pdd_source_sync_detail (status, source_code, created_at DESC);

COMMENT ON TABLE audit.pdd_source_sync_run IS
    'Cabecera auditable del contrato diario de fuentes requerido por PDD.';
COMMENT ON TABLE audit.pdd_source_sync_detail IS
    'Estado por fuente canonica observado al finalizar una sincronizacion PDD.';
COMMENT ON COLUMN audit.pdd_source_sync_run.business_date IS
    'Fecha operativa que consumira PDD; las fuentes analiticas deben cerrar business_date - 1.';
COMMENT ON COLUMN audit.pdd_source_sync_run.common_closed_date IS
    'Menor fecha cerrada entre ventas crudas, ventas enriquecidas y stock historico T710.';

COMMIT;
