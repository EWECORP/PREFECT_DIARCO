ALTER TABLE IF EXISTS src.t100_empresa_suc
    ADD COLUMN IF NOT EXISTS fuente_origen varchar(100),
    ADD COLUMN IF NOT EXISTS fecha_extraccion timestamp without time zone,
    ADD COLUMN IF NOT EXISTS cdc_lsn bytea,
    ADD COLUMN IF NOT EXISTS estado_sincronizacion smallint;

DO $$
DECLARE
    v_cdc_lsn_type text;
BEGIN
    SELECT data_type
    INTO v_cdc_lsn_type
    FROM information_schema.columns
    WHERE table_schema = 'src'
      AND table_name = 't100_empresa_suc'
      AND column_name = 'cdc_lsn';

    IF v_cdc_lsn_type IS NOT NULL AND v_cdc_lsn_type <> 'bytea' THEN
        RAISE EXCEPTION
            'La columna src.t100_empresa_suc.cdc_lsn debe ser BYTEA para almacenar LSN de SQL Server. Tipo actual: %',
            v_cdc_lsn_type;
    END IF;
END $$;

COMMENT ON COLUMN src.t100_empresa_suc.fuente_origen IS
'Origen del proceso CDC que publico el registro.';

COMMENT ON COLUMN src.t100_empresa_suc.fecha_extraccion IS
'Fecha y hora de aplicacion del cambio en PostgreSQL.';

COMMENT ON COLUMN src.t100_empresa_suc.cdc_lsn IS
'LSN binario del cambio aplicado desde SQL Server CDC.';

COMMENT ON COLUMN src.t100_empresa_suc.estado_sincronizacion IS
'Estado tecnico del registro publicado por CDC. 0=aplicado, 1=pendiente, 2=error, 9=reseed_required.';

CREATE INDEX IF NOT EXISTS t100_empresa_suc_cdc_lsn_idx
    ON src.t100_empresa_suc (cdc_lsn);

CREATE UNIQUE INDEX IF NOT EXISTS t100_empresa_suc_pk_operativa_uidx
    ON src.t100_empresa_suc (c_sucu_empr);

-- Supuesto inicial del piloto: la PK operativa es `c_sucu_empr`.
-- Validacion recomendada antes de activar CDC:
-- SELECT c_sucu_empr, COUNT(*)
-- FROM src.t100_empresa_suc
-- GROUP BY c_sucu_empr
-- HAVING COUNT(*) > 1;
