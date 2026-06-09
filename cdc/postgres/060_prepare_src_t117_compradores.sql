ALTER TABLE IF EXISTS src.t117_compradores
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
      AND table_name = 't117_compradores'
      AND column_name = 'cdc_lsn';

    IF v_cdc_lsn_type IS NOT NULL AND v_cdc_lsn_type <> 'bytea' THEN
        RAISE EXCEPTION
            'La columna src.t117_compradores.cdc_lsn debe ser BYTEA para almacenar LSN de SQL Server. Tipo actual: %',
            v_cdc_lsn_type;
    END IF;
END $$;

COMMENT ON COLUMN src.t117_compradores.fuente_origen IS
'Origen del proceso CDC que publico el registro.';

COMMENT ON COLUMN src.t117_compradores.fecha_extraccion IS
'Fecha y hora de aplicacion del cambio en PostgreSQL.';

COMMENT ON COLUMN src.t117_compradores.cdc_lsn IS
'LSN binario del cambio aplicado desde SQL Server CDC.';

COMMENT ON COLUMN src.t117_compradores.estado_sincronizacion IS
'Estado tecnico del registro publicado por CDC. 0=aplicado, 1=pendiente, 2=error, 9=reseed_required.';

CREATE INDEX IF NOT EXISTS t117_compradores_cdc_lsn_idx
    ON src.t117_compradores (cdc_lsn);

CREATE UNIQUE INDEX IF NOT EXISTS t117_compradores_pk_operativa_uidx
    ON src.t117_compradores (c_comprador);

-- Supuesto inicial del piloto: la PK operativa es `c_comprador`.
-- Validacion recomendada antes de activar CDC:
-- SELECT c_comprador, COUNT(*)
-- FROM src.t117_compradores
-- GROUP BY c_comprador
-- HAVING COUNT(*) > 1;
