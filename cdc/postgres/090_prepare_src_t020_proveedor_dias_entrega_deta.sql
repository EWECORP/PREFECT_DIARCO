ALTER TABLE IF EXISTS src.t020_proveedor_dias_entrega_deta
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
      AND table_name = 't020_proveedor_dias_entrega_deta'
      AND column_name = 'cdc_lsn';

    IF v_cdc_lsn_type IS NOT NULL AND v_cdc_lsn_type <> 'bytea' THEN
        RAISE EXCEPTION
            'La columna src.t020_proveedor_dias_entrega_deta.cdc_lsn debe ser BYTEA para almacenar LSN de SQL Server. Tipo actual: %',
            v_cdc_lsn_type;
    END IF;
END $$;

COMMENT ON COLUMN src.t020_proveedor_dias_entrega_deta.fuente_origen IS
'Origen del proceso CDC que publico el registro.';

COMMENT ON COLUMN src.t020_proveedor_dias_entrega_deta.fecha_extraccion IS
'Fecha y hora de aplicacion del cambio en PostgreSQL.';

COMMENT ON COLUMN src.t020_proveedor_dias_entrega_deta.cdc_lsn IS
'LSN binario del cambio aplicado desde SQL Server CDC.';

COMMENT ON COLUMN src.t020_proveedor_dias_entrega_deta.estado_sincronizacion IS
'Estado tecnico del registro publicado por CDC. 0=aplicado, 1=pendiente, 2=error, 9=reseed_required.';

CREATE INDEX IF NOT EXISTS t020_proveedor_dias_entrega_deta_cdc_lsn_idx
    ON src.t020_proveedor_dias_entrega_deta (cdc_lsn);

CREATE UNIQUE INDEX IF NOT EXISTS t020_proveedor_dias_entrega_deta_pk_operativa_uidx
    ON src.t020_proveedor_dias_entrega_deta (c_proveedor, c_sucu_empr, c_articulo);

-- PK operativa confirmada: (`c_proveedor`, `c_sucu_empr`, `c_articulo`).
-- Validacion recomendada antes de activar CDC:
-- SELECT c_proveedor, c_sucu_empr, c_articulo, COUNT(*)
-- FROM src.t020_proveedor_dias_entrega_deta
-- GROUP BY c_proveedor, c_sucu_empr, c_articulo
-- HAVING COUNT(*) > 1;
