ALTER TABLE IF EXISTS src.t055_articulos_param_stock
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
      AND table_name = 't055_articulos_param_stock'
      AND column_name = 'cdc_lsn';

    IF v_cdc_lsn_type IS NOT NULL AND v_cdc_lsn_type <> 'bytea' THEN
        RAISE EXCEPTION
            'La columna src.t055_articulos_param_stock.cdc_lsn debe ser BYTEA para almacenar LSN de SQL Server. Tipo actual: %',
            v_cdc_lsn_type;
    END IF;
END $$;

COMMENT ON COLUMN src.t055_articulos_param_stock.fuente_origen IS
'Origen del proceso CDC que publico el registro.';

COMMENT ON COLUMN src.t055_articulos_param_stock.fecha_extraccion IS
'Fecha y hora de aplicacion del cambio en PostgreSQL.';

COMMENT ON COLUMN src.t055_articulos_param_stock.cdc_lsn IS
'LSN binario del cambio aplicado desde SQL Server CDC.';

COMMENT ON COLUMN src.t055_articulos_param_stock.estado_sincronizacion IS
'Estado tecnico del registro publicado por CDC. 0=aplicado, 1=pendiente, 2=error, 9=reseed_required.';

CREATE INDEX IF NOT EXISTS t055_articulos_param_stock_cdc_lsn_idx
    ON src.t055_articulos_param_stock (cdc_lsn);

CREATE UNIQUE INDEX IF NOT EXISTS t055_articulos_param_stock_pk_operativa_uidx
    ON src.t055_articulos_param_stock (
        c_sucu_empr,
        c_clasificacion_compra,
        c_familia,
        c_rubro
    );

-- PK operativa confirmada: (`c_sucu_empr`, `c_clasificacion_compra`, `c_familia`, `c_rubro`).
-- Validacion recomendada antes de activar CDC:
-- SELECT c_sucu_empr, c_clasificacion_compra, c_familia, c_rubro, COUNT(*)
-- FROM src.t055_articulos_param_stock
-- GROUP BY c_sucu_empr, c_clasificacion_compra, c_familia, c_rubro
-- HAVING COUNT(*) > 1;
