ALTER TABLE IF EXISTS src.t050_articulos
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
      AND table_name = 't050_articulos'
      AND column_name = 'cdc_lsn';

    IF v_cdc_lsn_type IS NOT NULL AND v_cdc_lsn_type <> 'bytea' THEN
        RAISE EXCEPTION
            'La columna src.t050_articulos.cdc_lsn debe ser BYTEA para almacenar LSN de SQL Server. Tipo actual: %',
            v_cdc_lsn_type;
    END IF;

END $$;

COMMENT ON COLUMN src.t050_articulos.fuente_origen IS
'Origen del proceso CDC que publico el registro.';

COMMENT ON COLUMN src.t050_articulos.fecha_extraccion IS
'Fecha y hora de aplicacion del cambio en PostgreSQL.';

COMMENT ON COLUMN src.t050_articulos.cdc_lsn IS
'LSN binario del cambio aplicado desde SQL Server CDC.';

COMMENT ON COLUMN src.t050_articulos.estado_sincronizacion IS
'Estado tecnico del registro publicado por CDC. 0=aplicado, 1=pendiente, 2=error, 9=reseed_required.';

CREATE INDEX IF NOT EXISTS t050_articulos_cdc_lsn_idx
    ON src.t050_articulos (cdc_lsn);

CREATE UNIQUE INDEX IF NOT EXISTS t050_articulos_pk_operativa_uidx
    ON src.t050_articulos (c_articulo);

-- Si tu tabla actual tiene `cdc_lsn` como `pg_lsn`, hay que cambiarla antes del piloto.
-- Recomendacion si la columna todavia no se usa:
-- ALTER TABLE src.t050_articulos DROP COLUMN cdc_lsn;
-- ALTER TABLE src.t050_articulos ADD COLUMN cdc_lsn bytea;
--
-- Si tu tabla actual tiene `estado_sincronizacion` como TEXT o VARCHAR, conviene cambiarla antes del piloto.
-- Recomendacion si la columna todavia no se usa:
-- ALTER TABLE src.t050_articulos DROP COLUMN estado_sincronizacion;
-- ALTER TABLE src.t050_articulos ADD COLUMN estado_sincronizacion smallint;
--
-- Validacion recomendada antes de usar ON CONFLICT (c_articulo):
-- SELECT c_articulo, COUNT(*)
-- FROM src.t050_articulos
-- GROUP BY c_articulo
-- HAVING COUNT(*) > 1;
