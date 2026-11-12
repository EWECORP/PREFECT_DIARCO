-- Correccion MINIMA del error observado el 2026-11-11.
-- Base: diarco_data. Ejecutar como postgres o administrador autorizado.
-- Este archivo fue preparado para revision; NO fue aplicado por el diagnostico.
-- No otorga superusuario ni membresia en postgres a connexa_user.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF current_database() <> 'diarco_data' THEN
        RAISE EXCEPTION 'Base incorrecta: %', current_database();
    END IF;
    IF NOT pg_has_role('connexa_user', 'connexa_owner', 'USAGE') THEN
        RAISE EXCEPTION 'connexa_user debe heredar connexa_owner';
    END IF;
END $$;

ALTER TABLE src.base_productos_vigentes OWNER TO connexa_owner;

COMMIT;

-- Verificar conectado como connexa_user: debe devolver true en ambos casos.
SELECT current_user,
       pg_get_userbyid(c.relowner) AS propietario,
       pg_has_role('connexa_user', c.relowner, 'USAGE') AS propietario_efectivo,
       has_schema_privilege('connexa_user', 'src', 'CREATE') AS puede_crear_en_src
FROM pg_class c
WHERE c.oid = 'src.base_productos_vigentes'::regclass;

-- Incidencia adicional: restaurar acceso del lector y conservarlo cuando
-- el ETL recrea sus tablas. Bloque OPCIONAL, revisar y ejecutar por separado
-- como administrador. Los defaults afectan futuras tablas de src creadas
-- por estos roles, no solamente las cinco tablas del forecast.
-- BEGIN;
-- GRANT SELECT ON src.base_stock_sucursal TO nexa_report_reader;
-- ALTER DEFAULT PRIVILEGES FOR ROLE connexa_user IN SCHEMA src
--     GRANT SELECT ON TABLES TO nexa_report_reader;
-- ALTER DEFAULT PRIVILEGES FOR ROLE connexa_owner IN SCHEMA src
--     GRANT SELECT ON TABLES TO nexa_report_reader;
-- COMMIT;
