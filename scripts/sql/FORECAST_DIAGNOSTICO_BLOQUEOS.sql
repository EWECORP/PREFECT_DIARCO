-- Diagnostico de solo lectura. Ejecutar en diarco_data.
-- No cancela sesiones ni modifica datos/permisos.
-- Algunos detalles de sesiones ajenas requieren un administrador o
-- un rol de monitorizacion. Un campo NULL no demuestra que no haya actividad.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '1s';

SELECT clock_timestamp() AS capturado_en, current_user, current_database();

SELECT a.pid, a.usename, a.application_name, a.client_addr,
       a.state, a.wait_event_type, a.wait_event,
       clock_timestamp() - a.query_start AS duracion_consulta,
       clock_timestamp() - a.xact_start AS duracion_transaccion,
       pg_blocking_pids(a.pid) AS bloqueadores,
       left(a.query, 2000) AS consulta
FROM pg_stat_activity a
WHERE a.datname = current_database()
  AND a.pid <> pg_backend_pid()
  AND (a.state IS DISTINCT FROM 'idle'
       OR cardinality(pg_blocking_pids(a.pid)) > 0)
ORDER BY a.query_start NULLS LAST;

SELECT l.pid, a.usename, a.application_name, a.state,
       l.mode, l.granted, a.query_start,
       clock_timestamp() - a.xact_start AS duracion_transaccion
FROM pg_locks l
LEFT JOIN pg_stat_activity a USING (pid)
WHERE l.relation = 'src.base_stock_sucursal'::regclass
ORDER BY l.granted, l.pid;

-- Leer catalogos directamente evita pg_get_indexdef, que puede esperar
-- detras de DDL pendiente. No consulta filas de la tabla bloqueada.
SELECT t.relname AS tabla, i.relname AS indice,
       x.indisvalid, x.indisready, x.indkey
FROM pg_index x
JOIN pg_class t ON t.oid = x.indrelid
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'src' AND t.relname = 'base_stock_sucursal';

ROLLBACK;
