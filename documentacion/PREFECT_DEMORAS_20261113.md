# Revision de demoras Prefect - 13/11/2026

Ventana de observacion: aproximadamente 08:44-08:48, hora Argentina (UTC-3).
Fuentes: API Prefect, pg_stat_activity, pg_locks, catalogos PostgreSQL y codigo
local de ETL_DIARCO/PDD. Revision de solo lectura; no se cancelaron consultas,
no se modificaron deployments, permisos, datos ni codigo operativo.

## Conclusion principal

FORECAST_PUSH_INPUT_DATA_PROD esta esperando un bloqueo de tabla en PostgreSQL.
No es el error de propiedad del 11/11 ni evidencia de lentitud en la extraccion
de SQL Server. El primer DROP de stock no termino y no se registro tabla
recreada ni el primer chunk. La tarea esta en su primer intento.

Run actual: honorable-whale, 01a1c4de-2430-74ed-b363-8f754f424c28.
https://orquestador.connexa-cloud.com/runs/flow-run/01a1c4de-2430-74ed-b363-8f754f424c28

Inicio 06:50:00. A las 08:44 llevaba aproximadamente 1 h 54 min, frente a
12 min 13 s el 12/11. Subflujo stock: soft-ferret,
fb6aef5d-757f-494f-bc17-57d6fae6fa7d.

## Evidencia de bloqueo

| PID observado | Usuario/aplicacion | Situacion |
| --- | --- | --- |
| 2497064 | connexa_user / ETL, aplicacion sin nombre | DROP TABLE IF EXISTS src.base_stock_sucursal CASCADE desde 06:50:08; wait_event_type=Lock, wait_event=relation; AccessExclusiveLock no concedido |
| 2367062 | connexa_user / pdd_backend | SELECT sobre stock desde 22:55:37 del 12/11; casi 9 h 49 min; AccessShareLock concedido; sin bloqueadores propios |
| 2379226 | postgres / pdd_backend | Tambien retiene AccessShareLock sobre stock; aparece como bloqueador directo del ETL; detalle de consulta y antiguedad ocultos para connexa_user |

Los PID son una captura temporal: revalidar actividad, inicio y consulta antes
de cualquier intervencion. Los PID paralelos adicionales no deben tratarse
como ejecuciones independientes.

La consulta visible de PDD usa scope_pairs y ranked con row_number() por
codigo_articulo/codigo_sucursal y fecha_extraccion, sobre stock del 11/11.
Coincide con _read_source_stock en PDD/backend/pdd_backend/jobs/daily_decas.py:479.
La conexion y la tarea de PDD PROD comienzan practicamente al mismo instante;
esa correlacion vincula con alta confianza la sesion visible al run PROD.
La vinculacion exacta del PID postgres con el run TEST requiere mas visibilidad
de sesiones; no se afirma como comprobada.

El SELECT conserva un bloqueo de lectura que es incompatible con el bloqueo
exclusivo requerido por DROP. Reemplazar DROP por TRUNCATE tampoco elimina
este conflicto: ambos requieren AccessExclusiveLock.

## Comparacion con ejecuciones normales

| Paso | 12/11 | 10/11 |
| --- | --- | --- |
| Stock, incluidos indices y ajustes | 7 min 41 s | 7 min 49 s |
| OC demoradas | 34 s | 47 s |
| Transferencias pendientes | 8 s | 8 s |
| Productos en transito | 15 s | 11 s |
| Productos vigentes | 2 min 54 s | 2 min 54 s |
| Ventas diarias/acumulados stock | 39 s | 45 s |
| Total | 12 min 13 s | 12 min 35 s |

El 09/11 termino en 10 min 35 s. Hoy la espera esta antes de esos trabajos,
no distribuida entre los seis pasos. Stock y productos vigentes pertenecen
actualmente a connexa_user; el flujo completo ya termino bien el 12/11.

Se encontro otro antecedente el 06/11: 6 h 6 min hasta fallar, con tres
intentos de aproximadamente dos horas cada uno en el primer script. Los logs
consultados de ese run no permiten atribuirle el mismo bloqueo de hoy.

## Otros procesos con demoras o estados anormales

### PDD_OPERATIONAL_DAILY_MASTER_PROD y PDD_OPERATIONAL_DAILY_MASTER (TEST)

- PROD, horned-numbat, 01a1c2cf-4bfc-741b-8e99-81e7d5665b2d:
  inicio 21:14:59 del 12/11. Tarea Construir posiciones y necesidades D y S
  desde 22:55:37. Sigue RUNNING durante la inspeccion.
- TEST, festive-porpoise, 01a1c2ea-e2ed-73de-acde-65b2c4a4dde4:
  inicio 21:45:06 del 12/11. Misma tarea desde 23:45:41.
  Sigue RUNNING durante la inspeccion.
- Ambos consultan la fuente compartida de stock; no hay limite de concurrencia
  configurado en esos dos deployments segun la API. Limitar cada deployment
  por separado no evita la superposicion entre PROD y TEST.
- Los indices de stock idx_bss_sucu_art e idx_bss_art existen, estan validos
  y listos. No corresponde atribuir el caso simplemente a falta de indices.
- En el codigo local PDD, _read_source_stock usa engine.connect(); los
  SET LOCAL statement_timeout/lock_timeout se aplican en otro helper,
  transactional_connection. Esta ruta de lectura no pasa por ese helper.
  _build_engine tampoco configura esos limites al conectar. Esto explica un
  posible escape de los limites definidos por Settings, pero debe verificarse
  contra la version desplegada. No hay overrides de timeout registrados en
  pg_db_role_setting para connexa_user/postgres en la consulta realizada.
- Falta medir el plan real de la consulta PDD para explicar su duracion extrema.
  No se ejecuto EXPLAIN ANALYZE de una consulta ya costosa en produccion.

### IOSDB_STOCK_MAYORISTA_LAB

Run amusing-civet, 01a1c51d-9418-7720-8fd8-57436ddc19c1, iniciado 08:00:04.
Durante una consulta de estados figuraba CANCELLING, pero su subflujo
enigmatic-copperhead, f458b3ba-cec2-4739-93cd-f998cb5855bf, seguia emitiendo
mensajes a las 08:46:28. El estado del padre no prueba que el trabajo hijo
haya finalizado.

La API devuelve HTTP 500 con error PostgreSQL product_stock_sales_products_fk:
product_id 75404, 75405 y 75406, entre los mensajes muestreados, no existen
en la tabla de productos referenciada. No es una demora causada por el mismo
bloqueo de stock confirmado para FORECAST. Revisar sincronizacion del maestro,
validacion de productos y corte ante errores permanentes repetidos.

### Estados viejos en Prefect

Se observaron 45 registros RUNNING/CANCELLING inicialmente. Una segunda
consulta devolvio 43 (24 RUNNING, 19 CANCELLING), 39 iniciados antes del 13/11,
algunos desde 2025. Esto es un inventario de estados, no prueba de 43 procesos
de sistema operativo consumiendo recursos.

FORECAST_REFRESH_INPUT_DATA_PROD tiene un run CANCELLING desde el 10/11
(invincible-starling, 01a1ac90-f914-73be-8734-9cad8b96988a) y un subflujo
todavia marcado RUNNING, con ultimo mensaje del 10/11. Verificar procesos
reales del worker antes de limpiar estados. No se cancelo ni forzo ningun estado.

## Acciones recomendadas

1. Revalidar las dos sesiones bloqueadoras y evaluar con el responsable de PDD
   la cancelacion controlada de sus consultas/transacciones. Un administrador
   debe inspeccionar especialmente el PID de postgres antes de actuar. Liberar
   solo una sesion puede dejar al ETL esperando por la otra. Verificar que los
   locks desaparezcan y que la transaccion sea cerrada; cancelar una consulta
   no equivale siempre a cerrar una transaccion. Vigilar que no se relance PDD
   automaticamente contra la misma fuente mientras se recupera el ETL.
2. Una vez liberado, observar si el run actual avanza. No lanzar otro forecast
   concurrente. Verificar los seis pasos y las fechas de stock resultantes.
3. ETL: configurar limites por sesion (punto inicial a validar: lock_timeout
   30 s, limites de consulta/tarea acordes con cada paso), application_name y
   logs explicitos antes/despues del DROP. Actualmente open_pg_conn no los fija
   y ejecutar_script no tiene un timeout de proceso. Los reintentos deben
   distinguir bloqueos transitorios de errores permanentes.
4. PDD: aplicar los timeouts tambien en la lectura de stock; revisar el plan,
   cardinalidades, filtro de fecha y ranking sobre el volumen completo antes
   de decidir indices o reescritura. Ordenar ventanas de PROD/TEST/ETL o usar
   coordinacion compartida para esta fuente.
5. Redisenar la publicacion de stock para evitar DROP/CREATE y preservar grants
   e indices. Una recarga con DELETE/INSERT en transaccion o snapshots
   versionados son alternativas a evaluar por costo, consistencia y lectores.
   Un intercambio de tablas mediante DDL tambien necesita locks; staging por
   si solo no resuelve lectores que retienen transacciones durante horas.
6. IOSDB: corregir productos faltantes y confirmar terminacion efectiva de
   hijos al cancelar. Depurar estados historicos solo tras verificar workers.
7. Alertar forecast por duracion anormal (por ejemplo mas de 20 min como
   umbral inicial), por esperas de lock y por silencio prolongado de logs.

## Limites y herramientas

Se uso connexa_user para inspeccion PostgreSQL. No hereda pg_read_all_stats,
por lo que varias columnas de sesiones de postgres no son visibles. No se
cambiaron privilegios para ampliar esa visibilidad.

Una lectura de pg_indexes que solicita definiciones de indices excedio el
timeout de diagnostico de 15 s mientras habia DDL esperando. Se cerro la
conexion y se verificaron indices mediante catalogos directos, sin esa funcion.
Las consultas posteriores usaron lock_timeout=1 s y statement_timeout=5 s.

scripts/sql/FORECAST_DIAGNOSTICO_BLOQUEOS.sql permite repetir una captura de
solo lectura. Los tiempos del informe corresponden a la captura, no a un
seguimiento continuo posterior.

Referencias PostgreSQL 14:
- https://www.postgresql.org/docs/14/explicit-locking.html
- https://www.postgresql.org/docs/14/runtime-config-client.html

## Intervencion posterior autorizada por el usuario

El usuario solicito eliminar el bloqueo y liberar los flujos inmediatamente.
Esta seccion registra acciones posteriores al diagnostico de solo lectura.

- Antes de intervenir, el PID 2367062 ya habia desaparecido. Los flujos PDD
  PROD y TEST figuraban CANCELLED desde aproximadamente las 08:46. Esta
  cancelacion de flujos no fue realizada por esta intervencion.
- Se revalido como administrador en la misma base diarco_data el PID 2379226:
  cliente pdd_backend, usuario postgres, consulta activa desde 23:45:41 del
  12/11, lectura de stock del 11/11. Seguía bloqueando directamente el DROP
  del forecast. Se ejecuto pg_cancel_backend exclusivamente sobre esa sesion,
  comprobando identidad/inicio y relacion de bloqueo: devolvio true.
- A las 08:50:06 el DROP del forecast dejo de esperar. La consulta PDD y sus
  trabajadores paralelos desaparecieron; no se uso pg_terminate_backend contra PDD.
- La sesion ETL 2497064 quedo idle in transaction luego del DROP, sin avanzar
  a CREATE. A las 08:51:16 Prefect registro connection already closed en el
  primer script, pero la sesion de servidor seguia abierta y conservaba locks
  exclusivos del DROP sin confirmar.
- Se revalido esa sesion por PID, backend_start, usuario, base, consulta exacta
  y estado idle in transaction de mas de 30 segundos. Se ejecuto
  pg_terminate_backend sobre esa sesion ETL huerfana: devolvio true y PostgreSQL
  revirtio la transaccion sin confirmar. La consulta posterior no encontro
  ninguna de las tres sesiones incidentadas ni sesiones bloqueadas en la base.
- No se modificaron .env, usuarios, permisos, deployments ni codigo operativo.
  Se utiliza el reintento existente del mismo flujo para recuperar la carga;
  no se lanzo una segunda ejecucion concurrente.
- A las 08:53:00 Prefect registro el inicio del segundo intento de
  obtener_base_stock.py. Una lectura acotada de stock y el control de sesiones
  confirmaron tabla accesible y ausencia de bloqueos a esa hora.
- El worker dmz-diarco esta ONLINE, con heartbeat a las 08:54:23. El primer
  subflujo de stock quedo FAILED por la conexion cerrada; el flujo padre
  permanece RUNNING y ya emitio el inicio del reintento. Aun no se confirmo
  la carga de nuevos chunks ni la finalizacion de los seis scripts.

## Acceso desde pgAdmin y segunda liberacion (aprox. 09:04-09:07)

El usuario informo lentitud al acceder a stock. La inspeccion mostro que el
reintento habia retomado efectivamente la carga: nuevo subflujo striped-doberman
(5426c3c8-7acc-4624-8455-39c3489d219d), iniciado 08:55:50; conexion SQL 2530360
desde 08:56:00, y al menos 680.000 filas copiadas a las 09:04:10.

La transaccion de DROP/CREATE/COPY retenia AccessExclusiveLock sobre el objeto
anterior durante toda la recarga, por lo que los lectores no podian acceder.
Esto es distinto de los bloqueadores PDD, que ya no estaban presentes.
Prueba: SELECT 1 respondio en 7 ms, mientras una lectura de una fila de stock
termino por lock_timeout a los 758 ms. Habia 9 conexiones en diarco_data,
frente a un max_connections de servidor de 1000; no se observo saturacion de
conexiones. No se midieron CPU, RAM ni disco del sistema operativo.

El forecast padre ya estaba CANCELLING desde las 09:01:00, pero el subflujo
seguia RUNNING y copiando datos. Para completar esa cancelacion y mantener el
acceso solicitado por el usuario:

1. Se revalido el estado CANCELLING del padre y se solicito CANCELLING al hijo.
2. Se revalido la sesion 2530360 por usuario, direccion cliente, backend_start,
   xact_start, consulta COPY y bloqueo exclusivo sobre stock. Se termino esa
   conexion (pg_terminate_backend=true); la transaccion incompleta se revirtio.
3. Stock respondio en 8 ms. No quedaron sesiones bloqueadas ni transacciones
   abiertas en diarco_data en la verificacion. Se conservaron las estimaciones
   anteriores de 1.339.963 filas; una fila muestreada tenia fecha_stock=11/11 y
   fecha_extraccion=12/11 20:01:04 (valor timestamp almacenado, sin inferir zona).
   La carga de hoy no se confirmo ni completo.
4. El subflujo registro Finished in state Cancelling a las 09:06:16. Despues
   de comprobarlo se completaron manualmente en Prefect los estados CANCELLED
   del hijo, del padre y de su tarea ejecutar_script, para cerrar la cancelacion
   pendiente y evitar nuevos reintentos. La API acepto las tres transiciones.

No se reinicio PostgreSQL ni se cambiaron permisos/configuracion. No se lanzo
otro forecast. Antes de relanzar, corregir la estrategia de recarga o acordar
una ventana sin lectores: la implementacion actual hace inaccesible stock
durante toda la transaccion de DROP/CREATE y copia de chunks.
