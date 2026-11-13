# Error maestros-diario-test-prod - 13/11/2026

## Resultado

El fallo actual es de calidad de datos en el destino TEST, no de permisos ni
del bloqueo de stock investigado antes. No se modificaron datos, permisos,
configuracion ni codigo operativo. Se consultaron logs Prefect, codigo local
de CNX_DIARCO_SYNC_V2 y ambos destinos mediante conexiones de solo lectura.

Deployment: a58d2f07-d8db-4b4f-9726-19ec0fa48895, pool cnx-sync-prod.
Entrypoint: prefect_entry.py:run_daily_suite.
Run del 13/11: convivial-mayfly, 01a1c501-fe5b-7f78-9e08-bedae6e5277d.
https://orquestador.connexa-cloud.com/runs/flow-run/01a1c501-fe5b-7f78-9e08-bedae6e5277d
Inicio 07:30:02, fin fallido 07:31:20 (Argentina, UTC-3).

Error: `ValueError: Codigo de articulo destino vacio`.

Ruta de la traza: run_daily_sync -> plan_source_cut -> normalize_target_barcode
-> canonical_target_product_code. plan_source_cut procesa primero TEST y luego
PROD. En la planificacion encuentra un barcode cuyo producto no tiene ext_code
y aborta antes de apply_source_cut. Esta ejecucion no llego a aplicar los
maestros en ninguno de los dos destinos.

## Datos actuales verificados (aprox. 09:15-09:17)

TEST: connexa_platform_test, host 186.158.182.223.
PROD: connexa_platform_ms, host 186.158.182.127.

| Comprobacion | TEST | PROD |
| --- | ---: | ---: |
| Productos sin ext_code o con codigo en blanco | 5 | 0 |
| Barcodes leidos por el flujo asociados a esos productos | 2 | 0 |
| Productos distintos afectados por esos barcodes | 1 | 0 |
| Relaciones articulo-proveedor con error de normalizacion | 2 | 0 |
| Grupos articulo/proveedor duplicados tras normalizacion | 1 | 0 |
| Filas en esos grupos duplicados | 3 | 0 |

### Barcodes que provocan el error actual

Producto TEST sin ext_code: 8ac90f23-e46e-44ad-a005-9bfd021e46c9.

| Barcode | ID de inventory.inv_barcode |
| --- | --- |
| 7789778977896 | 9b877850-fa84-43a5-b949-9dd707b21a61 |
| 7789778978976 | a7e4b6a1-2446-4c4c-9695-5afc0850c19b |

Ambos tienen tipo de barcode ext_code=01. La consulta fetch_target_barcodes
devuelve p.ext_code AS product_code sin excluir codigos vacios. Al normalizar
ese campo, canonical_target_product_code rechaza el NULL.

### Otros errores que permanecen despues de corregir barcodes

Dos relaciones inventory.inv_product_supplier tambien tienen product_code NULL:

| ID de relacion | Codigo proveedor |
| --- | --- |
| 680d2ff7-8335-4c69-a41c-0fa7733e45ed | 12471 |
| 52d4a510-e75d-407a-8075-5f63920f2e59 | 5424 |

Ademas existen tres relaciones para la clave normalizada articulo=40,
proveedor=4647:

| ID de relacion | active | supplier_product_code |
| --- | --- | --- |
| 99635b07-a09c-4396-9691-685eb0aa78bd | false | 27651 |
| ef43c814-3caf-4982-9811-9d3fc5ac7bc9 | false | 27652 |
| 0b0381c1-001d-40db-b641-11947714a667 | true | 27653 |

Se aplicaron las mismas funciones normalize_target_product_supplier del codigo
local a las 22.685 relaciones TEST y 22.394 PROD. TEST dio dos errores de codigo
vacio y un grupo de tres duplicados; PROD no dio errores ni duplicados.

Solo inactivar filas NO elimina este error de duplicados: el lector incluye
activas e inactivas y build_product_supplier_plan exige una sola fila por clave
normalizada articulo/proveedor. Debe definirse el tratamiento de historicos y
la relacion canonica antes de cambiar datos o modificar el lector.

## Historial

La ultima ejecucion COMPLETED fue el 06/11/2026, de 07:29:59 a 07:38:14.
Run psychedelic-goshawk, 01a1a0f6-16d6-78f1-95dd-8dbbba795303.
Las siete ejecuciones diarias consultadas del 07/11 al 13/11 estan FAILED.

No todas fallaron por el mismo mensaje: los logs del 09/11 y 12/11 muestran
`El destino contiene relaciones articulo-proveedor duplicadas`. El 13/11
falla antes, al normalizar barcodes asociados a producto sin codigo.

## Correccion recomendada

1. Identificar los productos TEST sin ext_code y decidir si son articulos
   legitimos de SGM o registros exclusivos de pruebas. Para los legitimos,
   completar el codigo real y validar unicidad y correspondencia con origen.
   No inventar codigos ni borrar productos/barcodes sin revisar dependencias.
2. Para datos exclusivos de TEST sin correspondencia SGM, acordar una politica
   explicita de exclusion del sincronizador con reporte de registros omitidos.
   El plan de productos ya aparta unkeyed_products, pero barcodes y relaciones
   no tienen el mismo tratamiento. No aplicar un filtro silencioso que oculte
   inconsistencias en productos que si deben sincronizarse.
3. Resolver las tres relaciones 40/4647, conservando trazabilidad y revisando
   dependencias; o definir un lector que trate historicos fuera de la clave de
   sincronizacion segun la regla de negocio. Elegir una fila arbitrariamente
   podria asociar un codigo de proveedor incorrecto.
4. Validar la normalizacion completa de productos, barcodes y relaciones en
   ambos destinos. Ejecutar una planificacion sin escrituras bajo los controles
   del proyecto y revisar el resultado antes de una nueva aplicacion diaria.

La correccion de datos y cualquier cambio de politica del sincronizador quedan
pendientes: el pedido actual fue revisar el error. No se relanzo el deployment.

## Ubicaciones de codigo revisadas

- CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/pipeline/daily.py:144
- CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/pipeline/cut_plan.py:128
- CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/targets/postgres.py:214
- CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/domain/barcodes.py:185
- CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/domain/products.py:109
- CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/domain/product_suppliers.py:153

La configuracion inspeccionada es la copia local de .env. Las conexiones de
solo lectura confirmaron las bases anteriores; la traza productiva coincide
con la logica revisada, pero no se comparo el hash del codigo desplegado Linux
contra el checkout local.

## Correccion solicitada y bloqueo por recreacion automatica

El usuario autorizo corregir TEST usando PROD como referencia. Se compararon
los UUID, SKU y barcodes de los afectados contra PROD. El producto de pruebas
Pan lactal (SKU 22142215) y sus dos barcodes no tienen equivalente en PROD.
Las tres relaciones 40/4647 tampoco existen en PROD; la relacion correcta
40/3379 ya existe en TEST con los mismos valores funcionales de PROD.

Se respaldaron y retiraron en TEST dos barcodes y cinco relaciones en una
transaccion. La validacion con los normalizadores reales del sincronizador
paso dentro de esa transaccion: 25.173 barcodes y 22.680 relaciones, sin
codigos vacios ni claves duplicadas. Dos intentos anteriores se revirtieron
por escrituras concurrentes antes de modificar datos.

Sin embargo, una comprobacion posterior detecto que inventory-service vuelve
a insertar relaciones. Se observaron los mismos UUID con updated_by=
inventory-service y nuevos timestamps, incluidos los dos productos sin codigo.
Hay copias locales de esas relaciones en otros esquemas. Se respaldaron y
retiraron los mismos cinco UUID de las tablas fisicas locales correspondientes
de inventory, procurement_and_sourcing, supply_planning, cost_management,
stock_management, commercial_agreements y public. No se escribio ni leyo
contenido de tablas FDW que enlazan con PROD.

La recreacion continuo incluso despues de retirar las copias. Se verifico
la API actual de inventario de TEST: la version publicada el 13/11 ya no
expone DELETE para product-supplier, sino PATCH /v1/product-supplier/{id}
con active=false. Un intento DELETE basado en el contrato antiguo fue
rechazado (HTTP 500, metodo no soportado), sin completar una baja.

Se enviaron bajas PATCH de los tres UUID que seguian activos: los tres
respondieron HTTP 200 y active=false. Los otros dos UUID respondieron 404.
Se retiraron nuevamente las copias locales con respaldo, pero a las 14:22 UTC
se observaron de nuevo los tres UUID en inventory con active=true; dos seguian
asociados al producto sin codigo. Por lo tanto, la correccion NO esta estable.

La instruccion de TEST es sobreescrita por una escritura automatica del servicio.
No se agregaron triggers ni filtros para ocultar el problema, no se modifico
el sincronizador, no se detuvieron servicios o replicacion y no se relanzo el
deployment que aplica en TEST y PROD. Se solicito al usuario la ubicacion del
servidor/panel de administracion para localizar y controlar el escritor que
recrea las filas antes de una ultima limpieza y validacion estable.

### Respaldos y trazabilidad

Archivos locales dentro de ETL_DIARCO/data/repairs/:

- maestros_test_20261113T141219Z.json: filas originales de inventory y referencia PROD.
- maestros_test_replicas_20261113T141521Z.json: primeras copias locales retiradas.
- maestros_test_replicas_20261113T142124Z.json: segunda limpieza de copias.
- maestros_test_api_deactivate_20261113.json: resultados PATCH.
- openapi_inventory_test_20261113.json: contrato actual consultado.
- maestros_test_20261113_restore.sql: restauracion opcional de datos respaldados,
  no ejecutada. Antes de usarla, coordinar con los servicios para evitar otra
  carrera de escrituras.

PROD se uso exclusivamente en modo lectura durante toda esta intervencion.

### Aclaracion del worker y origen de las escrituras (15:03 UTC)

El usuario indico pool cnx-sync-prod, worker cnx-diarco-prod, servidor
186.158.182.127. Se verifico worker ONLINE y cero ejecuciones RUNNING,
PENDING o CANCELLING de los cuatro deployments del pool. Por lo tanto,
no hay evidencia para detener ese worker como solucion a la recreacion.

Mientras tanto, los mismos tres UUID seguian siendo actualizados en TEST
con updated_by=inventory-service. Un muestreo de pg_stat_activity capturo
en el PID 2892795, application_name=PostgreSQL JDBC Driver, client_addr=
127.0.0.1, tanto INSERT INTO inventory.inv_product_supplier como UPDATE de
la misma tabla. La conexion local a PostgreSQL TEST identifica al escritor
JDBC observado; no identifica por si sola el contenedor o servicio exacto
ni excluye la existencia de un proxy local.

Se intento exclusivamente hostname mediante SSH BatchMode y verificacion
estricta al servidor TEST .223. No existe una host key ED25519 conocida;
la conexion se rechazo antes de ejecutar el comando. No se deshabilito la
verificacion ni se detuvo ningun servicio. Se solicito la ubicacion del
panel/Docker/servicio o una conexion administrativa validada para continuar
con el escritor correcto, en lugar de interrumpir el pool productivo.

### Adjunto de systemctl en diarco-test y verificacion DEV (15:13 UTC)

El usuario aporto la salida de systemctl status pre* desde una sesion
zeetrex.local@diarco-test. Muestra workers CNX DEV, Forecast TEST, PDD TEST y LAB,
pero no enumera los servicios Java ni los contenedores de la aplicacion.

Se amplio la consulta Prefect a todos los deployments relacionados con maestros,
CNX e Inventory. cnx-diarco-dev en pool cnx-sync-lab y cnx-diarco-prod en
cnx-sync-prod estan ONLINE. No se encontraron ejecuciones RUNNING, PENDING o
CANCELLING en los deployments relacionados. No se detuvo ninguno de esos
workers. Falta identificar el servicio/contenedor que origina las escrituras
JDBC, mediante listado de servicios de aplicacion, procesos Java y contenedores
en la sesion administrativa de TEST.
