# Replicacion Continua SQL Server -> PostgreSQL con CDC + Python + Prefect

## Objetivo

Implementar un pipeline robusto y de baja latencia para replicar datos desde SQL Server hacia PostgreSQL utilizando:

- CDC en SQL Server
- Python para leer y aplicar cambios
- Prefect como orquestador
- upserts y deletes en PostgreSQL para mantener sincronizacion continua

El objetivo no es reemplazar de golpe todo el ETL actual, sino migrar gradualmente desde el esquema `repl` legacy hacia una sincronizacion directa sobre `src` en `diarco_data`.

## Contexto actual de DIARCO

Historicamente, el esquema `repl` en SQL Server se utilizaba como etapa intermedia dentro del servidor ETL de la DMZ porque el servidor productivo no soportaba CDC.

El flujo era, en esencia:

1. replicar o preparar datos en SQL Server `repl`
2. mover esos datos con Prefect hacia PostgreSQL
3. publicarlos en `src`
4. dejar que Connexa consuma desde `src`

Ahora que SQL Server productivo soporta CDC, la estrategia recomendada cambia:

- `src` en PostgreSQL pasa a ser la capa canonica
- `repl` en SQL Server queda como capa legacy o transitoria
- los nuevos procesos CDC deben leer directo desde SQL Server productivo y publicar directo en `src`

## Arquitectura recomendada

```text
SQL Server productivo
(CDC habilitado)
        |
        | LSN incremental
        v
Servidor ETL en DMZ
(Python + Prefect worker)
        |
        | UPSERT / DELETE
        v
PostgreSQL diarco_data.src
        |
        v
Connexa y procesos consumidores
```

## Donde conviene ejecutar los procesos Python

La ubicacion recomendada para los procesos Python que leen CDC y publican en PostgreSQL es el servidor ETL en la DMZ de DIARCO.

### Motivos

- ya tiene el rol de integracion entre plataformas
- puede ver directamente las bases SQL Server de produccion
- evita agregar logica operativa dentro de SQL Server
- permite monitoreo, retries y orquestacion con Prefect
- facilita una migracion gradual manteniendo compatibilidad con el flujo actual

### Recomendacion practica

- leer cambios CDC desde SQL Server productivo
- ejecutar el aplicador Python en el servidor ETL de la DMZ
- escribir directo en PostgreSQL `diarco_data.src`
- mantener `repl` solo mientras existan flujos viejos que todavia dependan de ese paso intermedio

La implementacion inicial de este proyecto queda estructurada en:

- `cdc/postgres/` para metadata y preparacion de tablas destino
- `cdc/sqlserver/` para habilitacion y validacion del origen CDC
- `scripts/cdc/cdc_replicar_tabla.py` como flujo parametrizable

## Capa canonica

En este proyecto, "capa canonica" significa la capa que se considera fuente oficial para los consumidores aguas abajo.

La definicion recomendada es:

- capa canonica: `src` en PostgreSQL `diarco_data`
- capa transitoria o legacy: `repl` en SQL Server

Consecuencia operativa:

- Connexa debe seguir leyendo desde `src`
- los controles de calidad, conteos y comparaciones deben hacerse contra `src`
- cualquier nueva tabla migrada a CDC debe apuntar a `src` como destino final

## Estrategia de migracion

Usar un modelo de dos pasos por tabla:

1. snapshot inicial o bootstrap
2. replicacion incremental via CDC

El snapshot deja la tabla alineada en PostgreSQL y el CDC mantiene los cambios posteriores sin truncar ni recargar todo.

## Estado actual de implementacion

La arquitectura propuesta en este documento ya fue implementada y validada en el proyecto.

Hoy el repositorio ya cuenta con:

- aplicador generico `scripts/cdc/cdc_replicar_tabla.py`
- metadata `etl.cdc_table_config`, `etl.cdc_state`, `etl.cdc_run_log`
- monitor `scripts/cdc/cdc_monitor.py`
- deployments Prefect por tabla
- pilotos operativos para tablas maestras y medianas priorizadas

Tablas ya cubiertas por la implementacion actual:

- `T050_ARTICULOS`
- `T020_PROVEEDOR`
- `T052_ARTICULOS_PROVEEDOR`
- `T100_EMPRESA_SUC`
- `T114_RUBROS`
- `T117_COMPRADORES`
- `T051_ARTICULOS_SUCURSAL`
- `T020_PROVEEDOR_DIAS_ENTREGA_CABE`
- `T020_PROVEEDOR_DIAS_ENTREGA_DETA`
- `T085_ARTICULOS_EAN_EDI`
- `T055_ARTICULOS_PARAM_STOCK`
- `T055_ARTICULOS_CONDCOMPRA_COSTOS`

Consecuencia practica:

- el siguiente paso recomendado ya no es construir mas esqueleto CDC
- el foco debe pasar a consolidacion, migracion de consumidores y retiro gradual del flujo legacy

## Tablas recomendadas para comenzar

### Primera ola

- `T050_ARTICULOS`
- `T020_PROVEEDOR`
- `T052_ARTICULOS_PROVEEDOR`
- `T100_EMPRESA_SUC`
- `T114_RUBROS`
- `T117_COMPRADORES`

### Segunda ola

- `T051_ARTICULOS_SUCURSAL`
- `T020_PROVEEDOR_DIAS_ENTREGA_CABE`
- `T020_PROVEEDOR_DIAS_ENTREGA_DETA`
- `T055_ARTICULOS_PARAM_STOCK`
- `T055_ARTICULOS_CONDCOMPRA_COSTOS`
- `T085_ARTICULOS_EAN_EDI`

### Dejar inicialmente en batch o modo hibrido

- `T060_STOCK`
- `T080_OC_CABE`
- `T081_OC_DETA`
- `T080_OC_PENDIENTES`
- `T702_EST_VTAS_POR_ARTICULO`
- `T710_ESTADIS_STOCK`

Nota importante:

- `T080_OC_PENDIENTES` no debe tratarse como tabla candidata a CDC directo
- en la arquitectura actual es un dataset derivado generado en la DMZ por `repl.usp_replicar_T080_OC_PENDIENTES`
- por lo tanto su evolucion debe resolverse como proceso derivado / materializacion intermedia, no como captura CDC sobre una tabla origen

## Proximo paso recomendado

Con la tanda actual estable, la hoja de ruta sugerida es:

1. consolidar `src` como origen canonico real para los consumidores
2. retirar lecturas innecesarias sobre `repl` en tablas ya migradas
3. redisenar primero los SP de la DMZ que siguen mezclando tablas CDC con tablas legacy:
   - `dbo.SP_BASE_PRODUCTOS_DMZ`
   - `dbo.SP_BASE_STOCK_EXTEND`
4. preparar una fase hibrida para:
   - `T080_OC_CABE`
   - `T081_OC_DETA`
   - `T080_OC_PENDIENTES` como artefacto derivado
5. dejar `T060_STOCK` para una etapa posterior y con estrategia especifica de volumen

En otras palabras:

- primero consolidacion y recorte de legacy
- despues tablas operativas sensibles
- al final tablas pesadas

## Habilitar CDC en SQL Server

### Habilitar CDC a nivel base

```sql
EXEC sys.sp_cdc_enable_db;
```

### Habilitar CDC por tabla

```sql
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'T050_ARTICULOS',
    @role_name     = NULL,
    @supports_net_changes = 0;
```

Para una primera etapa, conviene empezar con `all changes` y no depender de net changes.

## Estructura de los cambios CDC

Las funciones CDC devuelven metadata mas columnas originales de la tabla:

- `__$start_lsn`
- `__$seqval`
- `__$operation`
- `__$update_mask`
- columnas capturadas

Valores de `__$operation` en `fn_cdc_get_all_changes_<capture_instance>`:

| Codigo | Operacion |
|---|---|
| 1 | DELETE |
| 2 | INSERT |
| 3 | UPDATE old, solo si se usa `all update old` |
| 4 | UPDATE new / after |

Para sincronizacion operativa, normalmente alcanza con `all`, porque devuelve:

- deletes como `1`
- inserts como `2`
- updates como `4`

## Lectura de cambios desde Python

### Conexion a SQL Server

```python
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=SERVIDOR;"
    "DATABASE=BASE;"
    "UID=USUARIO;"
    "PWD=CLAVE"
)
```

### Lectura recomendada

No conviene leer la tabla `_CT` directamente en productivo. La recomendacion es usar la funcion:

```python
query = """
SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_T050_ARTICULOS(?, ?, 'all')
ORDER BY __$start_lsn, __$seqval
"""

cursor.execute(query, from_lsn, to_lsn)
rows = cursor.fetchall()
```

## Aplicacion de cambios en PostgreSQL

### Conexion

```python
import psycopg

pg = psycopg.connect("host=HOST dbname=diarco_data user=USER password=PASS")
```

### Upsert

```python
upsert_sql = """
INSERT INTO src.t050_articulos (id, descripcion, costo, cdc_lsn, fecha_extraccion)
VALUES (%s, %s, %s, %s, now())
ON CONFLICT (id)
DO UPDATE SET
    descripcion = EXCLUDED.descripcion,
    costo = EXCLUDED.costo,
    cdc_lsn = EXCLUDED.cdc_lsn,
    fecha_extraccion = EXCLUDED.fecha_extraccion;
"""
```

### Delete

```python
delete_sql = """
DELETE FROM src.t050_articulos
WHERE id = %s
"""
```

### Regla importante

No usar `append` simple como estrategia final de CDC. El proceso productivo debe:

- upsertear inserts y updates
- borrar deletes por PK
- persistir el ultimo LSN solo si la transaccion destino termino bien

Tipos recomendados para columnas tecnicas comunas:

- `cdc_lsn`: `bytea`
- `estado_sincronizacion`: `smallint`

## Manejo del LSN

Para productivo, el LSN no deberia guardarse en un JSON local. Conviene persistirlo en PostgreSQL.

### Tabla sugerida

`etl.cdc_state`

Columnas recomendadas:

- `source_server`
- `source_db`
- `source_schema`
- `source_table`
- `target_schema`
- `target_table`
- `last_start_lsn`
- `last_status`
- `last_error`
- `updated_at`

### Regla de avance

1. leer `from_lsn` desde `etl.cdc_state`
2. calcular `to_lsn` en SQL Server
3. aplicar cambios en PostgreSQL
4. grabar `to_lsn` solo al finalizar correctamente

## Flujo Prefect recomendado

```python
from prefect import flow, task

@task
def get_table_config(table_name):
    ...

@task
def read_state(table_config):
    ...

@task
def read_cdc_changes(table_config, from_lsn, to_lsn):
    ...

@task
def apply_changes_to_postgres(table_config, rows):
    ...

@task
def save_state(table_config, to_lsn):
    ...

@flow
def replicate_table(table_name):
    config = get_table_config(table_name)
    from_lsn = read_state(config)
    to_lsn = ...
    rows = read_cdc_changes(config, from_lsn, to_lsn)
    apply_changes_to_postgres(config, rows)
    save_state(config, to_lsn)
```

## Metadata recomendada

### `etl.cdc_table_config`

- `source_server`
- `source_db`
- `source_schema`
- `source_table`
- `capture_instance`
- `target_schema`
- `target_table`
- `pk_columns`
- `enabled`
- `mode`
- `poll_seconds`
- `apply_order`

### `etl.cdc_run_log`

- `run_id`
- `source_table`
- `from_lsn`
- `to_lsn`
- `rows_read`
- `rows_upserted`
- `rows_deleted`
- `status`
- `duration_ms`
- `error_text`
- `created_at`

## Estrategia operativa

- dimensiones y maestras: corrida cada 1 a 5 minutos
- tablas pesadas: batch nocturno o modo hibrido
- monitoreo: Prefect UI + logs estructurados
- alertas: falla de corrida, atraso de LSN, reseed requerido

## Riesgos principales

### 1. El cleanup de CDC avanza mas rapido que el consumidor

Mitigacion:

- subir la retencion al inicio
- monitorear atraso entre `min_lsn`, `last_lsn` y `max_lsn`
- ejecutar reseed si el estado queda fuera de ventana

### 2. Tablas sin PK clara

Mitigacion:

- relevar PK o indice unico antes de migrar
- si no existe, dejar la tabla en batch temporalmente

### 3. Mezcla de `repl` y `src` como destinos activos

Mitigacion:

- definir `src` como unico destino canonico
- usar `repl` solo mientras haya dependencias legacy

## Buenas practicas

- mantener LSN por tabla y por origen
- ordenar por `__$start_lsn` y `__$seqval`
- guardar estado y logs en PostgreSQL
- hacer bootstrap controlado antes de activar CDC
- usar dual-run en la migracion inicial
- no reemplazar todos los SP a la vez

## Recomendacion final para DIARCO

La estrategia mas consistente con la arquitectura actual es:

1. correr los workers Python CDC en el servidor ETL de la DMZ
2. leer directo desde SQL Server productivo
3. publicar directo en `src` de PostgreSQL
4. conservar `repl` solo como compatibilidad temporal
5. migrar tabla por tabla, empezando por `T050_ARTICULOS`

## Artefactos del piloto

- `cdc/postgres/001_create_cdc_metadata.sql`
- `cdc/postgres/002_prepare_src_t050_articulos.sql`
- `cdc/postgres/003_seed_pilot_t050_articulos.sql`
- `cdc/sqlserver/001_enable_cdc_t050_articulos.sql`
- `cdc/sqlserver/002_validate_cdc_t050_articulos.sql`
- `scripts/cdc/cdc_replicar_tabla.py`
