# Plan de Implementacion Gradual de CDC en ETL_DIARCO

## Objetivo

Incorporar Change Data Capture (CDC) en SQL Server para reducir tiempos de replica, evitar truncados y recargas completas innecesarias, y simplificar la sincronizacion entre DIARCO y CONNEXA sin reemplazar de una sola vez los flujos actuales.

## Estado actual del proyecto

Al 9 de junio de 2026, la base CDC ya no esta en etapa de idea o prototipo: quedo implementada, validada y funcionando sobre una tanda amplia de tablas maestras y medianas.

Tablas ya preparadas y operativas dentro del esquema CDC:

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

Componentes ya implementados:

- metadata central en PostgreSQL: `etl.cdc_table_config`, `etl.cdc_state`, `etl.cdc_run_log`
- aplicador generico: `scripts/cdc/cdc_replicar_tabla.py`
- monitoreo y alertas: `scripts/cdc/cdc_monitor.py`
- deployments Prefect para ejecucion recurrente de pilotos y monitoreo
- validaciones SQL por tabla en `cdc/sqlserver/` y `cdc/postgres/`

Conclusion practica:

- Fase 0 completada
- Fase 1 completada
- Fase 2 completada
- Fase 3, en el sentido de expansion a maestras y tablas medianas, completada para la tanda priorizada

Por lo tanto, el proximo paso natural ya no es "seguir sumando tablas maestras" de forma indefinida, sino pasar a una fase de consolidacion y recorte controlado del flujo legacy.

## Diagnostico del estado actual

### Lo que hoy funciona bien

- El proyecto ya tiene una separacion clara entre orquestacion, extraccion y carga con Prefect.
- Ya existen tablas y procesos de replica consolidados.
- Ya existe una base ordenada para el piloto CDC en:
  - `cdc/postgres/001_create_cdc_metadata.sql`
  - `cdc/postgres/002_prepare_src_t050_articulos.sql`
  - `cdc/postgres/003_seed_pilot_t050_articulos.sql`
  - `cdc/sqlserver/001_enable_cdc_t050_articulos.sql`
  - `scripts/cdc/cdc_replicar_tabla.py`

### Lo que hoy genera costo operativo

- La replica principal depende de stored procedures y lotes completos, por ejemplo en [scripts/repl/flujo_replicar_DMZ_en_LOTES.py](/E:/ETL/ETL_DIARCO/scripts/repl/flujo_replicar_DMZ_en_LOTES.py:52).
- Algunas tablas se vacian y recargan completas antes de importar datos, por ejemplo en [scripts/send/refresh_tablas_maestras.py](/E:/ETL/ETL_DIARCO/scripts/send/refresh_tablas_maestras.py:192) y [scripts/push/obtener_base_productos_vigentes.py](/E:/ETL/ETL_DIARCO/scripts/push/obtener_base_productos_vigentes.py:146).
- Ese enfoque agrega:
  - locks mas agresivos en PostgreSQL
  - mayor ventana de inconsistencia
  - mas I/O y mas tiempo de corrida
  - complejidad extra para tablas grandes

### Observaciones sobre el prototipo CDC actual

Los prototipos anteriores de CDC ya no deben mantenerse como camino activo. La nueva base del piloto resuelve los problemas principales que esos prototipos dejaban abiertos:

- estado LSN persistido en PostgreSQL y no en JSON local
- configuracion por tabla en metadata y no en archivos Python hardcodeados
- consumo por `fn_cdc_get_all_changes_<capture_instance>`
- aplicacion por `upsert/delete` sobre `src`

## Recomendacion de arquitectura

### Estrategia general

Usar un modelo de dos capas:

1. Carga inicial o refresh controlado por tabla.
2. Aplicacion incremental continua via CDC.

La carga inicial deja una fotografia consistente en PostgreSQL. Desde ese momento, CDC solo aplica deltas.

### Significado de capa canonica

En este proyecto, la capa canonica es la capa que debe considerarse fuente oficial para los consumidores aguas abajo.

Con el contexto actual de DIARCO, la recomendacion es:

- `src` en PostgreSQL `diarco_data` debe ser la capa canonica para CDC.
- `repl` en SQL Server debe quedar como capa legacy o de transicion.

Esto significa que:

- Connexa y los procesos posteriores deben seguir leyendo desde `src`.
- Los nuevos procesos CDC deben publicar directo en `src`, sin requerir pasar antes por `repl`.
- `repl` solo deberia mantenerse mientras existan procesos heredados que todavia dependan de ese esquema.

### Donde conviene ejecutar los procesos Python de CDC

La mejor ubicacion para los workers Python que leen CDC y publican a PostgreSQL es el servidor ETL en la DMZ de DIARCO, siempre que cumpla estas condiciones:

- conectividad estable hacia SQL Server productivo
- conectividad estable hacia PostgreSQL `diarco_data`
- capacidad para correr Prefect workers o tareas agendadas
- observabilidad basica de logs, reintentos y alertas

La razon principal es operativa:

- el servidor ETL ya esta en el camino natural de integracion
- evita meter logica Python dentro de SQL Server
- desacopla la captura de cambios del consumo final
- permite mantener en paralelo el modelo viejo y el nuevo durante la migracion

Recomendacion concreta:

1. leer CDC desde SQL Server productivo
2. aplicar transformacion minima en Python
3. publicar directo en PostgreSQL `src`
4. dejar `repl` solo para compatibilidad temporal si algun flujo viejo todavia lo necesita

### Componentes nuevos recomendados

#### 1. Metadata de configuracion en PostgreSQL

Crear una tabla de configuracion, por ejemplo `etl.cdc_table_config`, con:

- `source_server`
- `source_db`
- `source_schema`
- `source_table`
- `capture_instance`
- `target_schema`
- `target_table`
- `pk_columns`
- `enabled`
- `mode` (`cdc`, `batch`, `hybrid`)
- `poll_seconds`
- `apply_order`

#### 2. Estado por tabla

Crear `etl.cdc_state` con:

- `source_table`
- `last_start_lsn`
- `last_commit_time`
- `last_status`
- `last_rowcount`
- `last_error`
- `updated_at`

Si despues aparecen multiples origenes o ambientes, la clave debe incluir servidor y base.

#### 3. Log operativo

Crear `etl.cdc_run_log` con:

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

#### 4. Aplicador generico

Implementar un modulo Python reutilizable con esta secuencia:

1. leer configuracion de tabla
2. obtener `from_lsn` desde `etl.cdc_state`
3. validar que `from_lsn` siga siendo >= `fn_cdc_get_min_lsn`
4. obtener `to_lsn` con `sys.fn_cdc_get_max_lsn()`
5. leer cambios con `cdc.fn_cdc_get_all_changes_<capture_instance>(from_lsn, to_lsn, 'all')`
6. separar inserts/updates de deletes
7. aplicar `upsert` por PK en PostgreSQL
8. aplicar deletes por PK
9. persistir `to_lsn` solo si toda la transaccion en PostgreSQL termino bien

#### 5. Modo de recuperacion

Si `from_lsn` queda por debajo de `fn_cdc_get_min_lsn`, el proceso debe marcar la tabla como `reseed_required` y ejecutar:

1. snapshot completo
2. resincronizacion de estado
3. reanudacion de CDC

Esto evita perder cambios cuando el cleanup de CDC avanza mas rapido que el consumidor.

## Clasificacion recomendada de tablas

### Fase 1: tablas ideales para arrancar con CDC

Son tablas relativamente maestras, con alta reutilizacion y menor volumen que hechos pesados:

- `T050_ARTICULOS`
- `T020_PROVEEDOR`
- `T052_ARTICULOS_PROVEEDOR`
- `T100_EMPRESA_SUC`
- `T114_RUBROS`
- `T117_COMPRADORES`

Opcional en esta fase si la PK esta clara y estable:

- `M_3_ARTICULOS`

### Fase 2: tablas medianas con mas dependencia funcional

- `T051_ARTICULOS_SUCURSAL`
- `T020_PROVEEDOR_DIAS_ENTREGA_CABE`
- `T020_PROVEEDOR_DIAS_ENTREGA_DETA`
- `T055_ARTICULOS_PARAM_STOCK`
- `T055_ARTICULOS_CONDCOMPRA_COSTOS`
- `T085_ARTICULOS_EAN_EDI`

### Fase 3: tablas operativas con mas sensibilidad

- `T080_OC_CABE`
- `T081_OC_DETA`
- `T080_OC_PENDIENTES`

Estas conviene tratarlas en modo hibrido primero:

- snapshot o SP actual para bootstrap
- CDC para cambios recientes
- comparacion de conteos y claves antes de reemplazar el flujo actual

### Mantener en batch o incremental clasico al principio

- `T060_STOCK`
- `T702_EST_VTAS_POR_ARTICULO`
- `T710_ESTADIS_STOCK`
- tablas historicas o de altisimo volumen

CDC ahi puede servir mas adelante, pero no deberia ser la primera ola.

## Hoja de ruta de implementacion

### Fase 0. Preparacion tecnica

Objetivo: validar que la infraestructura soporte CDC de forma estable.

Checklist:

- confirmar que SQL Server Agent este operativo
- validar retencion de CDC y frecuencia de cleanup
- relevar PK o indice unico por tabla
- definir en que esquema de PostgreSQL se aplicaran los deltas
- confirmar formalmente `src` como capa canonica para CDC y documentar excepciones temporales en `repl`
- definir politica de monitoreo y alertas en Prefect

Entregables:

- inventario de tablas candidatas
- PK por tabla
- frecuencia objetivo por tabla
- tabla de compatibilidad `tabla -> modo`

Estado:

- completada

### Fase 1. Piloto productivo con una sola tabla

Tabla sugerida: `T050_ARTICULOS`

Pasos:

1. habilitar CDC en `T050_ARTICULOS`
2. hacer snapshot inicial hacia PostgreSQL
3. crear `etl.cdc_state`, `etl.cdc_run_log` y `etl.cdc_table_config`
4. desarrollar un aplicador generico solo para una tabla
5. programarlo en Prefect cada 1 o 5 minutos
6. comparar contra la tabla actual durante algunos dias

Criterio de salida:

- sin duplicados
- sin deletes perdidos
- sin drift en conteos y claves
- tiempo estable de aplicacion

Estado:

- completada

### Fase 2. Generalizacion del motor CDC

Objetivo: dejar de tener un script por tabla y pasar a un framework parametrico.

Pasos:

1. mover configuracion a una tabla de metadata
2. soportar PK simples y compuestas
3. soportar varios `capture_instance`
4. agregar reintentos y manejo de reseed
5. agregar limites de lote por corrida si el atraso es alto

Estado:

- completada

### Fase 3. Expansion a maestras relacionadas

Incorporar:

- `T020_PROVEEDOR`
- `T052_ARTICULOS_PROVEEDOR`
- `T100_EMPRESA_SUC`
- `T51_ARTICULOS_SUCURSAL`
- `T114_RUBROS`
- `T117_COMPRADORES`

Objetivo:

- bajar la dependencia de recargas completas
- reducir ventanas de lock
- alimentar mas rapido forecast, tableros y maestras

Estado:

- completada para la tanda priorizada del proyecto
- tablas cubiertas: `T020`, `T050`, `T051`, `T052`, `T055`, `T085`, `T100`, `T114`, `T117`

### Fase 4. Tablas operativas sensibles

Incorporar OC y tablas relacionadas solo despues de validar el motor:

- `T080_OC_CABE`
- `T081_OC_DETA`
- `T080_OC_PENDIENTES`

Recomendacion:

- dual run contra el flujo actual
- controles diarios de cantidades
- muestreo de claves y fechas

### Fase 4.1. Consolidacion y retiro selectivo de legacy

Antes de entrar de lleno en OC, conviene hacer una fase intermedia de ordenamiento operativo.

Objetivo:

- consolidar `src` como capa canonica efectiva y no solo declarativa
- empezar a retirar dependencias innecesarias de `repl` para las tablas ya migradas
- bajar costo operativo del esquema legacy

Acciones sugeridas:

1. armar una matriz `tabla -> origen actual -> origen objetivo -> estado`
2. identificar que flujos siguen leyendo desde `repl` para tablas que ya tienen CDC estable en `src`
3. hacer dual-run corto y comparaciones para cada consumidor importante
4. reemplazar gradualmente lecturas `repl.*` por `src.*` en los procesos downstream
5. desactivar SP, refresh o replicaciones completas solo cuando haya equivalencia probada
6. dejar rollback simple por tabla durante la transicion

Criterio de salida:

- consumidores principales leyendo desde `src` para las tablas ya migradas
- baja de ejecuciones legacy innecesarias
- monitoreo CDC estable durante al menos 1 o 2 semanas sin drift relevante

### Fase 4.2. Tablas operativas sensibles

Una vez cerrada la consolidacion, avanzar con:

- `T080_OC_CABE`
- `T081_OC_DETA`
- `T080_OC_PENDIENTES`

En estas tablas conviene mantener enfoque hibrido:

- bootstrap inicial por snapshot o SP actual
- CDC para deltas recientes
- validaciones diarias por conteo, fechas y muestreo de claves

### Fase 4.3. Tabla pesada en modo hibrido

Despues de OC, la candidata natural es `T060_STOCK`, pero no como replica CDC "plena" desde el dia uno.

Recomendacion:

- conservar batch o snapshot controlado para bootstrap
- usar CDC solo como complemento incremental
- monitorear volumen, latenica y costo operativo antes de reemplazar el flujo actual

### Fase 5. Retiro selectivo de flujos legacy

No conviene apagar todo junto.

Ir reemplazando solo los SP o refresh completos que ya tengan equivalencia probada con CDC. En cada reemplazo, dejar un mecanismo simple de rollback al flujo anterior.

Estado:

- pendiente
- debe iniciarse tabla por tabla, empezando por las ya estabilizadas en `src`

## Decisiones tecnicas recomendadas

### 1. Preferir `all` antes que `all update old`

Para aplicar cambios a una tabla destino sincronizada, normalmente alcanza con:

- `1` = delete
- `2` = insert
- `4` = update after

Solo usar `all update old` si realmente se necesita la imagen previa del registro.

### 2. No usar `supports_net_changes = 1` por defecto

Puede ser util, pero agrega mantenimiento e indices extra sobre las tablas de cambio. Para una primera etapa, suele ser mejor empezar con `all changes` y resolver consolidacion del lado Python cuando haga falta.

### 3. Upsert real en PostgreSQL

Evitar `to_sql(... if_exists='append')` como mecanismo final de sincronizacion CDC. Para productivo conviene:

- `INSERT ... ON CONFLICT (...) DO UPDATE`
- o staging temporal + merge/upsert

### 4. Estado fuera de archivos JSON

No guardar el estado productivo en archivos tipo `lsn_store.json`. Conviene guardarlo en PostgreSQL para:

- trazabilidad
- concurrencia controlada
- recuperacion mas simple
- monitoreo desde SQL

### 5. Bootstrap controlado

Cada tabla que pase a CDC debe tener dos modos:

- `snapshot_full`
- `cdc_incremental`

Esto simplifica resembrados y altas nuevas.

## Riesgos a considerar

### Riesgo 1. Cleanup de CDC antes de consumir cambios

Mitigacion:

- aumentar retencion al inicio
- monitorear atraso entre `last_start_lsn` y `max_lsn`
- disparar reseed automatico o manual

### Riesgo 2. Tablas sin PK clara

Mitigacion:

- relevar PK o indice unico antes de migrar
- si no existe, mantener batch hasta resolver la clave de negocio

### Riesgo 3. Deletes no aplicados

Mitigacion:

- definir delete por PK desde el primer piloto
- incluir pruebas de baja y reactivacion

### Riesgo 4. Drift entre capas `repl`, `src` y tablas consumidas

Mitigacion:

- definir `src` como capa canonica de CDC
- no mezclar refresh completos y CDC sobre la misma tabla final sin reglas claras

## Propuesta concreta para este proyecto

### Lo que yo haria primero

Eso ya fue realizado. A partir del estado actual, lo siguiente que conviene hacer es:

1. congelar la tanda actual como base estable de CDC
2. documentar que tablas ya quedaron "canonicas en `src`"
3. relevar que procesos todavia consumen `repl` para esas tablas
4. priorizar el reemplazo de lecturas legacy mas simples
5. preparar luego la entrada de `T080_OC_CABE`, `T081_OC_DETA` y `T080_OC_PENDIENTES` en modo hibrido

### Lo que dejaria para despues

- `T060_STOCK` en modo CDC pleno
- estadisticas pesadas
- tablas historicas de alto volumen
- apagado total de `repl` sin retiro gradual

## Siguientes pasos sugeridos

1. Armar una matriz de transicion `tabla -> consumidor -> hoy lee repl/src -> decision`.
2. Elegir 2 o 3 consumidores sencillos y migrarlos para que lean desde `src` en tablas ya estabilizadas.
3. Medir durante algunos dias drift, alertas y tiempos antes de apagar pasos legacy por tabla.
4. Definir el plan hibrido para `T080_OC_CABE`, `T081_OC_DETA` y `T080_OC_PENDIENTES`.
5. Dejar `T060_STOCK` para una etapa posterior y con estrategia especifica de volumen.

Documento de trabajo recomendado para esta etapa:

- `CDC_TRANSICION_REPL_A_SRC_MATRIX.md`
