# CDC Piloto

Esta carpeta concentra la base del nuevo esquema de CDC para ETL_DIARCO.

## Estructura

- `postgres/`
  - `001_create_cdc_metadata.sql`: crea el esquema `etl` y las tablas de metadata CDC
  - `001b_alter_cdc_metadata_add_source_port_env.sql`: agrega soporte de puerto al metadata CDC ya creado
  - `002_prepare_src_t050_articulos.sql`: prepara `src.t050_articulos` para el piloto
  - `003_seed_pilot_t050_articulos.sql`: inserta la configuracion inicial del piloto
  - `004_validate_pilot_t050_articulos.sql`: valida estado, corridas y ultimos registros impactados del piloto
  - `010_prepare_src_t020_proveedor.sql`: prepara `src.t020_proveedor` para el segundo piloto
  - `011_seed_pilot_t020_proveedor.sql`: inserta la configuracion inicial de `T020_PROVEEDOR`
  - `012_validate_pilot_t020_proveedor.sql`: valida estado, corridas y ultimos registros impactados del segundo piloto
  - `020_prepare_src_t052_articulos_proveedor.sql`: prepara `src.t052_articulos_proveedor` para el tercer piloto
  - `021_seed_pilot_t052_articulos_proveedor.sql`: inserta la configuracion inicial de `T052_ARTICULOS_PROVEEDOR`
  - `022_validate_pilot_t052_articulos_proveedor.sql`: valida estado, corridas y ultimos registros impactados del tercer piloto
  - `040_prepare_src_t100_empresa_suc.sql`: prepara `src.t100_empresa_suc` para la siguiente ola
  - `041_seed_pilot_t100_empresa_suc.sql`: inserta la configuracion inicial de `T100_EMPRESA_SUC`
  - `042_validate_pilot_t100_empresa_suc.sql`: valida estado, corridas y ultimos registros impactados de `T100_EMPRESA_SUC`
  - `050_prepare_src_t114_rubros.sql`: prepara `src.t114_rubros` para la siguiente ola
  - `051_seed_pilot_t114_rubros.sql`: inserta la configuracion inicial de `T114_RUBROS`
  - `052_validate_pilot_t114_rubros.sql`: valida estado, corridas y ultimos registros impactados de `T114_RUBROS`
  - `060_prepare_src_t117_compradores.sql`: prepara `src.t117_compradores` para la siguiente ola
  - `061_seed_pilot_t117_compradores.sql`: inserta la configuracion inicial de `T117_COMPRADORES`
  - `062_validate_pilot_t117_compradores.sql`: valida estado, corridas y ultimos registros impactados de `T117_COMPRADORES`
  - `070_prepare_src_t051_articulos_sucursal.sql`: prepara `src.t051_articulos_sucursal` para la segunda ola
  - `071_seed_pilot_t051_articulos_sucursal.sql`: inserta la configuracion inicial de `T051_ARTICULOS_SUCURSAL`
  - `072_validate_pilot_t051_articulos_sucursal.sql`: valida estado, corridas y ultimos registros impactados de `T051_ARTICULOS_SUCURSAL`
  - `080_prepare_src_t020_proveedor_dias_entrega_cabe.sql`: prepara `src.t020_proveedor_dias_entrega_cabe`
  - `081_seed_pilot_t020_proveedor_dias_entrega_cabe.sql`: inserta la configuracion inicial de `T020_PROVEEDOR_DIAS_ENTREGA_CABE`
  - `082_validate_pilot_t020_proveedor_dias_entrega_cabe.sql`: valida estado, corridas y ultimos registros impactados de `T020_PROVEEDOR_DIAS_ENTREGA_CABE`
  - `090_prepare_src_t020_proveedor_dias_entrega_deta.sql`: prepara `src.t020_proveedor_dias_entrega_deta`
  - `091_seed_pilot_t020_proveedor_dias_entrega_deta.sql`: inserta la configuracion inicial de `T020_PROVEEDOR_DIAS_ENTREGA_DETA`
  - `092_validate_pilot_t020_proveedor_dias_entrega_deta.sql`: valida estado, corridas y ultimos registros impactados de `T020_PROVEEDOR_DIAS_ENTREGA_DETA`
  - `030_create_cdc_monitoring_view.sql`: crea una vista consolidada de salud para todos los pilotos CDC
  - `031_validate_cdc_monitoring.sql`: consultas operativas sobre salud, alertas abiertas y ultimas corridas
- `sqlserver/`
  - `001_enable_cdc_t050_articulos.sql`: habilita CDC en SQL Server para `T050_ARTICULOS`
  - `002_validate_cdc_t050_articulos.sql`: validaciones operativas del piloto
  - `010_enable_cdc_t020_proveedor.sql`: habilita CDC en SQL Server para `T020_PROVEEDOR`
  - `011_validate_cdc_t020_proveedor.sql`: validaciones operativas del segundo piloto
  - `020_enable_cdc_t052_articulos_proveedor.sql`: habilita CDC en SQL Server para `T052_ARTICULOS_PROVEEDOR`
  - `021_validate_cdc_t052_articulos_proveedor.sql`: validaciones operativas del tercer piloto
  - `040_enable_cdc_t100_empresa_suc.sql`: habilita CDC en SQL Server para `T100_EMPRESA_SUC`
  - `041_validate_cdc_t100_empresa_suc.sql`: validaciones operativas de `T100_EMPRESA_SUC`
  - `050_enable_cdc_t114_rubros.sql`: habilita CDC en SQL Server para `T114_RUBROS`
  - `051_validate_cdc_t114_rubros.sql`: validaciones operativas de `T114_RUBROS`
  - `060_enable_cdc_t117_compradores.sql`: habilita CDC en SQL Server para `T117_COMPRADORES`
  - `061_validate_cdc_t117_compradores.sql`: validaciones operativas de `T117_COMPRADORES`
  - `070_enable_cdc_t051_articulos_sucursal.sql`: habilita CDC en SQL Server para `T051_ARTICULOS_SUCURSAL`
  - `071_validate_cdc_t051_articulos_sucursal.sql`: validaciones operativas de `T051_ARTICULOS_SUCURSAL`
  - `080_enable_cdc_t020_proveedor_dias_entrega_cabe.sql`: habilita CDC en SQL Server para `T020_PROVEEDOR_DIAS_ENTREGA_CABE`
  - `081_validate_cdc_t020_proveedor_dias_entrega_cabe.sql`: validaciones operativas de `T020_PROVEEDOR_DIAS_ENTREGA_CABE`
  - `090_enable_cdc_t020_proveedor_dias_entrega_deta.sql`: habilita CDC en SQL Server para `T020_PROVEEDOR_DIAS_ENTREGA_DETA`
  - `091_validate_cdc_t020_proveedor_dias_entrega_deta.sql`: validaciones operativas de `T020_PROVEEDOR_DIAS_ENTREGA_DETA`

## Orden sugerido

1. Ejecutar `sqlserver/001_enable_cdc_t050_articulos.sql`.
2. Ejecutar `postgres/001_create_cdc_metadata.sql`.
3. Si el metadata ya existia, ejecutar `postgres/001b_alter_cdc_metadata_add_source_port_env.sql`.
4. Ejecutar `postgres/002_prepare_src_t050_articulos.sql`.
5. Ejecutar `postgres/003_seed_pilot_t050_articulos.sql`.
6. Ejecutar el flujo `scripts/cdc/cdc_replicar_tabla.py`.

## Validacion del piloto T050

Una vez que el piloto este en marcha, ejecutar:

```sql
\i cdc/postgres/004_validate_pilot_t050_articulos.sql
```

## Siguiente tabla candidata

La siguiente tabla preparada para seguir el mismo patron es `T020_PROVEEDOR`.

Orden sugerido:

1. Ejecutar `sqlserver/010_enable_cdc_t020_proveedor.sql`.
2. Ejecutar `sqlserver/011_validate_cdc_t020_proveedor.sql`.
3. Ejecutar `postgres/010_prepare_src_t020_proveedor.sql`.
4. Ejecutar `postgres/011_seed_pilot_t020_proveedor.sql`.
5. Ejecutar el flujo `scripts/cdc/cdc_replicar_tabla.py pilot_t020_proveedor current_max_lsn`.
6. Validar con `postgres/012_validate_pilot_t020_proveedor.sql`.

## Tercer piloto: T052_ARTICULOS_PROVEEDOR

Esta tabla quedo preparada asumiendo como PK operativa `c_proveedor + c_articulo`, que es el patron dominante en los procesos actuales.

Orden sugerido:

1. Ejecutar `sqlserver/020_enable_cdc_t052_articulos_proveedor.sql`.
2. Ejecutar `sqlserver/021_validate_cdc_t052_articulos_proveedor.sql`.
3. Ejecutar `postgres/020_prepare_src_t052_articulos_proveedor.sql`.
4. Ejecutar `postgres/021_seed_pilot_t052_articulos_proveedor.sql`.
5. Ejecutar el flujo `scripts/cdc/cdc_replicar_tabla.py pilot_t052_articulos_proveedor current_max_lsn`.
6. Validar con `postgres/022_validate_pilot_t052_articulos_proveedor.sql`.

## Siguiente ola recomendada

Con la fase 1 cerrada, la siguiente ola preparada en el repo es:

- `T100_EMPRESA_SUC`
- `T114_RUBROS`
- `T117_COMPRADORES`

Orden sugerido por tabla:

1. Ejecutar `sqlserver/040_enable_cdc_t100_empresa_suc.sql`.
2. Ejecutar `sqlserver/041_validate_cdc_t100_empresa_suc.sql`.
3. Ejecutar `postgres/040_prepare_src_t100_empresa_suc.sql`.
4. Ejecutar `postgres/041_seed_pilot_t100_empresa_suc.sql`.
5. Ejecutar el flujo `scripts/cdc/cdc_replicar_tabla.py pilot_t100_empresa_suc current_max_lsn`.
6. Validar con `postgres/042_validate_pilot_t100_empresa_suc.sql`.

Luego repetir el mismo patron para:

- `T114_RUBROS` con `050/051/052`
- `T117_COMPRADORES` con `060/061/062`

Deployments Prefect ya preparados para esta ola:

- `CDC_T100_EMPRESA_SUC_PILOTO`
- `CDC_T114_RUBROS_PILOTO`
- `CDC_T117_COMPRADORES_PILOTO`

Estos deployments usan el mismo patron de ejecucion que fase 1:

- `scripts/cdc/cdc_replicar_tabla.py:<replicar_tabla_cdc>`
- `config_name` correspondiente a cada piloto
- cron `*/5 8-17 * * *`
- `work_queue_name: replicas-dmz`

## Segunda ola preparada

La siguiente tanda ya quedo armada en el repo:

- `T051_ARTICULOS_SUCURSAL`
- `T020_PROVEEDOR_DIAS_ENTREGA_CABE`
- `T020_PROVEEDOR_DIAS_ENTREGA_DETA`

Orden sugerido:

1. Ejecutar `sqlserver/070_enable_cdc_t051_articulos_sucursal.sql`.
2. Ejecutar `sqlserver/071_validate_cdc_t051_articulos_sucursal.sql`.
3. Ejecutar `postgres/070_prepare_src_t051_articulos_sucursal.sql`.
4. Ejecutar `postgres/071_seed_pilot_t051_articulos_sucursal.sql`.
5. Ejecutar `scripts/cdc/cdc_replicar_tabla.py pilot_t051_articulos_sucursal current_max_lsn`.
6. Validar con `postgres/072_validate_pilot_t051_articulos_sucursal.sql`.

Luego repetir el mismo patron para:

- `T020_PROVEEDOR_DIAS_ENTREGA_CABE` con `080/081/082`
- `T020_PROVEEDOR_DIAS_ENTREGA_DETA` con `090/091/092`

Deployments Prefect ya preparados para esta tanda:

- `CDC_T051_ARTICULOS_SUCURSAL_PILOTO`
- `CDC_T020_PROVEEDOR_DIAS_ENTREGA_CABE_PILOTO`
- `CDC_T020_PROVEEDOR_DIAS_ENTREGA_DETA_PILOTO`

Notas operativas:

- `T051_ARTICULOS_SUCURSAL` arranca con `poll_seconds = 600` y cron cada 10 minutos por volumen.
- `T020_PROVEEDOR_DIAS_ENTREGA_CABE` queda con PK `c_proveedor`.
- `T020_PROVEEDOR_DIAS_ENTREGA_DETA` queda con PK `c_proveedor + c_sucu_empr + c_articulo`.
- si alguna tabla falla al crear el indice `UNIQUE`, conviene relevar duplicados antes de activar el piloto.

## Flujo Python

El piloto se ejecuta con:

```powershell
python scripts/cdc/cdc_replicar_tabla.py pilot_t050_articulos
```

El flujo tambien puede usarse desde Prefect con el entrypoint:

```text
scripts/cdc/cdc_replicar_tabla.py:replicar_tabla_cdc
```

Una vez realizado el bootstrap manual inicial, los deployments programados deben ejecutarse sin `bootstrap_mode`.

## Monitoreo fase 1

Para cerrar la fase 1 se agrego un monitor especifico de CDC:

- `scripts/cdc/cdc_monitor.py`: revisa `etl.cdc_table_config`, `etl.cdc_state` y `etl.cdc_run_log`
- deployment Prefect: `CDC_MONITOR_FASE_1`
- vista SQL: `etl.v_cdc_monitor_status`

Reglas base del monitor:

- `critical` si la ultima corrida fallo
- `critical` si una tabla queda mas atrasada que `max(poll_seconds * 3, 15 min)` dentro de la ventana operativa
- `critical` si acumula al menos 2 fallas recientes
- `warning` si sigue en `never_run` o `bootstrapped`
- fuera de la ventana `08:00-18:00` en `America/Argentina/Buenos_Aires`, no dispara atraso

Ejecucion manual:

```powershell
python scripts/cdc/cdc_monitor.py
```

En ejecucion manual el script no corta con error aunque detecte `critical`; solo informa el estado.
En Prefect, el deployment `CDC_MONITOR_FASE_1` sigue configurado para fallar ante alertas criticas.
Al agregar nuevos pilotos en `etl.cdc_table_config`, el monitor los toma automaticamente sin cambios adicionales.

Preparacion SQL sugerida:

1. Ejecutar `postgres/030_create_cdc_monitoring_view.sql`.
2. Validar con `postgres/031_validate_cdc_monitoring.sql`.

Notificaciones:

- el monitor busca `CDC_MONITOR_DISCORD_WEBHOOK`
- si no existe, usa `IOSDB_DISCORD_WEBHOOK`
- si tampoco existe, intenta `DISCORD_WEBHOOK`

## Requisito de ON CONFLICT

Cada tabla `src` usada por el piloto CDC debe tener una PK o un indice `UNIQUE` que coincida exactamente con `pk_columns` definido en `etl.cdc_table_config`.

- `src.t050_articulos`: `UNIQUE (c_articulo)`
- `src.t020_proveedor`: `UNIQUE (c_proveedor)`
- `src.t052_articulos_proveedor`: `UNIQUE (c_proveedor, c_articulo)`
