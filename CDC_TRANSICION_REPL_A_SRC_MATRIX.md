# Matriz de Transicion `repl` -> `src`

## Objetivo

Ordenar el retiro gradual del esquema legacy `repl` para las tablas que ya quedaron estables con CDC directo sobre `src`.

La idea no es apagar todo junto, sino identificar:

- que procesos todavia leen `repl`
- cuales de esos procesos ya podrian leer `src`
- que dependencias siguen obligando a mantener componentes legacy

## Regla de decision

- si una tabla ya esta estable en CDC y el consumidor solo necesita esa tabla o combinaciones ya migradas, conviene moverlo a `src`
- si el proceso mezcla tablas ya migradas con otras todavia no migradas, conviene dejarlo en modo hibrido
- si el artefacto es manual, exploratorio o auxiliar, queda en baja prioridad

## Estado base

Tablas ya estabilizadas en CDC:

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

Tablas u objetos que siguen condicionando una migracion completa de algunos procesos:

- `T060_STOCK`
- `T061_STOCK_DIARIO`
- `T051_ARTICULOS_SUCURSAL_BARRIO`
- `T080_OC_CABE`
- `T081_OC_DETA`
- `T080_OC_PENDIENTES` como dataset derivado en DMZ
- `T710_ESTADIS_*`

## Inventario DMZ de stored procedures

Los objetos de `scripts/sql/` agregan una capa intermedia importante en la DMZ. No todos son equivalentes:

- algunos consumen tablas legacy o CDC y publican datasets derivados que luego se envian a PostgreSQL
- otros materializan tablas `repl` auxiliares que no existen fisicamente en el sistema transaccional

| SP DMZ | Archivo | Salida principal | Dependencias relevantes | Tipo | Nota de transicion |
|---|---|---|---|---|---|
| `dbo.SP_BASE_PRODUCTOS_DMZ` | `scripts/sql/sp_SP_BASE_PRODUCTOS_DMZ.sql` | dataset de productos para `base_productos_vigentes` | `T050`, `T051`, `T052`, `T100`, `T060`, `T051_*_BARRIO`, `T804_*` | derivado de negocio | es el SP efectivamente ejecutado hoy por `obtener_base_productos_vigentes.py` |
| `dbo.SP_BASE_PRODUCTOS_SUCURSAL` | `scripts/sql/sp_SP_BASE_PRODUCTOS_SUCURSAL.sql` | variante / antecedente de base productos | `T050`, `T051`, `T052`, `T100`, `T060`, `T020_PROVEEDOR_DIAS_ENTREGA_DETA` | derivado de negocio | sirve como referencia de dependencias, aunque no sea el camino principal actual |
| `dbo.SP_BASE_STOCK_DMZ` | `scripts/sql/sp_SP_BASE_STOCK_DMZ.sql` | dataset de stock enriquecido | `T080_OC_PENDIENTES`, `T055_*`, `T020_PROVEEDOR_DIAS_ENTREGA_DETA`, `T060`, `T050`, `T100`, `T051`, `T710_*` | derivado de negocio | fuerte dependencia de objetos legacy y derivados |
| `dbo.SP_BASE_STOCK_EXTEND` | `scripts/sql/sp_SP_BASE_STOCK_EXTEND.sql` | dataset consumido por `obtener_base_stock.py` | `T080_OC_PENDIENTES`, `T055_*`, `T020_PROVEEDOR_DIAS_ENTREGA_DETA`, `T060`, `T050`, `T100`, `T051`, `T710_*` | derivado de negocio | hoy es el camino operativo para la base de stock |
| `dbo.SP_BASE_PRODUCTOS_EN_TRANSITO` | `scripts/sql/sp_SP_BASE_PRODUCTOS_EN_TRANSITO.sql` | dataset de transito | tablas `dbo` directas en servidores origen | derivado de negocio | no es prioridad para el corte `repl -> src` |
| `repl.usp_replicar_T080_OC_PENDIENTES` | `scripts/sql/sp_SP_T080_OC_PENDIENTES.sql` | `repl.T080_OC_PENDIENTES` | `T080_OC_CABE`, `T081_OC_DETA`, `T050`, `T052` | materializacion legacy derivada | no debe modelarse como tabla CDC; se mantiene como artefacto derivado en DMZ |

## Matriz por proceso

| Proceso | Archivo / objeto | Tablas CDC involucradas | Origen actual | Origen objetivo | Decision recomendada | Prioridad | Observaciones |
|---|---|---|---|---|---|---|---|
| Replica legacy DMZ | `scripts/repl/flujo_replicar_DMZ_en_LOTES.py` | `T020`, `T050`, `T051`, `T052`, `T055`, `T085`, `T100`, `T114`, `T117` | `SQL Server -> repl` via SP | dejar solo tablas no migradas | recortar SP de tablas ya cubiertas por CDC cuando no tengan consumidores activos en `repl` | Alta | es el productor legacy principal |
| Refresh de maestras | `scripts/send/actualizar_tablas_maestras.py` | `T050`, `T020`, `T052`, `T100`, `T114`, `T117`, `T020_*`, `T051` | exporta desde `repl` hacia `src` | consumir `src` directo o eliminar ese paso | sacar de la lista batch las tablas ya cubiertas por CDC | Alta | hoy duplica funcionalidad ya resuelta por CDC |
| Refresh legacy adicional | `scripts/send/refresh_tablas_maestras.py` | `T050`, `T020`, `T052`, `T051` | SP sobre `repl` + envio a Postgres | flujo partido: dejar solo legacy real | retirar tablas CDC de este flujo | Alta | fuerte candidato a simplificacion |
| Base productos vigentes | `scripts/push/obtener_base_productos_vigentes.py` + `scripts/sql/sp_SP_BASE_PRODUCTOS_DMZ.sql` | `T050`, `T051`, `T052`, `T100`, `T060` | SP SQL Server en DMZ mezclando `repl` y tablas derivadas | version nueva leyendo `src` + remanentes legacy | migrar despues de definir estrategia para `T060_STOCK` y `T051_*_BARRIO` | Media | el ejecutable actual usa `SP_BASE_PRODUCTOS_DMZ`, no `SP_BASE_PRODUCTOS_SUCURSAL` |
| Base stock | `scripts/push/obtener_base_stock.py` + `scripts/sql/sp_SP_BASE_STOCK_EXTEND.sql` | `T050`, `T051`, `T055_*`, `T100`, `T020_PROVEEDOR_DIAS_ENTREGA_DETA` | SP SQL Server en DMZ leyendo `repl` + `T080_OC_PENDIENTES` | fase hibrida | mantener en legacy hasta resolver `T060`, `T710_*` y `T080_OC_PENDIENTES` | Alta | concentra varias dependencias operativas aun no migradas |
| Materializacion OC pendientes | `scripts/sql/sp_SP_T080_OC_PENDIENTES.sql` | `T050`, `T052` | dataset derivado en `repl` | mantener derivado en DMZ | no migrar por CDC; redisenar luego como proceso derivado | Alta | `T080_OC_PENDIENTES` no existe como tabla fisica en el legacy |
| Script auxiliar de consulta | `scripts/zvarios/sp.sql` | `T050`, `T051`, `T055`, `T100` | `repl` | `src` cuando se reutilice | no prioritario | Baja | artefacto manual / exploratorio |
| Base ventas extendida | `scripts/push/actualizar_base_ventas_extendida.py` y variantes | `T050` | `src` | `src` | sin accion | Nula | ya esta alineado con la arquitectura objetivo |
| SQL apoyo ventas | `scripts/push/PG_Actualizar_Base_Ventas.sql` | `T050` | `src` | `src` | sin accion | Nula | ya usa capa canonica |
| IOSdb | `IOSdb/flows/*/query.py` | `T055_ARTICULOS_CONDCOMPRA_COSTOS` y otras | `dbo` directo en SQL Server | definir aparte | fuera del alcance de esta matriz inicial | Media | no depende de `repl`, pero tampoco de `src` |

## Matriz por tabla

| Tabla | Estado CDC | Dependencias `repl` detectadas | Siguiente accion |
|---|---|---|---|
| `T050_ARTICULOS` | estable | `actualizar_tablas_maestras.py`, `refresh_tablas_maestras.py`, `SP_BASE_PRODUCTOS_DMZ.sql`, `SP_BASE_PRODUCTOS_SUCURSAL.sql`, `SP_BASE_STOCK_EXTEND.sql`, `sp_SP_T080_OC_PENDIENTES.sql`, `scripts/zvarios/sp.sql` | retirar de refresh batch y planificar migracion de los SP DMZ |
| `T020_PROVEEDOR` | estable | `actualizar_tablas_maestras.py`, `refresh_tablas_maestras.py` | retirar de refresh batch |
| `T052_ARTICULOS_PROVEEDOR` | estable | `actualizar_tablas_maestras.py`, `refresh_tablas_maestras.py`, `SP_BASE_PRODUCTOS_DMZ.sql`, `SP_BASE_PRODUCTOS_SUCURSAL.sql`, `sp_SP_T080_OC_PENDIENTES.sql` | retirar de refresh batch y migrar con los SP DMZ |
| `T100_EMPRESA_SUC` | estable | `actualizar_tablas_maestras.py`, `SP_BASE_PRODUCTOS_DMZ.sql`, `SP_BASE_PRODUCTOS_SUCURSAL.sql`, `SP_BASE_STOCK_EXTEND.sql`, `scripts/zvarios/sp.sql` | retirar de batch y migrar con los SP DMZ |
| `T114_RUBROS` | estable | `actualizar_tablas_maestras.py` | retirar de refresh batch |
| `T117_COMPRADORES` | estable | `actualizar_tablas_maestras.py` | retirar de refresh batch |
| `T051_ARTICULOS_SUCURSAL` | estable | `actualizar_tablas_maestras.py`, `SP_BASE_PRODUCTOS_DMZ.sql`, `SP_BASE_PRODUCTOS_SUCURSAL.sql`, `SP_BASE_STOCK_EXTEND.sql`, `scripts/zvarios/sp.sql` | migrar en conjunto con la estrategia de base productos y stock |
| `T020_PROVEEDOR_DIAS_ENTREGA_CABE` | estable | `actualizar_tablas_maestras.py` | retirar de refresh batch |
| `T020_PROVEEDOR_DIAS_ENTREGA_DETA` | estable | `actualizar_tablas_maestras.py`, `SP_BASE_PRODUCTOS_SUCURSAL.sql`, `SP_BASE_STOCK_EXTEND.sql` | retirar de batch y migrar con los SP DMZ |
| `T085_ARTICULOS_EAN_EDI` | estable | no se detectaron consumidores `repl` activos en procesos principales | mantener monitoreo, sin corte urgente |
| `T055_ARTICULOS_PARAM_STOCK` | estable | no se detectaron consumidores `repl` activos en procesos principales | mantener monitoreo, sin corte urgente |
| `T055_ARTICULOS_CONDCOMPRA_COSTOS` | estable | `SP_BASE_STOCK_EXTEND.sql`, `scripts/zvarios/sp.sql` | baja prioridad en transicion; revisar junto con stock e IOSdb por uso directo a `dbo` |
| `T080_OC_PENDIENTES` | derivada en DMZ | `SP_BASE_STOCK_DMZ.sql`, `SP_BASE_STOCK_EXTEND.sql`, `actualizar_tablas_maestras.py`, `refresh_tablas_maestras.py` | mantener como artefacto derivado y separar su estrategia de las tablas CDC |

## Sprint sugerido

### Sprint 1

- quitar de `scripts/send/actualizar_tablas_maestras.py` las tablas ya cubiertas por CDC y de menor riesgo:
  - `T020_PROVEEDOR`
  - `T100_EMPRESA_SUC`
  - `T114_RUBROS`
  - `T117_COMPRADORES`
  - `T020_PROVEEDOR_DIAS_ENTREGA_CABE`

Estado:

- aplicado en `scripts/send/actualizar_tablas_maestras.py`

### Sprint 2

- quitar de `scripts/send/refresh_tablas_maestras.py`:
  - `T050_ARTICULOS`
  - `T020_PROVEEDOR`
  - `T052_ARTICULOS_PROVEEDOR`

Estado:

- aplicado en `scripts/send/refresh_tablas_maestras.py`

### Sprint 3

- rediseñar `SP_BASE_PRODUCTOS_DMZ.sql` para que lea desde `src` en `T050`, `T051`, `T052`, `T100`
- mantener en SQL Server / DMZ solo las partes que hoy dependen de `T060_STOCK`, `T051_*_BARRIO` o tablas derivadas
- en paralelo, mantener `T060_STOCK` fuera del corte hasta definir su estrategia

Estado:

- disponible un primer esqueleto en `scripts/push/obtener_base_productos_vigentes.py` con `BASE_PRODUCTOS_SOURCE_MODE=hybrid_src`
- el modo por defecto sigue siendo `sqlserver_sp` para conservar rollback simple

### Sprint 4

- revisar `SP_BASE_STOCK_EXTEND.sql` y `SP_BASE_STOCK_DMZ.sql`
- separar dependencias en tres grupos:
  - tablas ya migradas a CDC que pueden venir de `src`
  - tablas aun legacy (`T060`, `T710_*`)
  - datasets derivados DMZ (`T080_OC_PENDIENTES`)
- definir si `T080_OC_PENDIENTES` sigue materializada en `repl` o pasa a una capa derivada propia

## Criterio de cierre por tabla

Una tabla puede considerarse retirada de `repl` cuando:

1. su flujo CDC lleva varios dias estable
2. el consumidor principal ya lee `src`
3. no quedan refresh o SP legacy obligatorios para esa tabla
4. existe rollback simple al camino anterior por unos dias
