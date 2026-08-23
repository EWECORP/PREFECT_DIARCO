# Orquestación productiva de ETL_DIARCO

## 1. Objetivo y alcance

Este documento describe la cadena que mueve información desde los sistemas
LEGACY/SGM hacia la DMZ SQL Server, luego hacia `diarco_data` y finalmente hacia
los consumidores analíticos y operativos.

`diarco_data` es una base productiva compartida. La consumen Comprador
Inteligente, FORECAST, PDD y otros procesos. Por lo tanto, una carga PDD no puede
tratarla como una base temporal ni reemplazar una foto válida por una foto
parcial.

La configuración autoritativa de deployments es `prefect.yaml`. El archivo
`prefect - FUTURO.yaml` es un antecedente y no se usa para describir el estado
actual.

## 2. Plataformas y responsabilidades

| Capa | Plataforma | Responsabilidad |
|---|---|---|
| Origen DIARCO | SQL Server SGM y servidores estadísticos | Transacciones, cierre de cadena, ventas DIARCO/BARRIO y maestros LEGACY. |
| DMZ | SQL Server, base `data-sync` | Réplicas `repl.*`, materializaciones derivadas y linked servers hacia los orígenes. |
| Orquestación fuente | Windows Server 2019 + Prefect, proyecto `ETL_DIARCO` | Ejecutar SP, transferir a PostgreSQL, construir fuentes canónicas y auditar el corte. |
| Capa canónica | PostgreSQL `diarco_data` | Fuentes `src`, derivados `datamart` y auditoría `audit`. Es productiva. |
| Analítica PDD | Linux + Prefect, proyecto `diarco-pdd-backend` | Features, PDVB, D/S y publicaciones operativas. No es propietario de la ingesta. |
| Operación Connexa | PostgreSQL `stock_management` en TEST/DESA/PROD | Backlog DECAS, planificación, viajes e integración Valkimia. |

## 3. Cadena nocturna actual

La cadena vigente usa horarios fijos como precedencia implícita:

| Hora ART | Deployment | Salida principal | Dependencia asumida |
|---:|---|---|---|
| 05:00 | `REPL_SYNC_CDC_LOTES_PROD` | Objetos `data-sync.repl.*` | Se supone que SGM ya cerró. |
| 05:30 | `MASTER_SYNC_TABULARES_POSTGRES_PROD` | `src.t710_estadis_stock`, precios y ofertas | Se supone terminada la réplica DMZ. |
| 05:45 | `FORECAST_PUSH_VENTAS_STOCK_DIARIO_PROD` | T702 DIARCO/BARRIO en PostgreSQL | Se supone disponible la estadística de ventas. |
| 05:50 | `MASTER_SYNC_MAESTROS_POSTGRES_PROD` | Maestros y objetos legacy en `src` | Se supone terminada la réplica DMZ. |
| 06:50 lun-vie | `FORECAST_PUSH_INPUT_DATA_PROD` | Stock, OC demoradas, tránsito, surtido y ventas para FORECAST | Se supone que sus dependencias DMZ están completas. |
| 07:05 | `ETL_REFRESH_BVE_DIARIO_PROD` | `src.base_ventas_extendida` | Se supone terminada la T702 PostgreSQL. |
| 13:05 | `MASTER_REFRESH_MAESTROS_POSTGRES_PROD` | Refresco intradiario de maestros | Independiente del cierre nocturno. |
| 13:35 lun-vie | `FORECAST_REFRESH_INPUT_DATA_PROD` | Refresco intradiario de entradas FORECAST | Depende del refresco de maestros por horario. |
| 18:30 | `PDD_SOURCE_DAILY_MASTER_PROD` | Fuentes PDD y contrato `READY/BLOCKED` | Vuelve a ejecutar varias cargas de la mañana. |
| 20:30 | `PDD_OPERATIONAL_DAILY_MASTER` | PDVB, logística, D/S y backlog TEST | Exige auditoría fuente `READY`. Se ejecuta en Linux. |

Problema principal: ningún intervalo de 15, 30 o 60 minutos demuestra que el
deployment anterior terminó correctamente. Si una réplica tarda más, los
procesos se superponen y pueden leer una DMZ parcialmente actualizada.

## 4. Cierre SGM

Las fuentes de control son:

```sql
USE [data-sync];

SELECT
    F_PROXIMO_CIERRE,
    M_TRABAJA_COMO_SABADO
FROM [DIARCOP001].[DiarcoP].[dbo].[T900_FECHA_CIERRE_PROXIMO];

SELECT
    C_PROCESO,
    F_ALTA_SIST
FROM [DIARCOP001].[DiarcoP].[dbo].[T900_CONTROL_PROCESOS_SECUENCIA];
```

La evidencia que habilita la cadena es el último evento `CIERRE`. El campo
`F_PROXIMO_CIERRE` sirve como calendario y control, pero no demuestra por sí
solo que el cierre terminó.

Consulta de diagnóstico de la compuerta:

```sql
USE [data-sync];

WITH ultimo_cierre AS (
    SELECT MAX(F_ALTA_SIST) AS cierre_sgm_at
    FROM [DIARCOP001].[DiarcoP].[dbo].[T900_CONTROL_PROCESOS_SECUENCIA]
    WHERE C_PROCESO = 'CIERRE'
)
SELECT
    f.F_PROXIMO_CIERRE,
    f.M_TRABAJA_COMO_SABADO,
    c.cierre_sgm_at,
    DATEADD(MINUTE, 45, c.cierre_sgm_at) AS habilitado_desde,
    SYSDATETIME() AS fecha_servidor,
    CASE
        WHEN c.cierre_sgm_at IS NULL THEN 'WAITING_CLOSE'
        WHEN SYSDATETIME() < DATEADD(MINUTE, 45, c.cierre_sgm_at)
            THEN 'WAITING_MARGIN'
        ELSE 'ELIGIBLE'
    END AS gate_status
FROM [DIARCOP001].[DiarcoP].[dbo].[T900_FECHA_CIERRE_PROXIMO] AS f
CROSS JOIN ultimo_cierre AS c;
```

La pantalla observada el 22/08/2026 muestra `CIERRE` a las 21:44 y procesos
posteriores hasta aproximadamente las 21:58. Con un margen de 45 minutos, la
cadena sería elegible cerca de las 22:29. El margen cubre ese caso, pero los
procesos posteriores (`RECEP`, `RECEP_DBARRIO`, `PACTU_RESTO`) deben quedar
registrados como diagnóstico hasta confirmar con SGM cuáles son obligatorios
para cada dataset.

### Regla propuesta

1. Consultar cada 5 minutos durante la ventana nocturna.
2. Obtener el último `CIERRE` y compararlo contra el último cierre procesado.
3. Exigir `fecha_actual >= F_ALTA_SIST(CIERRE) + 45 minutos`.
4. Disparar una sola corrida por evento de cierre.
5. Persistir el `cierre_sgm_at`, el estado y el UUID Prefect para idempotencia.
6. Si no existe cierre antes de una hora límite, alertar y conservar la última
   foto productiva; no iniciar automáticamente con datos incompletos.
7. El antiguo horario 05:00 debe quedar inicialmente como watchdog/alerta, no
   como autorización alternativa sin evidencia de cierre.

No conviene que un flow ocupe un worker durante horas esperando. La compuerta
debe ser un deployment corto y recurrente que termine `WAITING`, `TRIGGERED` o
`ALREADY_PROCESSED`.

### Fecha del cierre frente a business_date

El maestro fuente actual, cuando recibe `business_date=null`, usa la fecha
calendario del servidor Windows. Luego exige ventas y T710 hasta
`business_date - 1`.

Si el cierre del día D termina a las 21:44 de D y la nueva cadena comienza a las
22:29 de D, pasar `null` procesaría el corte D-1, no el cierre D recién
terminado. El orquestador anticipado debe resolver y pasar explícitamente:

```text
closed_date = D
business_date = D + 1
cutoff_date = D
```

La regla exacta para obtener D desde `F_PROXIMO_CIERRE` y el evento `CIERRE`
debe confirmarse con SGM mediante las siete corridas en sombra. Hasta entonces
no debe inferirse solamente con `CAST(F_ALTA_SIST AS date)`, porque pueden
existir cierres posteriores a medianoche o calendarios especiales indicados por
`M_TRABAJA_COMO_SABADO`.

## 5. Qué hace realmente REPL_SYNC_CDC_LOTES_PROD

Aunque el nombre contiene `CDC`, el entrypoint
`scripts/repl/flujo_replicar_DMZ_en_LOTES.py:sync_dmz_optimizado` es un batch de
stored procedures sobre SQL Server. No constituye una única transacción ni un
checkpoint CDC global.

### Grupos actuales

1. Rápidos en paralelo: proveedores, artículos, sucursales, rubros,
   compradores, días de entrega, precarga y competencia.
2. Encadenados: artículo-sucursal DIARCO/BARRIO, artículo-proveedor, stock,
   `M_3_ARTICULOS` y OC.
3. Estadísticas pesadas: T710 y ventas T702 DIARCO/BARRIO.
4. Parámetros y condiciones: lead time, parámetros de stock, negocios
   especiales y EAN.
5. Tableros: gestión de compra, SNC, cuotas, uso Connexa y marcas.
6. Competencia y estadística de precios.

### Hallazgos

#### 5.1 Precedencia incorrecta de OC

Actualmente se ejecuta:

```text
T080_OC_PENDIENTES
T080_OC_CABE
T081_OC_DETA
```

`T080_OC_PENDIENTES` es un derivado que depende de cabecera y detalle. El orden
debe ser:

```text
T080_OC_CABE
T081_OC_DETA
T080_OC_PENDIENTES
```

La cadena crítica debe ser fail-fast: si falla cabecera o detalle no debe
reconstruirse el pendiente usando una combinación vieja/nueva.

#### 5.2 Caché sobre tareas con efectos laterales

`ejecutar_sp` usa caché Prefect durante diez minutos. Una tarea que modifica una
base no debería considerarse reutilizable sólo porque recibe el mismo nombre de
SP. Debe conservar reintentos, pero retirar `cache_key_fn` y
`cache_expiration`.

#### 5.3 Estado parcial durante la corrida

Los SP independientes continúan aunque otro falle y el flow falla recién al
final. Esto permite completar objetos no relacionados, pero no debe aplicarse a
una cadena de dependencias. Se deben clasificar los SP en:

- críticos y fail-fast;
- independientes que pueden continuar;
- no críticos, fuera del camino de publicación.

#### 5.4 Duplicación T702

T702 DIARCO/BARRIO se refresca dentro de `REPL_SYNC_CDC_LOTES_PROD` y nuevamente
en `actualizar_bases_ventas`. El segundo camino tiene locks, comparación por
fecha, staging PostgreSQL y publicación atómica; es el propietario técnico más
seguro. Debe definirse un único dueño de T702 y retirar el refresco duplicado
del otro maestro después de una corrida paralela de control.

## 6. DMZ hacia diarco_data

### 6.1 Tabulares

`MASTER_SYNC_TABULARES_POSTGRES_PROD` publica el mes correspondiente de:

- `T710_ESTADIS_STOCK`;
- `T710_ESTADIS_PRECIOS`;
- `T710_ESTADIS_OFERTA_FOLDER`.

El flujo espera la eliminación antes de cargar y propaga errores. Sin embargo,
su fecha se calcula con `datetime.today() - 1 día`; el futuro maestro debe pasar
explícitamente la fecha de negocio derivada del cierre para evitar ambigüedades
en límites de mes.

### 6.2 Ventas

`actualizar_bases_ventas`:

1. refresca las réplicas SQL Server T702 DIARCO y BARRIO;
2. toma tres fechas de solapamiento diario;
3. copia por bloques a staging PostgreSQL;
4. reemplaza las fechas dentro de una transacción;
5. compara fecha, filas, unidades e importe;
6. mantiene ambos canales separados hasta `base_ventas_extendida`.

Es idempotente por fecha y dispone de un lock compartido con la reconciliación
semanal.

### 6.3 Maestros

`MASTER_SYNC_MAESTROS_POSTGRES_PROD` exporta tablas de `repl` y `dbo`. Su
implementación captura la excepción de cada tabla, registra el error y continúa,
pero no falla el flow al final. En consecuencia, Prefect puede mostrar
`Completed` con una o más tablas vacías o atrasadas.

Debe acumular fallos y lanzar una excepción final, como mínimo. Para tablas
productivas debe preferirse staging + intercambio/publicación atómica en lugar
de vaciar el destino antes de demostrar que la nueva copia es válida.

### 6.4 Base de ventas extendida y enriquecida

`ETL_REFRESH_BVE_DIARIO_PROD` construye una staging de 14 días, aplica ofertas,
precios prefijados, factor precio, deduplicación y upsert.

El maestro PDD vuelve a ejecutar este proceso y luego
`datamart.sp_procesar_promos_mes`. El enriquecimiento reconstruye desde el
inicio del mes de corte porque su baseline es mensual. Esta etapa puede crecer
durante el mes y debe medirse para garantizar el SLA del consumidor siguiente.

### 6.5 Stock, surtido, logística y OC

El maestro fuente PDD genera o refresca:

- `src.base_stock_sucursal`;
- `src.base_productos_vigentes`;
- `src.base_articulos_logistica` mediante SCD2;
- `src.mv_base_oc_pendientes` mediante `REFRESH MATERIALIZED VIEW`;
- el contrato auditado en `audit.pdd_source_sync_run/detail`.

El refresh de la vista de OC pertenece al maestro fuente de `ETL_DIARCO`; el
backend PDD consumidor no debe repetirlo.

## 7. Fuentes exigidas por el contrato PDD

El contrato diario controla doce fuentes lógicas:

| Fuente lógica | Relación principal | Criterio resumido |
|---|---|---|
| `RAW_SALES_DIARCO` | `src.t702_est_vtas_por_articulo` | Debe cubrir el corte. |
| `RAW_SALES_BARRIO` | `src.t702_est_vtas_por_articulo_dbarrio` | Debe cubrir el corte. |
| `EXTENDED_SALES` | `src.base_ventas_extendida` | Debe cubrir el corte. |
| `ENRICHED_SALES` | `datamart.dm_bve_ventas_enriquecidas` | Debe cubrir el corte. |
| `HISTORICAL_STOCK` | `src.t710_estadis_stock` | Debe cubrir el corte. |
| `BRANCH_STOCK` | `src.base_stock_sucursal` | Foto operativa vigente y sin stock nulo. |
| `ASSORTMENT` | `src.base_productos_vigentes` | Surtido y ruta de abastecimiento vigentes. |
| `PRODUCT_LOGISTICS` | `src.v_base_articulos_logistica_actual` | Ausencia de peso/volumen es WARN; calidad inválida bloquea. |
| `OPEN_PURCHASE_ORDERS` | `src.mv_base_oc_pendientes` | Debe existir evidencia del refresh del día. |
| `ARTICLE_MASTER` | `src.m_3_articulos` | Referencia obligatoria. |
| `CATEGORY_MASTER` | `src.m_1_categorias` | Referencia obligatoria. |
| `EXCLUDED_BRANCH_POLICY` | `src.sucursales_excluidas` | Política obligatoria. |

Sólo una corrida `READY` puede habilitar el maestro operativo PDD.

## 8. Arquitectura objetivo de precedencia

```text
Evento SGM CIERRE
    ↓ + 45 minutos
Compuerta idempotente de cierre
    ↓
REPL DMZ crítica
    ├─ maestros/artículo-sucursal/stock
    ├─ T080_OC_CABE → T081_OC_DETA → T080_OC_PENDIENTES
    ├─ T710
    └─ parámetros requeridos
    ↓
Publicación PostgreSQL
    ├─ maestros
    ├─ tabulares
    └─ T702 DIARCO/BARRIO
         ↓
    base_ventas_extendida
         ↓
    ventas enriquecidas/baseline
    ↓
Stock + surtido + logística + refresh OC
    ↓
Contrato auditado READY/BLOCKED
    ├─ consumidores de Comprador Inteligente/FORECAST
    └─ maestro operativo PDD
```

Las flechas deben representar espera por estado `Completed` y controles de
salida, no minutos estimados entre cron schedules.

## 9. Propuesta de deployments

### 9.1 Nuevo: SGM_CLOSE_GATE_PROD

- Schedule corto: cada 5 minutos en la ventana nocturna.
- Sólo consulta; no mantiene un worker esperando.
- Emite `WAITING_CLOSE`, `WAITING_MARGIN`, `TRIGGERED` o
  `ALREADY_PROCESSED`.
- Identidad natural: timestamp del último `CIERRE` más la fecha de negocio
  validada.
- Alerta si no hay evento nuevo antes de la hora límite.

### 9.2 Nuevo: ETL_DIARCO_NIGHTLY_MASTER_PROD

Debe recibir como parámetros el evento de cierre y la fecha de negocio. Ejecuta
las fases en orden causal y registra, por etapa:

- inicio/fin;
- estado;
- filas y fecha máxima;
- checksum o agregado de control;
- UUID del flow/subflow;
- mensaje de error.

No debe usar `run_deployment(..., wait=True)` sobre una cola cuyo único worker
está ocupado por el propio maestro. Se deben usar subflows directos, workers con
capacidad suficiente o automations por estado final.

### 9.3 Cambios de schedule

No adelantar directamente el cron 05:00. Primero crear la compuerta y ejecutar
en sombra durante al menos siete cierres, comparando:

- hora de `CIERRE`;
- hora elegible;
- inicio/fin de REPL;
- fechas máximas y conteos finales;
- hora en que `diarco_data` queda `READY`.

Después de validar, el cron fijo se retira o queda únicamente como watchdog.

## 10. Duplicaciones a resolver

| Función | Camino 1 | Camino 2 | Decisión propuesta |
|---|---|---|---|
| Réplica T702 | `REPL_SYNC_CDC_LOTES_PROD` | `actualizar_bases_ventas` | Dejar como dueño al flujo atómico de ventas. |
| Tabulares T710 | 05:30 independiente | Maestro PDD 18:30 | Ejecutar una vez en el maestro nocturno. |
| T702 PostgreSQL | 05:45 independiente | Maestro PDD 18:30 | Ejecutar una vez en el maestro nocturno. |
| Base ventas extendida | 07:05 independiente | Maestro PDD 18:30 | Ejecutar una vez después de T702. |
| Stock/surtido | FORECAST 06:50 | Maestro PDD 18:30 | Separar dataset compartido de exportaciones específicas FORECAST. |
| Maestros CDC/batch | Pilotos directos | Refresh legacy | Retirar gradualmente según `CDC_TRANSICION_REPL_A_SRC_MATRIX.md`. |

No se debe desactivar ningún camino por nombre solamente. Antes del corte hay
que identificar todos sus consumidores y comparar las salidas durante una
ventana controlada.

## 11. Otros deployments de ETL_DIARCO

### 11.1 Intradiarios productivos

- `OC_PUBLISH_PRECARGA_PROD`: outbound hacia SQL Server, cada 10 minutos hábiles.
- `MASTER_REFRESH_MAESTROS_POSTGRES_PROD`: 13:05.
- `FORECAST_REFRESH_INPUT_DATA_PROD`: 13:35 de lunes a viernes.
- `TRANSFER_PUBLISH_VKM_PROD`: 17:35.

No forman parte de la compuerta nocturna, pero deben declararse consumidores o
productores de las tablas compartidas para evitar bloqueos concurrentes.

### 11.2 CDC directo en transición

Los deployments `CDC_*_PILOTO` actualizan durante 08:00–18:00 artículos,
proveedores, sucursales, rubros, compradores, días de entrega, EAN y parámetros
de stock. `CDC_MONITOR_FASE_1` controla su salud.

Inventario configurado:

- `CDC_T050_ARTICULOS_PILOTO`;
- `CDC_T020_PROVEEDOR_PILOTO`;
- `CDC_T052_ARTICULOS_PROVEEDOR_PILOTO`;
- `CDC_T100_EMPRESA_SUC_PILOTO`;
- `CDC_T114_RUBROS_PILOTO`;
- `CDC_T117_COMPRADORES_PILOTO`;
- `CDC_T051_ARTICULOS_SUCURSAL_PILOTO`;
- `CDC_T020_PROVEEDOR_DIAS_ENTREGA_CABE_PILOTO`;
- `CDC_T020_PROVEEDOR_DIAS_ENTREGA_DETA_PILOTO`;
- `CDC_T085_ARTICULOS_EAN_EDI_PILOTO`;
- `CDC_T055_ARTICULOS_PARAM_STOCK_PILOTO`;
- `CDC_T055_ARTICULOS_CONDCOMPRA_COSTOS_PILOTO`;
- `CDC_MONITOR_FASE_1`.

Aunque varios datasets están estables, los nombres y tags continúan indicando
piloto. Su promoción y la retirada del batch legacy deben seguir la matriz
`CDC_TRANSICION_REPL_A_SRC_MATRIX.md`.

### 11.3 LAB y manuales

Los deployments `IOSDB_*_LAB`, `PDD_*_MANUAL`, `envio-manual-tabla`,
`push_tablas_dmz_a_postgres` y `PAS_PUSH_OC_RECEPCION_*` no deben incorporarse
automáticamente al camino crítico sin una decisión específica.

LAB con schedule actual:

- `IOSDB_PRODUCTS_LAB`: 07:00;
- `IOSDB_STOCK_MAYORISTA_LAB`: 08:00;
- `IOSDB_STOCK_BARRIO_LAB`: 09:00;
- `IOSDB_RETRY_LAB`: 10:30.

Deployments sin schedule o de ejecución controlada:

- `exportar_tabla_sql_sftp`;
- `push_tablas_dmz_a_postgres`;
- `PDD_ENRICHED_SALES_REPROCESS_MANUAL`;
- `envio-manual-tabla`;
- `PDD_BASE_ARTICULOS_LOGISTICA_MANUAL`;
- `PDD_SOURCE_READINESS_MANUAL`;
- `IOSDB_CATEGORIES_MANUAL`;
- `IOSDB_CADENA_MANUAL`;
- `IOSDB_INITIAL_PRODUCTS_MANUAL`;
- `IOSDB_MASTER_MANUAL`;
- `PAS_PUSH_OC_RECEPCION_TEST`;
- `PAS_PUSH_OC_RECEPCION_DESA`;
- `PAS_PUSH_OC_RECEPCION_PROD`.

La reconciliación `PDD_SALES_RECONCILIATION_WEEKLY` permanece separada los
domingos a las 10:00 y comparte lock con la actualización diaria de ventas.

## 12. Controles de aceptación

Una cadena nocturna se considera exitosa solamente si:

1. existe un evento `CIERRE` nuevo y se cumplió el margen;
2. todas las etapas críticas finalizaron correctamente;
3. DIARCO y BARRIO cubren la fecha esperada;
4. T710 cubre el mismo cierre;
5. `base_ventas_extendida` y ventas enriquecidas concilian con las ventas crudas;
6. stock no contiene valores nulos para el universo requerido;
7. surtido, logística y OC tienen evidencia de actualización;
8. `audit.pdd_source_sync_run.status = 'READY'`;
9. una falla conserva las últimas tablas publicadas válidas;
10. no existe más de una corrida activa para el mismo evento de cierre.

Control del último contrato:

```sql
SELECT
    source_sync_run_uuid,
    business_date,
    cutoff_date,
    status,
    refresh_mode,
    common_closed_date,
    recommended_business_date,
    source_count,
    ready_count,
    warning_count,
    blocker_count,
    started_at,
    finished_at,
    error_message
FROM audit.pdd_source_sync_run
ORDER BY started_at DESC
LIMIT 10;
```

## 13. Plan de implementación

### Prioridad 0: antes de adelantar el horario

1. Medir siete cierres SGM y confirmar la relación entre
   `F_PROXIMO_CIERRE`, el evento `CIERRE` y la fecha comercial.
2. Corregir el orden de OC en `REPL_SYNC_CDC_LOTES_PROD`.
3. Retirar la caché de ejecución de SP con efectos laterales.
4. Hacer que `MASTER_SYNC_MAESTROS_POSTGRES_PROD` falle si una tabla falla.
5. Registrar duración y salida por etapa.

### Prioridad 1: precedencia causal

1. Implementar `SGM_CLOSE_GATE_PROD` en modo observación.
2. Implementar `ETL_DIARCO_NIGHTLY_MASTER_PROD`.
3. Ejecutarlo en sombra sin desactivar los schedules actuales.
4. Comparar fechas, filas, unidades, importes y checksums.

### Prioridad 2: simplificación

1. Elegir un único propietario para T702.
2. Eliminar ejecuciones duplicadas de T710, BVE y enriquecimiento.
3. Separar fuentes compartidas de exportaciones específicas FORECAST.
4. Retirar gradualmente refresh legacy ya cubierto por CDC estable.

### Prioridad 3: activación

1. Promover la compuerta a disparador autoritativo.
2. Convertir el cron 05:00 en watchdog.
3. Notificar `BLOCKED`, atrasos y duración anómala.
4. Mantener un procedimiento manual de reanudación por etapa, sin repetir
   etapas ya publicadas correctamente.
