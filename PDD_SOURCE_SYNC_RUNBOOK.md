# Sincronizacion diaria de fuentes PDD en diarco_data

## Objetivo

`ETL_DIARCO` es propietario de la ingesta y de las fuentes canonicas. El backend
PDD transforma y publica datos, pero no debe decidir si una carga upstream fue
exitosa solamente porque una tabla tiene filas.

El deployment `PDD_SOURCE_DAILY_MASTER_PROD` actualiza las fuentes PDD, persiste
la evidencia en `audit.pdd_source_sync_*` y finaliza `READY` o `BLOCKED`. La
reconciliacion historica se ejecuta por separado mediante
`PDD_SALES_RECONCILIATION_WEEKLY`; nunca publica un contrato global de fuentes.

## Orden diario

| Orden | Fuente/salida | Proceso |
|---:|---|---|
| 1 | `src.t710_estadis_*` | Recarga del mes actual de las tablas tabulares. |
| 2 | `data-sync.repl.T702_*` | Los SP de DIARCO y BARRIO reconstruyen los tres días recientes desde sus fuentes SGM. |
| 3 | `src.t702_est_vtas_por_articulo*` | Reemplazo atómico por fecha mediante staging; DIARCO y BARRIO permanecen físicamente separados. |
| 4 | `src.base_ventas_extendida` | Upsert idempotente de 14 días o reproceso ampliado si se reparó una fecha antigua. |
| 5 | `datamart.dm_bve_ventas_enriquecidas` | `datamart.sp_procesar_promos_mes`, ampliando el rango cuando existieron reparaciones antiguas. |
| 6 | `src.base_stock_sucursal` | Foto operativa de stock sucursal/CD. |
| 7 | `src.base_productos_vigentes` | Surtido y forma de abastecimiento artículo/sucursal. |
| 8 | `src.base_articulos_logistica` | Snapshot completo SCD2 desde el SP contractual. |
| 9 | `src.mv_base_oc_pendientes` | `REFRESH MATERIALIZED VIEW` bajo lock asesor. |
| 10 | `audit.pdd_source_sync_*` | Validación y evidencia por fuente. |

Los maestros `src.m_3_articulos`, `src.m_1_categorias` y la política
`src.sucursales_excluidas` se validan como referencias obligatorias. Su ingesta
sigue perteneciendo a los flujos generales/CDC existentes.

## Semántica de fechas

Para una fecha operativa `business_date`:

- ventas crudas, ventas enriquecidas y T710 deben cubrir hasta
  `business_date - 1`;
- `src.base_stock_sucursal` debe tener una foto con fecha igual o posterior a
  `business_date`;
- la vista de OC debe tener evidencia auditada de refresh durante `business_date`;
- si esa evidencia no existe se informa
  `OPEN_PURCHASE_ORDERS:REFRESH_NOT_PROVEN`; este código no significa que la
  vista esté vacía, sino que su actualización no pudo demostrarse;
- `common_closed_date` es el mínimo cierre entre ventas crudas, ventas
  enriquecidas y T710;
- `recommended_business_date = common_closed_date + 1`.

## Ventas tardías y reconciliación

La sincronización de ventas utiliza dos niveles complementarios:

- diariamente transfiere exactamente el intervalo que cada procedimiento de
  SQL Server reconstruyó. Cuando aparece un día nuevo, esto normalmente incluye
  los tres días que ya existían más el nuevo día;
- diariamente el maestro ejecuta solamente el solapamiento de tres dias;
- los domingos a las 10:00, `PDD_SALES_RECONCILIATION_WEEKLY` compara 45 días
  en ambos saltos: servidor original contra `data-sync.repl`, y luego
  `data-sync.repl` contra PostgreSQL.

La comparación semanal utiliza fecha, cantidad de registros, unidades e importe.
Primero repara selectivamente la réplica SQL Server y luego PostgreSQL; esto evita
que dos copias igualmente atrasadas se consideren consistentes. Sólo los días
distintos se agregan al conjunto de reparación. Las filas se
transfieren directamente y por bloques desde SQL Server a `src._stg_t702_*` con
`COPY`; no intervienen archivos ZIP ni SFTP. Luego se realiza el `DELETE + INSERT`
dentro de una única transacción PostgreSQL. Si la transferencia, el `COPY` o el
control posterior fallan, la versión productiva anterior queda intacta.

DIARCO y BARRIO se tratan como dos canales físicos independientes durante toda
la réplica. No se decide el origen aplicando rangos de sucursal: la integración
recién ocurre en `base_ventas_extendida`, donde la sucursal conserva la
separación lógica de los registros.

Los procedimientos `repl.usp_replicar_T702_*` se ejecutan en la base SQL Server
definida por `SQL_REPL_DATABASE`. Si la variable no existe, se utiliza
explícitamente `data-sync`; no se reutiliza `SQL_DATABASE`, porque esa variable
general puede apuntar a otra base operativa.

Cuando aparece una diferencia anterior a la ventana normal de 14 días, el mismo
maestro amplía automáticamente el reproceso de `base_ventas_extendida` y
`dm_bve_ventas_enriquecidas` hasta la fecha reparada más antigua.

Para forzar excepcionalmente una revisión de 120 días se utiliza el deployment
semanal, sin convertir esa corrida en el último contrato `READY/BLOCKED`:

```powershell
$env:PREFECT_API_URL = "https://orquestador.connexa-cloud.com/api"
prefect deployment run `
    "PDD - Reconciliar ventas semanal/PDD_SALES_RECONCILIATION_WEEKLY" `
    --param "sales_reconciliation_days=120" `
    --param "created_by=eduardo.ettlin" `
    --watch
```

Peso y volumen logísticos faltantes producen `WARN`, no `BLOCKED`. Una calidad
logística `INVALID`, stock físico nulo o una fuente analítica atrasada sí bloquean.

## Migración inicial

En una terminal PowerShell nueva, las variables configuradas dentro del servicio
Windows no se heredan automáticamente. Si están almacenadas en `.env`, pueden
cargarse en el proceso actual con:

```powershell
Get-Content ".env" | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
        $name, $value = $line.Split("=", 2)
        $value = $value.Trim().Trim('"').Trim("'")
        Set-Item -Path "Env:$($name.Trim())" -Value $value
    }
}
```

Primero actualizar una sola vez los dos procedimientos en SQL Server `data-sync`:

```powershell
$sqlReplicaDatabase = if ($env:SQL_REPL_DATABASE) {
    $env:SQL_REPL_DATABASE
} else {
    "data-sync"
}

sqlcmd `
    -S $env:SQL_SERVER -d $sqlReplicaDatabase `
    -U $env:SQL_USER -P $env:SQL_PASSWORD -b `
    -i "scripts\sql\PDD_UPGRADE_T702_REPLICA_ATOMICA_V2.sql"
```

El parámetro `-b` es obligatorio para devolver código de error si alguno de los
dos `CREATE OR ALTER` falla. También puede ejecutarse el archivo completo desde
SSMS con un usuario autorizado.

Después ejecutar una sola vez en `diarco_data`:

```powershell
$env:PGPASSWORD = $env:PG_PASSWORD

psql `
    -h $env:PG_HOST -p $env:PG_PORT -U $env:PG_USER -d $env:PG_DB `
    -v ON_ERROR_STOP=1 `
    -f "scripts\sql\pdd\002_create_pdd_source_sync_audit.sql"
```

La migración debe ejecutarse con el mismo usuario fuente configurado como
`PG_USER` en `ETL_DIARCO` y en PDD, o debe otorgársele acceso equivalente. En el
entorno actual ambos proyectos usan la misma identidad lógica. Control:

```sql
SELECT
    current_user,
    has_table_privilege(
        current_user,
        'audit.pdd_source_sync_run',
        'SELECT,INSERT,UPDATE'
    ) AS acceso_run,
    has_table_privilege(
        current_user,
        'audit.pdd_source_sync_detail',
        'SELECT,INSERT'
    ) AS acceso_detail;
```

## Despliegue Prefect

Ejecutar desde PowerShell, situado en la raíz de `ETL_DIARCO`. Si corresponde,
activar primero el entorno virtual instalado en el servidor:

```powershell
Set-Location "E:\ETL\ETL_DIARCO"
.\venv\Scripts\Activate.ps1
```

Si el entorno virtual está en otra ruta, utilizar la ruta real del worker.
Antes de cualquier comando Prefect:

```powershell
$env:PREFECT_API_URL = "https://orquestador.connexa-cloud.com/api"
```

Validar el código con el mismo entorno virtual del worker:

```powershell
python -m pip check
python -m py_compile `
    "scripts\send\actualizar_bases_ventas.py" `
    "scripts\pdd\pdd_source_daily.py"
python -m pytest -q tests
```

Desplegar únicamente los tres deployments PDD de fuentes, evitando modificar
los demás deployments existentes en `ETL_DIARCO`:

```powershell
prefect deploy --name PDD_SOURCE_READINESS_MANUAL
prefect deploy --name PDD_SOURCE_DAILY_MASTER_PROD
prefect deploy --name PDD_SALES_RECONCILIATION_WEEKLY
```

Comprobar que los tres apuntan al pool `dmz-diarco` y al directorio Windows:

```powershell
prefect deployment inspect `
    "PDD - Sincronizar fuentes diarco_data/PDD_SOURCE_READINESS_MANUAL"

prefect deployment inspect `
    "PDD - Sincronizar fuentes diarco_data/PDD_SOURCE_DAILY_MASTER_PROD"

prefect deployment inspect `
    "PDD - Reconciliar ventas semanal/PDD_SALES_RECONCILIATION_WEEKLY"
```

El maestro diario queda programado a las `18:30` de Argentina y sólo ejecuta el
solapamiento de tres días. La reconciliación de 45 días queda programada los
domingos a las `10:00`, con tiempo suficiente para terminar antes del maestro
diario y sin alterar la última auditoría global. Ambos comparten un lock asesor y
no pueden modificar ventas simultáneamente. El maestro operativo PDD puede
continuar a las `20:30`.

Primera reconciliación controlada:

```powershell
$env:PREFECT_API_URL = "https://orquestador.connexa-cloud.com/api"
prefect deployment run `
    "PDD - Reconciliar ventas semanal/PDD_SALES_RECONCILIATION_WEEKLY" `
    --param "business_date=2026-08-22" `
    --param "sales_reconciliation_days=120" `
    --param "created_by=eduardo.ettlin" `
    --watch
```

La reconciliación ampliada se usa en esta primera corrida para reemplazar el
control manual inicial. Las ejecuciones programadas siguientes utilizan 45 días;
el maestro diario permanece siempre limitado al solapamiento de tres días.

Diagnóstico sin modificar fuentes:

```powershell
$env:PREFECT_API_URL = "https://orquestador.connexa-cloud.com/api"
$params = @{
    business_date = "2026-08-22"
    created_by = "eduardo.ettlin"
} | ConvertTo-Json -Compress

prefect deployment run `
    "PDD - Sincronizar fuentes diarco_data/PDD_SOURCE_READINESS_MANUAL" `
    --params $params `
    --watch
```

## Control SQL

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

Fechas distintas y reparadas en la conciliación de ventas:

```sql
SELECT
    source_sync_run_uuid,
    summary #> '{sales_sync,sql_replicas,DIARCO,historical_mismatch_dates}'
        AS diarco_origen_vs_replica,
    summary #> '{sales_sync,sql_replicas,BARRIO,historical_mismatch_dates}'
        AS barrio_origen_vs_replica,
    summary #> '{sales_sync,sources,DIARCO,mismatch_dates}'
        AS diarco_replica_vs_postgres,
    summary #> '{sales_sync,sources,BARRIO,mismatch_dates}'
        AS barrio_replica_vs_postgres,
    summary #> '{sales_sync,repaired_dates}' AS fechas_reprocesadas
FROM audit.pdd_source_sync_run
ORDER BY started_at DESC
LIMIT 10;
```

```sql
SELECT
    d.source_code,
    d.physical_relation,
    d.status,
    d.max_business_date,
    d.as_of_ts,
    d.row_count,
    d.blocker_codes,
    d.warning_codes,
    d.detail
FROM audit.pdd_source_sync_detail AS d
WHERE d.source_sync_run_uuid = '<UUID_CORRIDA>'::uuid
ORDER BY d.source_code;
```

El backend PDD sólo debe comenzar con la última corrida del día en `READY`. El
scope de distribución continúa versionado y congelado: una actualización de
`base_productos_vigentes` no crea automáticamente una nueva versión del scope.

El refresh de `src.mv_base_oc_pendientes` deja de ejecutarse dentro del maestro
PDD consumidor: desde esta versión pertenece exclusivamente al maestro fuente de
`ETL_DIARCO`. La función anterior permanece disponible sólo por compatibilidad,
pero el orquestador diario no la invoca.

Desde `diarco-pdd-backend 0.17.2`, el maestro operativo consulta explícitamente
`audit.pdd_source_sync_run`. Si la última auditoría no corresponde a su misma
`business_date` o no está `READY`, el pipeline de las 20:30 se bloquea antes de
generar features, estimaciones o publicaciones.

El orden de puesta en servicio es:

1. aplicar `PDD_UPGRADE_T702_REPLICA_ATOMICA_V2.sql` en SQL Server;
2. aplicar la migración `002_create_pdd_source_sync_audit.sql` en PostgreSQL;
3. desplegar y probar `PDD_SOURCE_READINESS_MANUAL`;
4. desplegar `PDD_SOURCE_DAILY_MASTER_PROD` y
   `PDD_SALES_RECONCILIATION_WEEKLY` en `ETL_DIARCO`;
5. instalar `diarco-pdd-backend 0.17.2` y reiniciar su worker;
6. ejecutar una corrida fuente completa y comprobar `READY`;
7. recién entonces habilitar/ejecutar el maestro operativo PDD de las 20:30.
