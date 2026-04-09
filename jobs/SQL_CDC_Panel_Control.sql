/****************************************************************************
🖥️ PANEL de CONTROL - CDC Change Data Capture -  PARA EJECUTAR DESDE SSMS   * 
*****************************************************************************/

--🔵 1. Estado general del CDC

-- ESTADO GENERAL DEL CDC
SELECT 
    name AS DatabaseName,
    is_cdc_enabled AS CDC_Enabled,
    recovery_model_desc AS RecoveryModel,
    create_date
FROM sys.databases
WHERE name = DB_NAME();

--🔵 2. Tablas CDC habilitadas

-- TABLAS CDC HABILITADAS
SELECT 
    capture_instance,
    source_object_id,
    object_id,
    start_lsn,
    end_lsn,
    filegroup_name,
    index_name
FROM cdc.change_tables;

--🔵 3. Estado de los jobs CDC (capture / cleanup)

-- ESTADO DE LOS JOBS CDC
SELECT 
    j.name AS JobName,
    j.enabled,
    ja.run_requested_date,
    ja.stop_execution_date,
    ja.last_executed_step_id,
    ja.last_executed_step_date
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobactivity ja ON j.job_id = ja.job_id
WHERE j.name LIKE 'cdc_%';

--🔵 4. LSNs actuales

-- LSNs ACTUALES
SELECT 
    sys.fn_cdc_get_min_lsn('dbo_T080_OC_CABE') AS MIN_LSN_CABE,
    sys.fn_cdc_get_min_lsn('dbo_T081_OC_DETA') AS MIN_LSN_DETA,
    sys.fn_cdc_get_max_lsn() AS MAX_LSN;

--🔵 5. Cantidad de cambios por tabla (últimos 1000 LSNs)

-- CANTIDAD DE CAMBIOS POR TABLA
-- Tipo de operación (1=delete, 2=insert, 3=update-before, 4=update-after)

DECLARE @max_lsn BINARY(10) = sys.fn_cdc_get_max_lsn();

SELECT TOP 1000
    'CABECERA' AS Tabla,
    __$operation AS Operacion,
    COUNT(*) AS Cantidad
FROM cdc.fn_cdc_get_all_changes_dbo_T080_OC_CABE(
        sys.fn_cdc_get_min_lsn('dbo_T080_OC_CABE'),
        @max_lsn,
        'all'
    )
GROUP BY __$operation
ORDER BY Operacion;

SELECT TOP 1000
    'DETALLE' AS Tabla,
    __$operation AS Operacion,
    COUNT(*) AS Cantidad
FROM cdc.fn_cdc_get_all_changes_dbo_T081_OC_DETA(
        sys.fn_cdc_get_min_lsn('dbo_T081_OC_DETA'),
        @max_lsn,
        'all'
    )
GROUP BY __$operation
ORDER BY Operacion;

--🔵 6. Últimos cambios de CABECERA

-- ÚLTIMOS CAMBIOS CABECERA
DECLARE @from_lsn_cabe BINARY(10) = sys.fn_cdc_get_min_lsn('dbo_T080_OC_CABE');
DECLARE @to_lsn BINARY(10) = sys.fn_cdc_get_max_lsn();

SELECT TOP 200 *
FROM cdc.fn_cdc_get_all_changes_dbo_T080_OC_CABE(
        @from_lsn_cabe,
        @to_lsn,
        'all'
    )
ORDER BY __$start_lsn DESC;

--🔵 7. Últimos cambios de DETALLE

-- ÚLTIMOS CAMBIOS DETALLE
DECLARE @from_lsn_deta BINARY(10) = sys.fn_cdc_get_min_lsn('dbo_T081_OC_DETA');
DECLARE @to_lsn2 BINARY(10) = sys.fn_cdc_get_max_lsn();

SELECT TOP 200 *
FROM cdc.fn_cdc_get_all_changes_dbo_T081_OC_DETA(
        @from_lsn_deta,
        @to_lsn2,
        'all'
    )
ORDER BY __$start_lsn DESC;

/********************************************
🎯 Qué te permite este panel
Ver si CDC está activo y funcionando
Ver qué tablas están capturando cambios
Ver si los jobs están corriendo correctamente
Ver los LSNs mínimos y máximos
Ver cuántos cambios hubo por tipo (insert/update/delete)
Ver los últimos cambios reales en CABE y DETA

Es un panel ideal para:

Diagnóstico
Auditoría
Validación del worker .NET
Monitoreo operativo
**********************************************/