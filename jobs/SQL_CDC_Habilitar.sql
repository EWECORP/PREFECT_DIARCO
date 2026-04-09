------------------------------------------------------------
-- 1) Habilitar CDC a nivel de base de datos
------------------------------------------------------------
USE [DiarcoP];
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'DiarcoP'
      AND is_cdc_enabled = 1
)
BEGIN
    PRINT 'Habilitando CDC en la base DiarcoP...';
    EXEC sys.sp_cdc_enable_db;
END
ELSE
BEGIN
    PRINT 'CDC ya estaba habilitado en la base DiarcoP.';
END
GO


------------------------------------------------------------
-- 2) Habilitar CDC en tabla CABECERA: T080_OC_CABE
------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'T080_OC_CABE'
      AND s.name = 'dbo'
      AND t.is_tracked_by_cdc = 1
)
BEGIN
    PRINT 'Habilitando CDC en dbo.T080_OC_CABE...';

    EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name   = 'T080_OC_CABE',
        @role_name     = NULL,       -- o un rol si querés controlar acceso
        @supports_net_changes = 1;   -- útil para obtener cambios netos
END
ELSE
BEGIN
    PRINT 'CDC ya estaba habilitado en dbo.T080_OC_CABE.';
END
GO


------------------------------------------------------------
-- 3) Habilitar CDC en tabla DETALLE: T081_OC_DETA
------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'T081_OC_DETA'
      AND s.name = 'dbo'
      AND t.is_tracked_by_cdc = 1
)
BEGIN
    PRINT 'Habilitando CDC en dbo.T081_OC_DETA...';

    EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name   = 'T081_OC_DETA',
        @role_name     = NULL,
        @supports_net_changes = 1;
END
ELSE
BEGIN
    PRINT 'CDC ya estaba habilitado en dbo.T081_OC_DETA.';
END
GO


------------------------------------------------------------
-- 4) Verificación final
------------------------------------------------------------
SELECT 
    t.name AS Tabla,
    s.name AS Esquema,
    t.is_tracked_by_cdc AS CDC_Habilitado
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name IN ('T080_OC_CABE', 'T081_OC_DETA');
GO

SELECT * FROM cdc.change_tables;
GO

------------------------------------------------------------
-- 5) Ajustar el Tiempo de Retención de los Cambios (opcional, por defecto es 3 días = 4320 minutos)
------------------------------------------------------------

EXEC sys.sp_cdc_change_job
    @job_type = 'cleanup',
    @retention = 4320; -- minutos