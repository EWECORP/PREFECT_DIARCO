USE [DiarcoP];
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = DB_NAME()
      AND is_cdc_enabled = 1
)
BEGIN
    PRINT 'Habilitando CDC en la base actual...';
    EXEC sys.sp_cdc_enable_db;
END
ELSE
BEGIN
    PRINT 'CDC ya estaba habilitado en la base actual.';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables
    WHERE capture_instance = 'dbo_T051_ARTICULOS_SUCURSAL'
)
BEGIN
    PRINT 'Habilitando CDC para dbo.T051_ARTICULOS_SUCURSAL...';

    EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name = 'T051_ARTICULOS_SUCURSAL',
        @capture_instance = 'dbo_T051_ARTICULOS_SUCURSAL',
        @role_name = NULL,
        @supports_net_changes = 0;
END
ELSE
BEGIN
    PRINT 'CDC ya estaba habilitado para dbo.T051_ARTICULOS_SUCURSAL.';
END
GO
