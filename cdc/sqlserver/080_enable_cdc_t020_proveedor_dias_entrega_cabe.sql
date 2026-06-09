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
    WHERE capture_instance = 'dbo_T020_PROVEEDOR_DIAS_ENTREGA_CABE'
)
BEGIN
    PRINT 'Habilitando CDC para dbo.T020_PROVEEDOR_DIAS_ENTREGA_CABE...';

    EXEC sys.sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name = 'T020_PROVEEDOR_DIAS_ENTREGA_CABE',
        @capture_instance = 'dbo_T020_PROVEEDOR_DIAS_ENTREGA_CABE',
        @role_name = NULL,
        @supports_net_changes = 0;
END
ELSE
BEGIN
    PRINT 'CDC ya estaba habilitado para dbo.T020_PROVEEDOR_DIAS_ENTREGA_CABE.';
END
GO
