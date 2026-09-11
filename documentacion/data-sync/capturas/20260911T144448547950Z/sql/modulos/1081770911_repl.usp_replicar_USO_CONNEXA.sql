-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE [repl].[usp_replicar_USO_CONNEXA]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio DATETIME = GETDATE();
    DECLARE @mensaje NVARCHAR(4000);
    DECLARE @total INT = 0;

    BEGIN TRY

        --- MENSUAL COMPRADOR
        TRUNCATE TABLE repl.MV_USO_MENSUAL_COMPRADOR;

        INSERT INTO repl.MV_USO_MENSUAL_COMPRADOR (
            [C_COMPRADOR],[MES],[Total_Prv_CNX],[Total_Pedidos_CNX],[Total_OC_CNX],[Total_BULTOS_CNX],
            [Total_Prv_SGM],[Total_OC_SGM],[Total_BULTOS_SGM],
            [FUENTE_ORIGEN],[FECHA_EXTRACCION],[CDC_LSN],[ESTADO_SINCRONIZACION]
        )
        SELECT [C_COMPRADOR],[MES],[Total_Prv_CNX],[Total_Pedidos_CNX],[Total_OC_CNX],[Total_BULTOS_CNX],
               [Total_Prv_SGM],[Total_OC_SGM],[Total_BULTOS_SGM],
               'VISTAS', GETDATE(), CONVERT(VARBINARY(10), NULL), 0
        FROM [data-sync].[dbo].[V_USO_MENSUAL_COMPRADOR];

        --- MENSUAL PROVEEDOR
        TRUNCATE TABLE repl.MV_USO_MENSUAL_PROVEEDOR;

        INSERT INTO repl.MV_USO_MENSUAL_PROVEEDOR (
            [C_PROVEEDOR],[MES],[Total_Pedidos_CNX],[Total_OC_CNX],[Total_BULTOS_CNX],
            [Total_OC_SGM],[Total_BULTOS_SGM],
            [FUENTE_ORIGEN],[FECHA_EXTRACCION],[CDC_LSN],[ESTADO_SINCRONIZACION]
        )
        SELECT [C_PROVEEDOR],[MES],[Total_Pedidos_CNX],[Total_OC_CNX],[Total_BULTOS_CNX],
               [Total_OC_SGM],[Total_BULTOS_SGM],
               'VISTAS', GETDATE(), CONVERT(VARBINARY(10), NULL), 0
        FROM dbo.V_USO_MENSUAL_PROVEEDOR;

        --- SEMANAL COMPRADOR
        TRUNCATE TABLE repl.MV_USO_SEMANAL_COMPRADOR;

        INSERT INTO repl.MV_USO_SEMANAL_COMPRADOR (
            [C_COMPRADOR],[SEMANA],[Total_Prv_CNX],[Total_Pedidos_CNX],[Total_OC_CNX],[Total_BULTOS_CNX],
            [Total_Prv_SGM],[Total_OC_SGM],[Total_BULTOS_SGM],
            [FUENTE_ORIGEN],[FECHA_EXTRACCION],[CDC_LSN],[ESTADO_SINCRONIZACION]
        )
        SELECT [C_COMPRADOR],[SEMANA],[Total_Prv_CNX],[Total_Pedidos_CNX],[Total_OC_CNX],[Total_BULTOS_CNX],
               [Total_Prv_SGM],[Total_OC_SGM],[Total_BULTOS_SGM],
               'VISTAS', GETDATE(), CONVERT(VARBINARY(10), NULL), 0
        FROM [data-sync].[dbo].[V_USO_SEMANAL_COMPRADOR];

        --- SEMANAL PROVEEDOR
        TRUNCATE TABLE repl.MV_USO_SEMANAL_PROVEEDOR;

        INSERT INTO repl.MV_USO_SEMANAL_PROVEEDOR (
            [C_PROVEEDOR],[C_COMPRADOR],[SEMANA],[Total_Pedidos_CNX],[Total_OC_CNX],[Total_BULTOS_CNX],
            [Total_OC_SGM],[Total_BULTOS_SGM],
            [FUENTE_ORIGEN],[FECHA_EXTRACCION],[CDC_LSN],[ESTADO_SINCRONIZACION]
        )
        SELECT [C_PROVEEDOR],[C_COMPRADOR],[SEMANA],[Total_Pedidos_CNX],[Total_OC_CNX],[Total_BULTOS_CNX],
               [Total_OC_SGM],[Total_BULTOS_SGM],
               'VISTAS', GETDATE(), CONVERT(VARBINARY(10), NULL), 0
        FROM dbo.V_USO_SEMANAL_PROVEEDOR;

    END TRY
    BEGIN CATCH
        SET @mensaje = ERROR_MESSAGE();
        RAISERROR('Error en usp_replicar_USO_CONNEXA: %s', 16, 1, @mensaje);
    END CATCH;

END;

GO
