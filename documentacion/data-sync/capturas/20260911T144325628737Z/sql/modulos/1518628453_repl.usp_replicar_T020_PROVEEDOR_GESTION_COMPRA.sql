-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE     PROCEDURE [repl].[usp_replicar_T020_PROVEEDOR_GESTION_COMPRA]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio DATETIME = GETDATE();
    DECLARE @mensaje NVARCHAR(4000);
    DECLARE @total INT = 0;

    BEGIN TRY
        -- 1. Truncar STG
        TRUNCATE TABLE repl.T020_PROVEEDOR_GESTION_COMPRA_STG;

        -- 2. Insertar desde origen
        INSERT INTO repl.T020_PROVEEDOR_GESTION_COMPRA_STG (
            C_PROVEEDOR, C_CUIT, C_USUARIO,
            FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION
        )
        SELECT
            C_PROVEEDOR, C_CUIT, C_USUARIO,
            'DIARCOP001' AS FUENTE_ORIGEN,
            GETDATE() AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL) AS CDC_LSN,
            0 AS ESTADO_SINCRONIZACION
        FROM [DIARCOP001].[DiarcoP].[dbo].[T020_PROVEEDOR_GESTION_COMPRA];

        -- 3. MERGE con tabla final
        MERGE repl.T020_PROVEEDOR_GESTION_COMPRA AS TARGET
        USING repl.T020_PROVEEDOR_GESTION_COMPRA_STG AS SOURCE
		ON TARGET.C_PROVEEDOR = SOURCE.C_PROVEEDOR AND TARGET.C_USUARIO = SOURCE.C_USUARIO


        WHEN MATCHED THEN
            UPDATE SET
                TARGET.C_USUARIO = SOURCE.C_USUARIO,
                TARGET.FUENTE_ORIGEN = SOURCE.FUENTE_ORIGEN,
                TARGET.FECHA_EXTRACCION = SOURCE.FECHA_EXTRACCION,
                TARGET.CDC_LSN = SOURCE.CDC_LSN,
                TARGET.ESTADO_SINCRONIZACION = SOURCE.ESTADO_SINCRONIZACION

        WHEN NOT MATCHED THEN
            INSERT (
                C_PROVEEDOR, C_CUIT, C_USUARIO,
                FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION
            )
            VALUES (
                SOURCE.C_PROVEEDOR, SOURCE.C_CUIT, SOURCE.C_USUARIO,
                SOURCE.FUENTE_ORIGEN, SOURCE.FECHA_EXTRACCION, SOURCE.CDC_LSN, SOURCE.ESTADO_SINCRONIZACION
            );

        SET @total = @@ROWCOUNT;
        SET @mensaje = 'MERGE finalizado correctamente';

    END TRY
    BEGIN CATCH
        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T020_PROV_GESTION_COMPRA_SYNC (
            fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
        )
        VALUES (
            GETDATE(), 'ERROR', @mensaje, 0, DATEDIFF(SECOND, @inicio, GETDATE())
        );
        THROW;
    END CATCH

    INSERT INTO repl.LOGS_T020_PROV_GESTION_COMPRA_SYNC (
        fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
    )
    VALUES (
        @inicio, 'OK', @mensaje, @total, DATEDIFF(SECOND, @inicio, GETDATE())
    );
END;

GO
