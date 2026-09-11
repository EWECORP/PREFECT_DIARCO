-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE   PROCEDURE repl.usp_replicar_T055_LEAD_TIME_B2_SUCURSALES
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio DATETIME = GETDATE();
    DECLARE @mensaje NVARCHAR(4000);
    DECLARE @total INT = 0;

    BEGIN TRY
        TRUNCATE TABLE repl.T055_LEAD_TIME_B2_SUCURSALES_STG;

        INSERT INTO repl.T055_LEAD_TIME_B2_SUCURSALES_STG (
            C_PROVEEDOR, C_SUCURSAL, DIAS_ENTREGA, FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION
        )
        SELECT
            C_PROVEEDOR,
            C_SUCURSAL,
            DIAS_ENTREGA,
            'DIARCOP001' AS FUENTE_ORIGEN,
            GETDATE() AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL) AS CDC_LSN,
            0 AS ESTADO_SINCRONIZACION
        FROM [DIARCOP001].[DiarcoP].[dbo].[T055_LEAD_TIME_B2_SUCURSALES];

        MERGE repl.T055_LEAD_TIME_B2_SUCURSALES AS TARGET
        USING repl.T055_LEAD_TIME_B2_SUCURSALES_STG AS SOURCE
        ON TARGET.C_PROVEEDOR = SOURCE.C_PROVEEDOR AND TARGET.C_SUCURSAL = SOURCE.C_SUCURSAL

        WHEN MATCHED THEN
            UPDATE SET
                TARGET.DIAS_ENTREGA = SOURCE.DIAS_ENTREGA,
                TARGET.FUENTE_ORIGEN = SOURCE.FUENTE_ORIGEN,
                TARGET.FECHA_EXTRACCION = SOURCE.FECHA_EXTRACCION,
                TARGET.CDC_LSN = SOURCE.CDC_LSN,
                TARGET.ESTADO_SINCRONIZACION = SOURCE.ESTADO_SINCRONIZACION

        WHEN NOT MATCHED THEN
            INSERT (C_PROVEEDOR, C_SUCURSAL, DIAS_ENTREGA, FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION)
            VALUES (SOURCE.C_PROVEEDOR, SOURCE.C_SUCURSAL, SOURCE.DIAS_ENTREGA, SOURCE.FUENTE_ORIGEN, SOURCE.FECHA_EXTRACCION, SOURCE.CDC_LSN, SOURCE.ESTADO_SINCRONIZACION);

        SET @total = @@ROWCOUNT;
        SET @mensaje = 'MERGE finalizado correctamente';
    END TRY
    BEGIN CATCH
        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T055_LEAD_TIME_B2_SUCURSALES_SYNC (
            fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
        ) VALUES (GETDATE(), 'ERROR', @mensaje, 0, DATEDIFF(SECOND, @inicio, GETDATE()));
        THROW;
    END CATCH

    INSERT INTO repl.LOGS_T055_LEAD_TIME_B2_SUCURSALES_SYNC (
        fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
    ) VALUES (@inicio, 'OK', @mensaje, @total, DATEDIFF(SECOND, @inicio, GETDATE()));
END;

GO
