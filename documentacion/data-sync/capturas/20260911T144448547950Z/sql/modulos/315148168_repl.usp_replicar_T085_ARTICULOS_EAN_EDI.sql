-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE repl.usp_replicar_T085_ARTICULOS_EAN_EDI
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio DATETIME = GETDATE();
    DECLARE @mensaje NVARCHAR(4000);
    DECLARE @total INT = 0;

    BEGIN TRY
        TRUNCATE TABLE repl.T085_ARTICULOS_EAN_EDI_STG;

        INSERT INTO repl.T085_ARTICULOS_EAN_EDI_STG (
            C_ARTICULO, C_EAN, FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION
        )
        SELECT
            C_ARTICULO,
            C_EAN,
            'DIARCOP001' AS FUENTE_ORIGEN,
            GETDATE() AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL) AS CDC_LSN,
            0 AS ESTADO_SINCRONIZACION
        FROM [DIARCOP001].[DiarcoP].[dbo].[T085_ARTICULOS_EAN_EDI];

        MERGE repl.T085_ARTICULOS_EAN_EDI AS TARGET
        USING repl.T085_ARTICULOS_EAN_EDI_STG AS SOURCE
        ON TARGET.C_ARTICULO = SOURCE.C_ARTICULO AND TARGET.C_EAN = SOURCE.C_EAN

        WHEN MATCHED THEN
            UPDATE SET
                TARGET.FUENTE_ORIGEN = SOURCE.FUENTE_ORIGEN,
                TARGET.FECHA_EXTRACCION = SOURCE.FECHA_EXTRACCION,
                TARGET.CDC_LSN = SOURCE.CDC_LSN,
                TARGET.ESTADO_SINCRONIZACION = SOURCE.ESTADO_SINCRONIZACION

        WHEN NOT MATCHED THEN
            INSERT (C_ARTICULO, C_EAN, FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION)
            VALUES (SOURCE.C_ARTICULO, SOURCE.C_EAN, SOURCE.FUENTE_ORIGEN, SOURCE.FECHA_EXTRACCION, SOURCE.CDC_LSN, SOURCE.ESTADO_SINCRONIZACION);

        SET @total = @@ROWCOUNT;
        SET @mensaje = 'MERGE finalizado correctamente';
    END TRY
    BEGIN CATCH
        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T085_ARTICULOS_EAN_EDI_SYNC (
            fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
        ) VALUES (GETDATE(), 'ERROR', @mensaje, 0, DATEDIFF(SECOND, @inicio, GETDATE()));
        THROW;
    END CATCH

    INSERT INTO repl.LOGS_T085_ARTICULOS_EAN_EDI_SYNC (
        fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
    ) VALUES (@inicio, 'OK', @mensaje, @total, DATEDIFF(SECOND, @inicio, GETDATE()));
END;

GO
