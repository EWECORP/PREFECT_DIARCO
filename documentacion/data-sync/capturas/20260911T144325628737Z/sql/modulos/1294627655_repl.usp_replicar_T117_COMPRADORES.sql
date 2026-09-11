-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE repl.usp_replicar_T117_COMPRADORES
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio DATETIME = GETDATE();
    DECLARE @total INT = 0;
    DECLARE @mensaje NVARCHAR(4000);

    BEGIN TRY
        -- 1. Limpiar STG
        TRUNCATE TABLE repl.T117_COMPRADORES_STG;

        -- 2. Insertar desde origen
        INSERT INTO repl.T117_COMPRADORES_STG (
            C_COMPRADOR, N_COMPRADOR, N_COMPRADOR_ABREV,
            C_SUCU_COMPRADOR, F_MODIF, M_BAJA, C_USUARIO,
            FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION
        )
        SELECT
            C_COMPRADOR, N_COMPRADOR, N_COMPRADOR_ABREV,
            C_SUCU_COMPRADOR, F_MODIF, M_BAJA, C_USUARIO,
            'DIARCOP001' AS FUENTE_ORIGEN,
            GETDATE() AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL) AS CDC_LSN,
            0 AS ESTADO_SINCRONIZACION
        FROM [DIARCOP001].[DiarcoP].[dbo].[T117_COMPRADORES];

        -- 3. MERGE en tabla final
        MERGE repl.T117_COMPRADORES AS TARGET
        USING repl.T117_COMPRADORES_STG AS SOURCE
        ON TARGET.C_COMPRADOR = SOURCE.C_COMPRADOR

        WHEN MATCHED THEN
            UPDATE SET
                TARGET.N_COMPRADOR = SOURCE.N_COMPRADOR,
                TARGET.N_COMPRADOR_ABREV = SOURCE.N_COMPRADOR_ABREV,
                TARGET.C_SUCU_COMPRADOR = SOURCE.C_SUCU_COMPRADOR,
                TARGET.F_MODIF = SOURCE.F_MODIF,
                TARGET.M_BAJA = SOURCE.M_BAJA,
                TARGET.C_USUARIO = SOURCE.C_USUARIO,
                TARGET.FUENTE_ORIGEN = SOURCE.FUENTE_ORIGEN,
                TARGET.FECHA_EXTRACCION = SOURCE.FECHA_EXTRACCION,
                TARGET.ESTADO_SINCRONIZACION = SOURCE.ESTADO_SINCRONIZACION,
                TARGET.CDC_LSN = SOURCE.CDC_LSN

        WHEN NOT MATCHED THEN
            INSERT (
                C_COMPRADOR, N_COMPRADOR, N_COMPRADOR_ABREV,
                C_SUCU_COMPRADOR, F_MODIF, M_BAJA, C_USUARIO,
                FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION
            )
            VALUES (
                SOURCE.C_COMPRADOR, SOURCE.N_COMPRADOR, SOURCE.N_COMPRADOR_ABREV,
                SOURCE.C_SUCU_COMPRADOR, SOURCE.F_MODIF, SOURCE.M_BAJA, SOURCE.C_USUARIO,
                SOURCE.FUENTE_ORIGEN, SOURCE.FECHA_EXTRACCION, SOURCE.CDC_LSN, SOURCE.ESTADO_SINCRONIZACION
            );

        SET @total = @@ROWCOUNT;
        SET @mensaje = 'MERGE finalizado OK';

    END TRY
    BEGIN CATCH
        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T117_COMPRADORES_SYNC (
            fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
        )
        VALUES (
            GETDATE(), 'ERROR', @mensaje, 0, DATEDIFF(SECOND, @inicio, GETDATE())
        );
        THROW;
    END CATCH

    INSERT INTO repl.LOGS_T117_COMPRADORES_SYNC (
        fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
    )
    VALUES (
        @inicio, 'OK', @mensaje, @total, DATEDIFF(SECOND, @inicio, GETDATE())
    );
END;

GO
