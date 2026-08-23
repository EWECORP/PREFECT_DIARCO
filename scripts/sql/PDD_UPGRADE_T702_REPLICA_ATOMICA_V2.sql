USE [data-sync]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [repl].[usp_replicar_T702_EST_VTAS_POR_ARTICULO]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @fecha_max date;
    DECLARE @inicio datetime = GETDATE();
    DECLARE @mensaje nvarchar(4000);
    DECLARE @registros int = 0;
    DECLARE @lock_result int;

    BEGIN TRY
        BEGIN TRANSACTION;

        EXEC @lock_result = sys.sp_getapplock
            @Resource = 'repl.T702_EST_VTAS_POR_ARTICULO.refresh',
            @LockMode = 'Exclusive',
            @LockOwner = 'Transaction',
            @LockTimeout = 0;

        IF @lock_result < 0
            THROW 51000, 'Ya existe una sincronizacion DIARCO T702 en ejecucion', 1;

        SELECT @fecha_max = ISNULL(MAX(F_VENTA), CONVERT(date, '20250501', 112))
        FROM repl.T702_EST_VTAS_POR_ARTICULO;

        SET @fecha_max = DATEADD(day, -3, @fecha_max);

        DELETE FROM repl.T702_EST_VTAS_POR_ARTICULO
        WHERE F_VENTA > @fecha_max;

        INSERT INTO repl.T702_EST_VTAS_POR_ARTICULO (
            F_VENTA, C_ARTICULO, C_FAMILIA, C_SUCU_EMPR,
            I_PRECIO_VENTA, I_PRECIO_COSTO, I_VENDIDO, Q_UNIDADES_VENDIDAS,
            I_PRECIO_COSTO_PP, I_PARTE_ULTIMO_INGRESO,
            I_COMPRA_ULTIMO_INGRESO, I_IMP_INTERNOS
        )
        SELECT
            F_VENTA, C_ARTICULO, C_FAMILIA, C_SUCU_EMPR,
            I_PRECIO_VENTA, I_PRECIO_COSTO, I_VENDIDO, Q_UNIDADES_VENDIDAS,
            I_PRECIO_COSTO_PP, I_PARTE_ULTIMO_INGRESO,
            I_COMPRA_ULTIMO_INGRESO, I_IMP_INTERNOS
        FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO]
        WHERE F_VENTA > @fecha_max;

        SET @registros = @@ROWCOUNT;
        SET @mensaje = 'Sincronizacion incremental atomica exitosa';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T702_VTAS_SYNC (
            fecha_ejecucion, estado, mensaje,
            registros_afectados, duracion_segundos
        )
        VALUES (
            GETDATE(), 'ERROR', @mensaje, 0,
            DATEDIFF(second, @inicio, GETDATE())
        );
        THROW;
    END CATCH;

    INSERT INTO repl.LOGS_T702_VTAS_SYNC (
        fecha_ejecucion, estado, mensaje,
        registros_afectados, duracion_segundos
    )
    VALUES (
        @inicio, 'OK', @mensaje, @registros,
        DATEDIFF(second, @inicio, GETDATE())
    );
END;
GO

CREATE OR ALTER PROCEDURE [repl].[usp_replicar_T702_EST_VTAS_POR_ARTICULO_BARRIO]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @fecha_max date;
    DECLARE @inicio datetime = GETDATE();
    DECLARE @mensaje nvarchar(4000);
    DECLARE @registros int = 0;
    DECLARE @lock_result int;

    BEGIN TRY
        BEGIN TRANSACTION;

        EXEC @lock_result = sys.sp_getapplock
            @Resource = 'repl.T702_EST_VTAS_POR_ARTICULO_DBARRIO.refresh',
            @LockMode = 'Exclusive',
            @LockOwner = 'Transaction',
            @LockTimeout = 0;

        IF @lock_result < 0
            THROW 51000, 'Ya existe una sincronizacion BARRIO T702 en ejecucion', 1;

        SELECT @fecha_max = ISNULL(MAX(F_VENTA), CONVERT(date, '20250601', 112))
        FROM repl.T702_EST_VTAS_POR_ARTICULO_DBARRIO;

        SET @fecha_max = DATEADD(day, -3, @fecha_max);

        DELETE FROM repl.T702_EST_VTAS_POR_ARTICULO_DBARRIO
        WHERE F_VENTA > @fecha_max;

        INSERT INTO repl.T702_EST_VTAS_POR_ARTICULO_DBARRIO (
            F_VENTA, C_ARTICULO, C_FAMILIA, C_SUCU_EMPR,
            I_PRECIO_VENTA, I_PRECIO_COSTO, I_VENDIDO, Q_UNIDADES_VENDIDAS,
            I_PRECIO_COSTO_PP, I_PARTE_ULTIMO_INGRESO,
            I_COMPRA_ULTIMO_INGRESO, I_IMP_INTERNOS
        )
        SELECT
            F_VENTA, C_ARTICULO, C_FAMILIA, C_SUCU_EMPR,
            I_PRECIO_VENTA, I_PRECIO_COSTO, I_VENDIDO, Q_UNIDADES_VENDIDAS,
            I_PRECIO_COSTO_PP, I_PARTE_ULTIMO_INGRESO,
            I_COMPRA_ULTIMO_INGRESO, I_IMP_INTERNOS
        FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO_DBARRIO]
        WHERE F_VENTA > @fecha_max;

        SET @registros = @@ROWCOUNT;
        SET @mensaje = 'Sincronizacion incremental atomica exitosa';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T702_VTAS_BARRIO_SYNC (
            fecha_ejecucion, estado, mensaje,
            registros_afectados, duracion_segundos
        )
        VALUES (
            GETDATE(), 'ERROR', @mensaje, 0,
            DATEDIFF(second, @inicio, GETDATE())
        );
        THROW;
    END CATCH;

    INSERT INTO repl.LOGS_T702_VTAS_BARRIO_SYNC (
        fecha_ejecucion, estado, mensaje,
        registros_afectados, duracion_segundos
    )
    VALUES (
        @inicio, 'OK', @mensaje, @registros,
        DATEDIFF(second, @inicio, GETDATE())
    );
END;
GO
