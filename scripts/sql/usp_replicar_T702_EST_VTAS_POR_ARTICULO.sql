USE [data-sync]
GO

/****** Object:  StoredProcedure [repl].[usp_replicar_T702_EST_VTAS_POR_ARTICULO]    Script Date: 22/08/2026 19:06:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE OR ALTER            PROCEDURE [repl].[usp_replicar_T702_EST_VTAS_POR_ARTICULO]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @fecha_max DATE;
    DECLARE @inicio DATETIME = GETDATE();
    DECLARE @mensaje NVARCHAR(4000);
    DECLARE @registros INT = 0;

    BEGIN TRY
		-- 1. Obtener la última fecha ya replicada
        SELECT @fecha_max = ISNULL(MAX(F_VENTA), '20250501') 
        FROM repl.T702_EST_VTAS_POR_ARTICULO;
	    
		-- 2. Margen de seguridad de Reprocesos
		SET @fecha_max = DATEADD(DAY, -3, @fecha_max);

		-- 3. Eliminar Registros del Margen de Seguridad
		DELETE from repl.T702_EST_VTAS_POR_ARTICULO
		 WHERE F_VENTA > @fecha_max;

        -- 4. Insertar solo nuevos registros desde la fuente
        INSERT INTO repl.T702_EST_VTAS_POR_ARTICULO (
            F_VENTA, C_ARTICULO, C_FAMILIA, C_SUCU_EMPR,
            I_PRECIO_VENTA, I_PRECIO_COSTO, I_VENDIDO, Q_UNIDADES_VENDIDAS,
            I_PRECIO_COSTO_PP, I_PARTE_ULTIMO_INGRESO, I_COMPRA_ULTIMO_INGRESO, I_IMP_INTERNOS
        )
        SELECT 
            F_VENTA, C_ARTICULO, C_FAMILIA, C_SUCU_EMPR,
            I_PRECIO_VENTA, I_PRECIO_COSTO, I_VENDIDO, Q_UNIDADES_VENDIDAS,
            I_PRECIO_COSTO_PP, I_PARTE_ULTIMO_INGRESO, I_COMPRA_ULTIMO_INGRESO, I_IMP_INTERNOS
        FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO]
        WHERE F_VENTA > @fecha_max;

        SET @registros = @@ROWCOUNT;
        SET @mensaje = 'Inserción incremental exitosa';

    END TRY
    BEGIN CATCH
        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T702_VTAS_SYNC (
            fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
        )
        VALUES (
            GETDATE(), 'ERROR', @mensaje, 0, DATEDIFF(SECOND, @inicio, GETDATE())
        );
        THROW;
    END CATCH

    INSERT INTO repl.LOGS_T702_VTAS_SYNC (
        fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos
    )
    VALUES (
        @inicio, 'OK', @mensaje, @registros, DATEDIFF(SECOND, @inicio, GETDATE())
    );
END;
GO


