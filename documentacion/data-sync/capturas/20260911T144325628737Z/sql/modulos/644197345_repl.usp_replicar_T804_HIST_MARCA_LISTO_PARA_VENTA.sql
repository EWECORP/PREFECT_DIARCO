-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- =============================================
-- Author:        Eduardo Ettlin
-- Create date:   2025-05-21
-- Description:   Replicación COMPLETA desde linked server
-- =============================================

CREATE PROCEDURE [repl].[usp_replicar_T804_HIST_MARCA_LISTO_PARA_VENTA]
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- 1. Eliminar tabla si existe
        IF OBJECT_ID('repl.T804_HIST_MARCA_LISTO_PARA_VENTA', 'U') IS NOT NULL
        BEGIN
            DROP TABLE repl.T804_HIST_MARCA_LISTO_PARA_VENTA;
        END

        -- 2. Crear tabla con datos replicados desde linked server
        SELECT 
            *,
            'SP_repl.T804_HIST' AS FUENTE_ORIGEN,
            GETDATE() AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL) AS CDC_LSN,
            0 AS ESTADO_SINCRONIZACION
        INTO repl.T804_HIST_MARCA_LISTO_PARA_VENTA
        FROM [DIARCOP001].[DiarcoP].[dbo].[T804_HIST_MARCA_LISTO_PARA_VENTA];
    
    END TRY
    BEGIN CATCH
        -- Manejo de errores
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();

        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;

GO
