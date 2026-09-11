-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO



-- =============================================
-- Author:        Eduardo Ettlin
-- Create date:   2025-11-21
-- Description:   Replicación COMPLETA desde linked server
-- =============================================

CREATE     PROCEDURE [repl].[usp_replicar_T874_PRECARGA_CONNEXA_HIST]
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- 1. Eliminar tabla si existe
        IF OBJECT_ID('repl.T874_PRECARGA_CONNEXA_HIST', 'U') IS NOT NULL
        BEGIN
            DROP TABLE repl.T874_PRECARGA_CONNEXA_HIST;
        END

        -- 2. Crear tabla con datos replicados desde linked server
        SELECT 
            [C_PROVEEDOR],[C_ARTICULO],[C_SUCU_EMPR],[Q_BULTOS_KILOS_DIARCO],[F_ALTA_SIST],[C_USUARIO_GENERO_OC]
			,[C_TERMINAL_GENERO_OC],[F_GENERO_OC],[C_USUARIO_BLOQUEO],[M_PROCESADO],[F_PROCESADO],[U_PREFIJO_OC]
			,[U_SUFIJO_OC],[C_COMPRA_KIKKER] AS C_COMPRA_CONNEXA
			,[C_USUARIO_MODIF],[C_COMPRADOR],
            'SP_repl.T874_HIST' AS FUENTE_ORIGEN,
            GETDATE() AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL) AS CDC_LSN,
            0 AS ESTADO_SINCRONIZACION
        INTO repl.T874_PRECARGA_CONNEXA_HIST
        FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
		WHERE [F_ALTA_SIST]>='20260101';
    
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
