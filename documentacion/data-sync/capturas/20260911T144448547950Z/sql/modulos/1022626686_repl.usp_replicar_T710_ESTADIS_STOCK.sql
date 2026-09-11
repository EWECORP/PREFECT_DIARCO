-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO



-- =============================================
-- Author:		Eduardo Ettlin
-- Create date: 2025-05-21
-- Description:	Replicación Incremental - MERGE
--              Se reemplazará en un futuro por CDC
-- =============================================

CREATE        PROCEDURE [repl].[usp_replicar_T710_ESTADIS_STOCK]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio DATETIME = GETDATE();
    DECLARE @total INT = 0;
    DECLARE @mensaje NVARCHAR(4000);
	DECLARE @fechadesde DATETIME = DATEADD(DAY, -5, GETDATE());

    BEGIN TRY
		------------------------------------------------------------
		-- 1. Limpiar staging
		------------------------------------------------------------
		TRUNCATE TABLE repl.T710_ESTADIS_STOCK_STG;

		------------------------------------------------------------
		-- 2. Cargar staging desde linked servers
		------------------------------------------------------------
		INSERT INTO repl.T710_ESTADIS_STOCK_STG (
			  C_ANIO, C_MES, C_SUCU_EMPR, C_ARTICULO,
			  Q_DIA1, Q_DIA2, Q_DIA3, Q_DIA4, Q_DIA5, Q_DIA6, Q_DIA7, Q_DIA8, Q_DIA9,
			  Q_DIA10, Q_DIA11, Q_DIA12, Q_DIA13, Q_DIA14, Q_DIA15, Q_DIA16, Q_DIA17, Q_DIA18, Q_DIA19, Q_DIA20,
			  Q_DIA21, Q_DIA22, Q_DIA23, Q_DIA24, Q_DIA25, Q_DIA26, Q_DIA27, Q_DIA28, Q_DIA29, Q_DIA30, Q_DIA31,
			  FUENTE_ORIGEN
		)
		(
			  ------------------------------------------------------------
			  -- Histórico Stock DIARCO
			  ------------------------------------------------------------
			  SELECT
					C_ANIO, C_MES, C_SUCU_EMPR, C_ARTICULO,
					Q_DIA1, Q_DIA2, Q_DIA3, Q_DIA4, Q_DIA5, Q_DIA6, Q_DIA7, Q_DIA8, Q_DIA9,
					Q_DIA10, Q_DIA11, Q_DIA12, Q_DIA13, Q_DIA14, Q_DIA15, Q_DIA16, Q_DIA17, Q_DIA18, Q_DIA19, Q_DIA20,
					Q_DIA21, Q_DIA22, Q_DIA23, Q_DIA24, Q_DIA25, Q_DIA26, Q_DIA27, Q_DIA28, Q_DIA29, Q_DIA30, Q_DIA31,
					'DIARCO'
			  FROM [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_STOCK]
			  WHERE C_ANIO * 100 + C_MES >= YEAR(@fechadesde) * 100 + MONTH(@fechadesde)
			  AND C_SUCU_EMPR <= 300

			  UNION ALL

			  ------------------------------------------------------------
			  -- Histórico Stock DIARCO BARRIO
			  ------------------------------------------------------------
			  SELECT
					C_ANIO, C_MES, C_SUCU_EMPR, C_ARTICULO,
					Q_DIA1, Q_DIA2, Q_DIA3, Q_DIA4, Q_DIA5, Q_DIA6, Q_DIA7, Q_DIA8, Q_DIA9,
					Q_DIA10, Q_DIA11, Q_DIA12, Q_DIA13, Q_DIA14, Q_DIA15, Q_DIA16, Q_DIA17, Q_DIA18, Q_DIA19, Q_DIA20,
					Q_DIA21, Q_DIA22, Q_DIA23, Q_DIA24, Q_DIA25, Q_DIA26, Q_DIA27, Q_DIA28, Q_DIA29, Q_DIA30, Q_DIA31,
					'BARRIO'
			  FROM [DIARCO-BARRIO].[DiarcoBarrio].[dbo].[T710_ESTADIS_STOCK]
			  WHERE C_ANIO * 100 + C_MES >= YEAR(@fechadesde) * 100 + MONTH(@fechadesde)
			  AND C_SUCU_EMPR > 300
		);


      
	    ------------------------------------------------------------
        -- 3. MERGE final
		------------------------------------------------------------
        MERGE repl.T710_ESTADIS_STOCK AS TARGET
        USING repl.T710_ESTADIS_STOCK_STG AS SOURCE
        ON TARGET.C_ANIO = SOURCE.C_ANIO
           AND TARGET.C_MES = SOURCE.C_MES
           AND TARGET.C_SUCU_EMPR = SOURCE.C_SUCU_EMPR
           AND TARGET.C_ARTICULO = SOURCE.C_ARTICULO

        WHEN MATCHED THEN
            UPDATE SET
                TARGET.Q_DIA1 = SOURCE.Q_DIA1,
                TARGET.Q_DIA2 = SOURCE.Q_DIA2,
                TARGET.Q_DIA3 = SOURCE.Q_DIA3,
                TARGET.Q_DIA4 = SOURCE.Q_DIA4,
                TARGET.Q_DIA5 = SOURCE.Q_DIA5,
                TARGET.Q_DIA6 = SOURCE.Q_DIA6,
                TARGET.Q_DIA7 = SOURCE.Q_DIA7,
                TARGET.Q_DIA8 = SOURCE.Q_DIA8,
                TARGET.Q_DIA9 = SOURCE.Q_DIA9,
                TARGET.Q_DIA10 = SOURCE.Q_DIA10,
                TARGET.Q_DIA11 = SOURCE.Q_DIA11,
                TARGET.Q_DIA12 = SOURCE.Q_DIA12,
                TARGET.Q_DIA13 = SOURCE.Q_DIA13,
                TARGET.Q_DIA14 = SOURCE.Q_DIA14,
                TARGET.Q_DIA15 = SOURCE.Q_DIA15,
                TARGET.Q_DIA16 = SOURCE.Q_DIA16,
                TARGET.Q_DIA17 = SOURCE.Q_DIA17,
                TARGET.Q_DIA18 = SOURCE.Q_DIA18,
                TARGET.Q_DIA19 = SOURCE.Q_DIA19,
                TARGET.Q_DIA20 = SOURCE.Q_DIA20,
                TARGET.Q_DIA21 = SOURCE.Q_DIA21,
                TARGET.Q_DIA22 = SOURCE.Q_DIA22,
                TARGET.Q_DIA23 = SOURCE.Q_DIA23,
                TARGET.Q_DIA24 = SOURCE.Q_DIA24,
                TARGET.Q_DIA25 = SOURCE.Q_DIA25,
                TARGET.Q_DIA26 = SOURCE.Q_DIA26,
                TARGET.Q_DIA27 = SOURCE.Q_DIA27,
                TARGET.Q_DIA28 = SOURCE.Q_DIA28,
                TARGET.Q_DIA29 = SOURCE.Q_DIA29,
                TARGET.Q_DIA30 = SOURCE.Q_DIA30,
                TARGET.Q_DIA31 = SOURCE.Q_DIA31,
				TARGET.FUENTE_ORIGEN = SOURCE.FUENTE_ORIGEN,
                TARGET.Fecha_Proceso = GETDATE(),
                TARGET.procesado_ok = 0

        WHEN NOT MATCHED THEN
            INSERT (
                C_ANIO, C_MES, C_SUCU_EMPR, C_ARTICULO,
                Q_DIA1, Q_DIA2, Q_DIA3, Q_DIA4, Q_DIA5, Q_DIA6, Q_DIA7, Q_DIA8, Q_DIA9,
                Q_DIA10, Q_DIA11, Q_DIA12, Q_DIA13, Q_DIA14, Q_DIA15, Q_DIA16, Q_DIA17, Q_DIA18, Q_DIA19, Q_DIA20,
                Q_DIA21, Q_DIA22, Q_DIA23, Q_DIA24, Q_DIA25, Q_DIA26, Q_DIA27, Q_DIA28, Q_DIA29, Q_DIA30, Q_DIA31,
				FUENTE_ORIGEN,
                Fecha_Proceso, procesado_ok
            )
            VALUES (
                SOURCE.C_ANIO, SOURCE.C_MES, SOURCE.C_SUCU_EMPR, SOURCE.C_ARTICULO,
                SOURCE.Q_DIA1, SOURCE.Q_DIA2, SOURCE.Q_DIA3, SOURCE.Q_DIA4, SOURCE.Q_DIA5, SOURCE.Q_DIA6, SOURCE.Q_DIA7, SOURCE.Q_DIA8, SOURCE.Q_DIA9,
                SOURCE.Q_DIA10, SOURCE.Q_DIA11, SOURCE.Q_DIA12, SOURCE.Q_DIA13, SOURCE.Q_DIA14, SOURCE.Q_DIA15, SOURCE.Q_DIA16, SOURCE.Q_DIA17, SOURCE.Q_DIA18, SOURCE.Q_DIA19, SOURCE.Q_DIA20,
                SOURCE.Q_DIA21, SOURCE.Q_DIA22, SOURCE.Q_DIA23, SOURCE.Q_DIA24, SOURCE.Q_DIA25, SOURCE.Q_DIA26, SOURCE.Q_DIA27, SOURCE.Q_DIA28, SOURCE.Q_DIA29, SOURCE.Q_DIA30, SOURCE.Q_DIA31,
				SOURCE.FUENTE_ORIGEN,
                GETDATE(), 0
            );

        SET @total = @@ROWCOUNT;
        SET @mensaje = 'MERGE finalizado OK';

    END TRY
    BEGIN CATCH
        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T710_SYNC (fecha_ejecucion, estado, mensaje, duracion_segundos)
        VALUES (GETDATE(), 'ERROR', @mensaje, DATEDIFF(SECOND, @inicio, GETDATE()));
        THROW;
    END CATCH

    -- 4. Registrar log de ejecución
    INSERT INTO repl.LOGS_T710_SYNC (fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos)
    VALUES (@inicio, 'OK', @mensaje, @total, DATEDIFF(SECOND, @inicio, GETDATE()));
END;

GO
