-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO



/* ============================================================
   Autor:        Eduardo Ettlin
   Creación:     2025-05-21
   Procedimiento: repl.usp_reprocesar_T710_ESTADIS_PRECIOS_MES
   Descripción:  Reproceso histórico por rango de meses (con MERGE) desde
                 linked servers hacia tablas REPL. No reemplaza el proceso
                 incremental habitual.
                 - Promueve precios a DECIMAL(19,4)
                 - Diagnóstico de filas no convertibles (#Bad)
                 - Logging de ejecución y advertencias
	Modificado:    2026-08-05 (VERSIÓN PARA REPROCESO HISTÓRICO)
   ============================================================ */
CREATE PROCEDURE [repl].[usp_reprocesar_T710_ESTADIS_PRECIOS_MES]
    @AnioDesde INT,
    @MesDesde  INT,
    @AnioHasta INT,
    @MesHasta  INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @inicio   DATETIME      = GETDATE();
    DECLARE @total    INT           = 0;
    DECLARE @mensaje  NVARCHAR(4000);
    DECLARE @PeriodoDesde INT;
    DECLARE @PeriodoHasta INT;

    ----------------------------------------------------------------------
    -- Validación de parámetros
    ----------------------------------------------------------------------
    IF @AnioDesde NOT BETWEEN 1900 AND 9999
       OR @AnioHasta NOT BETWEEN 1900 AND 9999
       OR @MesDesde NOT BETWEEN 1 AND 12
       OR @MesHasta NOT BETWEEN 1 AND 12
    BEGIN
        THROW 50001, 'Año o mes inválido. Los meses deben estar entre 1 y 12.', 1;
    END;

    SET @PeriodoDesde = (@AnioDesde * 100) + @MesDesde;
    SET @PeriodoHasta = (@AnioHasta * 100) + @MesHasta;

    IF @PeriodoDesde > @PeriodoHasta
    BEGIN
        THROW 50002, 'El período desde no puede ser posterior al período hasta.', 1;
    END;

    BEGIN TRY
        ----------------------------------------------------------------------
        -- 1) Limpiar staging
        ----------------------------------------------------------------------
        TRUNCATE TABLE repl.T710_ESTADIS_PRECIOS_STG;

        ----------------------------------------------------------------------
        -- 2) Cargar desde origen a #Src con promoción de precisión
        --    y construir #Bad para diagnóstico de conversiones
        ----------------------------------------------------------------------
        IF OBJECT_ID('tempdb..#Src') IS NOT NULL DROP TABLE #Src;
        IF OBJECT_ID('tempdb..#Bad') IS NOT NULL DROP TABLE #Bad;

        SELECT
            C_ANIO,
            C_MES,
            C_ARTICULO,
            C_SUCU_EMPR,

            -- Origen: money NOT NULL -> promover a DECIMAL(19,4) (mapeo 1:1)
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_1 ) AS I_PRECIO_VTA_1,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_2 ) AS I_PRECIO_VTA_2,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_3 ) AS I_PRECIO_VTA_3,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_4 ) AS I_PRECIO_VTA_4,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_5 ) AS I_PRECIO_VTA_5,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_6 ) AS I_PRECIO_VTA_6,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_7 ) AS I_PRECIO_VTA_7,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_8 ) AS I_PRECIO_VTA_8,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_9 ) AS I_PRECIO_VTA_9,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_10) AS I_PRECIO_VTA_10,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_11) AS I_PRECIO_VTA_11,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_12) AS I_PRECIO_VTA_12,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_13) AS I_PRECIO_VTA_13,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_14) AS I_PRECIO_VTA_14,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_15) AS I_PRECIO_VTA_15,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_16) AS I_PRECIO_VTA_16,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_17) AS I_PRECIO_VTA_17,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_18) AS I_PRECIO_VTA_18,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_19) AS I_PRECIO_VTA_19,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_20) AS I_PRECIO_VTA_20,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_21) AS I_PRECIO_VTA_21,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_22) AS I_PRECIO_VTA_22,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_23) AS I_PRECIO_VTA_23,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_24) AS I_PRECIO_VTA_24,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_25) AS I_PRECIO_VTA_25,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_26) AS I_PRECIO_VTA_26,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_27) AS I_PRECIO_VTA_27,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_28) AS I_PRECIO_VTA_28,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_29) AS I_PRECIO_VTA_29,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_30) AS I_PRECIO_VTA_30,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_31) AS I_PRECIO_VTA_31,

            'DIARCOP001'                AS FUENTE_ORIGEN,
            GETDATE()                   AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL) AS CDC_LSN,
            0                           AS ESTADO_SINCRONIZACION
        INTO #Src
        FROM DIARCOP001.DiarcoP.dbo.T710_ESTADIS_PRECIOS
        WHERE (C_ANIO * 100 + C_MES) BETWEEN @PeriodoDesde AND @PeriodoHasta
		AND C_SUCU_EMPR <= 300;


		--- Agregar Registros de BARRIO
		INSERT INTO #Src (
            C_ANIO, C_MES, C_ARTICULO, C_SUCU_EMPR,
            I_PRECIO_VTA_1 , I_PRECIO_VTA_2 , I_PRECIO_VTA_3 , I_PRECIO_VTA_4 , I_PRECIO_VTA_5 ,
            I_PRECIO_VTA_6 , I_PRECIO_VTA_7 , I_PRECIO_VTA_8 , I_PRECIO_VTA_9 , I_PRECIO_VTA_10, I_PRECIO_VTA_11, I_PRECIO_VTA_12, I_PRECIO_VTA_13, I_PRECIO_VTA_14,
            I_PRECIO_VTA_15, I_PRECIO_VTA_16, I_PRECIO_VTA_17, I_PRECIO_VTA_18, I_PRECIO_VTA_19, I_PRECIO_VTA_20, I_PRECIO_VTA_21, I_PRECIO_VTA_22, I_PRECIO_VTA_23,
            I_PRECIO_VTA_24, I_PRECIO_VTA_25, I_PRECIO_VTA_26, I_PRECIO_VTA_27, I_PRECIO_VTA_28, I_PRECIO_VTA_29, I_PRECIO_VTA_30, I_PRECIO_VTA_31,
            FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION
        )
		        SELECT
            C_ANIO,
            C_MES,
            C_ARTICULO,
            C_SUCU_EMPR,

            -- Origen: money NOT NULL -> promover a DECIMAL(19,4) (mapeo 1:1)
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_1 ) AS I_PRECIO_VTA_1,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_2 ) AS I_PRECIO_VTA_2,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_3 ) AS I_PRECIO_VTA_3,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_4 ) AS I_PRECIO_VTA_4,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_5 ) AS I_PRECIO_VTA_5,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_6 ) AS I_PRECIO_VTA_6,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_7 ) AS I_PRECIO_VTA_7,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_8 ) AS I_PRECIO_VTA_8,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_9 ) AS I_PRECIO_VTA_9,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_10) AS I_PRECIO_VTA_10,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_11) AS I_PRECIO_VTA_11,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_12) AS I_PRECIO_VTA_12,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_13) AS I_PRECIO_VTA_13,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_14) AS I_PRECIO_VTA_14,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_15) AS I_PRECIO_VTA_15,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_16) AS I_PRECIO_VTA_16,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_17) AS I_PRECIO_VTA_17,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_18) AS I_PRECIO_VTA_18,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_19) AS I_PRECIO_VTA_19,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_20) AS I_PRECIO_VTA_20,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_21) AS I_PRECIO_VTA_21,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_22) AS I_PRECIO_VTA_22,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_23) AS I_PRECIO_VTA_23,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_24) AS I_PRECIO_VTA_24,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_25) AS I_PRECIO_VTA_25,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_26) AS I_PRECIO_VTA_26,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_27) AS I_PRECIO_VTA_27,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_28) AS I_PRECIO_VTA_28,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_29) AS I_PRECIO_VTA_29,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_30) AS I_PRECIO_VTA_30,
            TRY_CONVERT(DECIMAL(19,4), I_PRECIO_VTA_31) AS I_PRECIO_VTA_31,

            'BARRIO'                     AS FUENTE_ORIGEN,
            GETDATE()                    AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL) AS CDC_LSN,
            0                            AS ESTADO_SINCRONIZACION
        
        FROM [DIARCO-BARRIO].[DiarcoBarrio].[dbo].[T710_ESTADIS_PRECIOS]
        WHERE (C_ANIO * 100 + C_MES) BETWEEN @PeriodoDesde AND @PeriodoHasta
		AND C_SUCU_EMPR > 300;

        -- Filas con algún precio no convertible (quedó NULL por TRY_CONVERT)
        SELECT s.*
        INTO #Bad
        FROM #Src AS s
        CROSS APPLY (VALUES
            (s.I_PRECIO_VTA_1 ),(s.I_PRECIO_VTA_2 ),(s.I_PRECIO_VTA_3 ),(s.I_PRECIO_VTA_4 ),(s.I_PRECIO_VTA_5 ),
            (s.I_PRECIO_VTA_6 ),(s.I_PRECIO_VTA_7 ),(s.I_PRECIO_VTA_8 ),(s.I_PRECIO_VTA_9 ),(s.I_PRECIO_VTA_10),
            (s.I_PRECIO_VTA_11),(s.I_PRECIO_VTA_12),(s.I_PRECIO_VTA_13),(s.I_PRECIO_VTA_14),(s.I_PRECIO_VTA_15),
            (s.I_PRECIO_VTA_16),(s.I_PRECIO_VTA_17),(s.I_PRECIO_VTA_18),(s.I_PRECIO_VTA_19),(s.I_PRECIO_VTA_20),
            (s.I_PRECIO_VTA_21),(s.I_PRECIO_VTA_22),(s.I_PRECIO_VTA_23),(s.I_PRECIO_VTA_24),(s.I_PRECIO_VTA_25),
            (s.I_PRECIO_VTA_26),(s.I_PRECIO_VTA_27),(s.I_PRECIO_VTA_28),(s.I_PRECIO_VTA_29),(s.I_PRECIO_VTA_30),
            (s.I_PRECIO_VTA_31)
        ) v(x)
        WHERE v.x IS NULL
        GROUP BY
            s.C_ANIO, s.C_MES, s.C_ARTICULO, s.C_SUCU_EMPR,
            s.I_PRECIO_VTA_1 , s.I_PRECIO_VTA_2 , s.I_PRECIO_VTA_3 , s.I_PRECIO_VTA_4 , s.I_PRECIO_VTA_5 ,
            s.I_PRECIO_VTA_6 , s.I_PRECIO_VTA_7 , s.I_PRECIO_VTA_8 , s.I_PRECIO_VTA_9 , s.I_PRECIO_VTA_10,
            s.I_PRECIO_VTA_11, s.I_PRECIO_VTA_12, s.I_PRECIO_VTA_13, s.I_PRECIO_VTA_14, s.I_PRECIO_VTA_15,
            s.I_PRECIO_VTA_16, s.I_PRECIO_VTA_17, s.I_PRECIO_VTA_18, s.I_PRECIO_VTA_19, s.I_PRECIO_VTA_20,
            s.I_PRECIO_VTA_21, s.I_PRECIO_VTA_22, s.I_PRECIO_VTA_23, s.I_PRECIO_VTA_24, s.I_PRECIO_VTA_25,
            s.I_PRECIO_VTA_26, s.I_PRECIO_VTA_27, s.I_PRECIO_VTA_28, s.I_PRECIO_VTA_29, s.I_PRECIO_VTA_30,
            s.I_PRECIO_VTA_31,
            s.FUENTE_ORIGEN, s.FECHA_EXTRACCION, s.CDC_LSN, s.ESTADO_SINCRONIZACION;

        -- Insertar al STG sólo las filas “buenas”
        INSERT INTO repl.T710_ESTADIS_PRECIOS_STG (
            C_ANIO, C_MES, C_ARTICULO, C_SUCU_EMPR,
            I_PRECIO_VTA_1 , I_PRECIO_VTA_2 , I_PRECIO_VTA_3 , I_PRECIO_VTA_4 , I_PRECIO_VTA_5 ,
            I_PRECIO_VTA_6 , I_PRECIO_VTA_7 , I_PRECIO_VTA_8 , I_PRECIO_VTA_9 , I_PRECIO_VTA_10, I_PRECIO_VTA_11, I_PRECIO_VTA_12, I_PRECIO_VTA_13, I_PRECIO_VTA_14,
            I_PRECIO_VTA_15, I_PRECIO_VTA_16, I_PRECIO_VTA_17, I_PRECIO_VTA_18, I_PRECIO_VTA_19, I_PRECIO_VTA_20, I_PRECIO_VTA_21, I_PRECIO_VTA_22, I_PRECIO_VTA_23,
            I_PRECIO_VTA_24, I_PRECIO_VTA_25, I_PRECIO_VTA_26, I_PRECIO_VTA_27, I_PRECIO_VTA_28, I_PRECIO_VTA_29, I_PRECIO_VTA_30, I_PRECIO_VTA_31,
            FUENTE_ORIGEN, FECHA_EXTRACCION, CDC_LSN, ESTADO_SINCRONIZACION
        )
        SELECT
            s.C_ANIO, s.C_MES, s.C_ARTICULO, s.C_SUCU_EMPR,
            s.I_PRECIO_VTA_1 , s.I_PRECIO_VTA_2 , s.I_PRECIO_VTA_3 , s.I_PRECIO_VTA_4 , s.I_PRECIO_VTA_5 ,
            s.I_PRECIO_VTA_6 , s.I_PRECIO_VTA_7 , s.I_PRECIO_VTA_8 , s.I_PRECIO_VTA_9 , s.I_PRECIO_VTA_10, s.I_PRECIO_VTA_11, s.I_PRECIO_VTA_12, s.I_PRECIO_VTA_13, s.I_PRECIO_VTA_14,
            s.I_PRECIO_VTA_15, s.I_PRECIO_VTA_16, s.I_PRECIO_VTA_17, s.I_PRECIO_VTA_18, s.I_PRECIO_VTA_19, s.I_PRECIO_VTA_20, s.I_PRECIO_VTA_21, s.I_PRECIO_VTA_22, s.I_PRECIO_VTA_23,
            s.I_PRECIO_VTA_24, s.I_PRECIO_VTA_25, s.I_PRECIO_VTA_26, s.I_PRECIO_VTA_27, s.I_PRECIO_VTA_28, s.I_PRECIO_VTA_29, s.I_PRECIO_VTA_30, s.I_PRECIO_VTA_31,
            s.FUENTE_ORIGEN, s.FECHA_EXTRACCION, s.CDC_LSN, s.ESTADO_SINCRONIZACION
        FROM #Src s
        WHERE NOT EXISTS (
            SELECT 1
            FROM #Bad b
            WHERE b.C_ANIO = s.C_ANIO
              AND b.C_MES  = s.C_MES
              AND b.C_ARTICULO = s.C_ARTICULO
              AND b.C_SUCU_EMPR = s.C_SUCU_EMPR
        );

        -- Logging de descartes por conversión (si los hubiera)
        IF EXISTS (SELECT 1 FROM #Bad)
        BEGIN
            INSERT INTO repl.LOGS_T710_ESTADIS_PRECIOS_SYNC (fecha_ejecucion, estado, mensaje, duracion_segundos)
            VALUES (GETDATE(), 'WARN',
                    CONCAT('Filas con precios no convertibles: ', (SELECT COUNT(*) FROM #Bad)),
                    DATEDIFF(SECOND, @inicio, GETDATE()));
        END

        ----------------------------------------------------------------------
        -- 3) MERGE final a tabla REPL
        --    (asume REPL con DECIMAL(19,4) en los 31 precios)
        ----------------------------------------------------------------------
        MERGE repl.T710_ESTADIS_PRECIOS AS TARGET
        USING (
            SELECT
                C_ANIO, C_MES, C_SUCU_EMPR, C_ARTICULO,
                I_PRECIO_VTA_1 , I_PRECIO_VTA_2 , I_PRECIO_VTA_3 , I_PRECIO_VTA_4 , I_PRECIO_VTA_5 ,
                I_PRECIO_VTA_6 , I_PRECIO_VTA_7 , I_PRECIO_VTA_8 , I_PRECIO_VTA_9 , I_PRECIO_VTA_10, I_PRECIO_VTA_11, I_PRECIO_VTA_12, I_PRECIO_VTA_13, I_PRECIO_VTA_14,
                I_PRECIO_VTA_15, I_PRECIO_VTA_16, I_PRECIO_VTA_17, I_PRECIO_VTA_18, I_PRECIO_VTA_19, I_PRECIO_VTA_20, I_PRECIO_VTA_21, I_PRECIO_VTA_22, I_PRECIO_VTA_23,
                I_PRECIO_VTA_24, I_PRECIO_VTA_25, I_PRECIO_VTA_26, I_PRECIO_VTA_27, I_PRECIO_VTA_28, I_PRECIO_VTA_29, I_PRECIO_VTA_30, I_PRECIO_VTA_31,FUENTE_ORIGEN
            FROM repl.T710_ESTADIS_PRECIOS_STG
        ) AS SOURCE
        ON  TARGET.C_ANIO      = SOURCE.C_ANIO
        AND TARGET.C_MES       = SOURCE.C_MES
        AND TARGET.C_SUCU_EMPR = SOURCE.C_SUCU_EMPR
        AND TARGET.C_ARTICULO  = SOURCE.C_ARTICULO

        WHEN MATCHED THEN
            UPDATE SET
                TARGET.I_PRECIO_VTA_1  = SOURCE.I_PRECIO_VTA_1,
                TARGET.I_PRECIO_VTA_2  = SOURCE.I_PRECIO_VTA_2,
                TARGET.I_PRECIO_VTA_3  = SOURCE.I_PRECIO_VTA_3,
                TARGET.I_PRECIO_VTA_4  = SOURCE.I_PRECIO_VTA_4,
                TARGET.I_PRECIO_VTA_5  = SOURCE.I_PRECIO_VTA_5,
                TARGET.I_PRECIO_VTA_6  = SOURCE.I_PRECIO_VTA_6,
                TARGET.I_PRECIO_VTA_7  = SOURCE.I_PRECIO_VTA_7,
                TARGET.I_PRECIO_VTA_8  = SOURCE.I_PRECIO_VTA_8,
                TARGET.I_PRECIO_VTA_9  = SOURCE.I_PRECIO_VTA_9,
                TARGET.I_PRECIO_VTA_10 = SOURCE.I_PRECIO_VTA_10,
                TARGET.I_PRECIO_VTA_11 = SOURCE.I_PRECIO_VTA_11,
                TARGET.I_PRECIO_VTA_12 = SOURCE.I_PRECIO_VTA_12,
                TARGET.I_PRECIO_VTA_13 = SOURCE.I_PRECIO_VTA_13,
                TARGET.I_PRECIO_VTA_14 = SOURCE.I_PRECIO_VTA_14,
                TARGET.I_PRECIO_VTA_15 = SOURCE.I_PRECIO_VTA_15,
                TARGET.I_PRECIO_VTA_16 = SOURCE.I_PRECIO_VTA_16,
                TARGET.I_PRECIO_VTA_17 = SOURCE.I_PRECIO_VTA_17,
                TARGET.I_PRECIO_VTA_18 = SOURCE.I_PRECIO_VTA_18,
                TARGET.I_PRECIO_VTA_19 = SOURCE.I_PRECIO_VTA_19,
                TARGET.I_PRECIO_VTA_20 = SOURCE.I_PRECIO_VTA_20,
                TARGET.I_PRECIO_VTA_21 = SOURCE.I_PRECIO_VTA_21,
                TARGET.I_PRECIO_VTA_22 = SOURCE.I_PRECIO_VTA_22,
                TARGET.I_PRECIO_VTA_23 = SOURCE.I_PRECIO_VTA_23,
                TARGET.I_PRECIO_VTA_24 = SOURCE.I_PRECIO_VTA_24,
                TARGET.I_PRECIO_VTA_25 = SOURCE.I_PRECIO_VTA_25,
                TARGET.I_PRECIO_VTA_26 = SOURCE.I_PRECIO_VTA_26,
                TARGET.I_PRECIO_VTA_27 = SOURCE.I_PRECIO_VTA_27,
                TARGET.I_PRECIO_VTA_28 = SOURCE.I_PRECIO_VTA_28,
                TARGET.I_PRECIO_VTA_29 = SOURCE.I_PRECIO_VTA_29,
                TARGET.I_PRECIO_VTA_30 = SOURCE.I_PRECIO_VTA_30,
                TARGET.I_PRECIO_VTA_31 = SOURCE.I_PRECIO_VTA_31,
				TARGET.FUENTE_ORIGEN = SOURCE.FUENTE_ORIGEN,
                TARGET.FECHA_EXTRACCION = GETDATE(),
                TARGET.ESTADO_SINCRONIZACION = 0

        WHEN NOT MATCHED THEN
            INSERT (
                C_ANIO, C_MES, C_SUCU_EMPR, C_ARTICULO,
                I_PRECIO_VTA_1 , I_PRECIO_VTA_2 , I_PRECIO_VTA_3 , I_PRECIO_VTA_4 , I_PRECIO_VTA_5 ,
                I_PRECIO_VTA_6 , I_PRECIO_VTA_7 , I_PRECIO_VTA_8 , I_PRECIO_VTA_9 ,
                I_PRECIO_VTA_10, I_PRECIO_VTA_11, I_PRECIO_VTA_12, I_PRECIO_VTA_13, I_PRECIO_VTA_14, I_PRECIO_VTA_15, I_PRECIO_VTA_16, I_PRECIO_VTA_17,
                I_PRECIO_VTA_18, I_PRECIO_VTA_19, I_PRECIO_VTA_20, I_PRECIO_VTA_21, I_PRECIO_VTA_22, I_PRECIO_VTA_23, I_PRECIO_VTA_24, I_PRECIO_VTA_25,
                I_PRECIO_VTA_26, I_PRECIO_VTA_27, I_PRECIO_VTA_28, I_PRECIO_VTA_29, I_PRECIO_VTA_30, I_PRECIO_VTA_31,FUENTE_ORIGEN,
                FECHA_EXTRACCION, ESTADO_SINCRONIZACION
            )
            VALUES (
                SOURCE.C_ANIO, SOURCE.C_MES, SOURCE.C_SUCU_EMPR, SOURCE.C_ARTICULO,
                SOURCE.I_PRECIO_VTA_1 , SOURCE.I_PRECIO_VTA_2 , SOURCE.I_PRECIO_VTA_3 , SOURCE.I_PRECIO_VTA_4 , SOURCE.I_PRECIO_VTA_5 ,
                SOURCE.I_PRECIO_VTA_6 , SOURCE.I_PRECIO_VTA_7 , SOURCE.I_PRECIO_VTA_8 , SOURCE.I_PRECIO_VTA_9 ,
                SOURCE.I_PRECIO_VTA_10, SOURCE.I_PRECIO_VTA_11, SOURCE.I_PRECIO_VTA_12, SOURCE.I_PRECIO_VTA_13, SOURCE.I_PRECIO_VTA_14, SOURCE.I_PRECIO_VTA_15, SOURCE.I_PRECIO_VTA_16, SOURCE.I_PRECIO_VTA_17,
                SOURCE.I_PRECIO_VTA_18, SOURCE.I_PRECIO_VTA_19, SOURCE.I_PRECIO_VTA_20, SOURCE.I_PRECIO_VTA_21, SOURCE.I_PRECIO_VTA_22, SOURCE.I_PRECIO_VTA_23, SOURCE.I_PRECIO_VTA_24, SOURCE.I_PRECIO_VTA_25,
                SOURCE.I_PRECIO_VTA_26, SOURCE.I_PRECIO_VTA_27, SOURCE.I_PRECIO_VTA_28, SOURCE.I_PRECIO_VTA_29, SOURCE.I_PRECIO_VTA_30, SOURCE.I_PRECIO_VTA_31, SOURCE.FUENTE_ORIGEN,
                GETDATE(), 0
            );

        SET @total   = @@ROWCOUNT;
        SET @mensaje = CONCAT(N'MERGE finalizado OK. Período: ', @PeriodoDesde, N' a ', @PeriodoHasta);
    END TRY
    BEGIN CATCH
        SET @mensaje = ERROR_MESSAGE();
        INSERT INTO repl.LOGS_T710_ESTADIS_PRECIOS_SYNC
            (fecha_ejecucion, estado, mensaje, duracion_segundos)
        VALUES
            (GETDATE(), 'ERROR', @mensaje, DATEDIFF(SECOND, @inicio, GETDATE()));
        THROW;
    END CATCH;

    ----------------------------------------------------------------------
    -- 4) Registrar log de ejecución
    ----------------------------------------------------------------------
    INSERT INTO repl.LOGS_T710_ESTADIS_PRECIOS_SYNC
        (fecha_ejecucion, estado, mensaje, registros_afectados, duracion_segundos)
    VALUES
        (@inicio, 'OK', @mensaje, @total, DATEDIFF(SECOND, @inicio, GETDATE()));
END;

GO
