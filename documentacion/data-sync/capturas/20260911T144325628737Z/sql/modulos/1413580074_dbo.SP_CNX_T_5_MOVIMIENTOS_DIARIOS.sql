-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO





CREATE PROCEDURE [dbo].[SP_CNX_T_5_MOVIMIENTOS_DIARIOS]
AS
BEGIN

    SET NOCOUNT ON;
	DECLARE @lastday DATETIME2(7) = ( SELECT MAX(F_MOVIMIENTO) FROM T_5_MOVIMIENTOS_DIARIOS); 

	DECLARE @fecha_desde AS DATETIME = DATEADD(DAY, -1, @lastday ) ; 
    DECLARE @fecha_hasta AS DATETIME = CAST(GETDATE() AS DATE);
	
	select @fecha_desde;
	select @fecha_hasta;

	TRUNCATE TABLE T_5_MOVIMIENTOS_DIARIOS;

    -- Insertar movimientos de stock
    INSERT INTO [data-sync].[dbo].[T_5_MOVIMIENTOS_DIARIOS] (
        C_ARTICULO, C_SUCURSAL, F_MOVIMIENTO, C_MOVIMIENTO, CANTIDAD, I_PRECIO_VTA,I_COSTO_ESTADISTICO, I_COSTO_PPP,PIS, F_PROC
    )
    SELECT 
        CONVERT(VARCHAR, C_ARTICULO) AS C_ARTICULO,
        CASE M.C_SUCU_EMPR 
            WHEN 41 THEN '41CD' 
            ELSE DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) 
        END AS C_SUCURSAL,
        CONVERT(DATE, F_MOV) AS F_MOVIMIENTO,
        CASE 
            WHEN c_tabla = 44 THEN CAST(c_tabla AS VARCHAR(5))												/*3 - Por devolución*/
            WHEN c_tabla = 4 AND c_mov = 75 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5))	/*3 - Por devolución*/
            WHEN c_tabla = 23 THEN CAST(c_tabla AS VARCHAR(5))												/*4 - Ajuste de Inventario Positivo c_mov único 71*/
            WHEN c_tabla = 24 THEN CAST(c_tabla AS VARCHAR(5))												/*4 - Ajuste de Inventario Positivo c_mov único 71*/
            WHEN c_tabla = 106 THEN CAST(c_tabla AS VARCHAR(5))												/*5 - Desperdicio o Merma*/
            WHEN c_tabla = 4 AND c_mov = 77 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5))	/*5 - Desperdicio o Merma*/
            WHEN c_tabla = 43 AND c_mov = 74 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5))	/*7 + Transferencia ingreso*/ 
            WHEN c_tabla = 0 AND c_mov = 10 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5))	/*2 - Recibido en la tienda*/ 
            WHEN c_tabla = 43 AND c_mov = 73 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5))	/*7 - Transferencia salida*/ 
        END AS C_MOVIMIENTO,
        CONVERT(DECIMAL(18,2), Q_BULTOS_KGS_MOV * Q_FACTOR_PZAS_MOV) AS CANTIDAD,
        CONVERT(DECIMAL(18,2), I_PRECIO_VTA) AS I_PRECIO_VTA,
		CONVERT(DECIMAL(18,2), I_COSTO_ESTADISTICO) AS I_COSTO_ESTADISTICO,
		CONVERT(DECIMAL(18,2), I_COSTO_PPP) AS I_COSTO_PPP,
        0 AS PIS,  -- Por defecto en 0
        GETDATE() AS F_PROC   -- Fecha y hora de procesamiento
    -- SELECT TOP 1000 *
	FROM 
        [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T870_HIST_MOVIMIENTOS_STOCK] M
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC 
        ON SUC.C_SUCU_EMPR = M.C_SUCU_EMPR
    WHERE 
        F_MOV >= @fecha_desde 
        AND F_MOV < @fecha_hasta
        AND c_tabla IN (44, 23, 24, 106, 4, 0, 43)
        AND SUC.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]
        );

    -- Insertar movimientos de ventas

    INSERT INTO [data-sync].[dbo].[T_5_MOVIMIENTOS_DIARIOS] (
          C_ARTICULO, C_SUCURSAL, F_MOVIMIENTO, C_MOVIMIENTO, CANTIDAD, I_PRECIO_VTA,I_COSTO_ESTADISTICO, I_COSTO_PPP,PIS, F_PROC
    )
	--    DECLARE @fecha_desde AS DATETIME = GETDATE() -90; DECLARE @fecha_hasta AS DATETIME = GETDATE()-1;
    SELECT 
        CONVERT(VARCHAR, C_ARTICULO) AS C_ARTICULO,
        CASE M.C_SUCU_EMPR 
            WHEN 41 THEN '41CD' 
            ELSE DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) 
        END AS C_SUCURSAL,
        CONVERT(DATE, F_VENTA) AS F_MOVIMIENTO,
        '99' AS COD_MOVIMIENTO, -- Tipo Ventas
        CONVERT(DECIMAL(18,2), Q_UNIDADES_VENDIDAS) AS CANTIDAD,
        CONVERT(DECIMAL(18,2), I_PRECIO_VENTA) AS I_PRECIO_VTA,
		CONVERT(DECIMAL(18,2), I_PRECIO_COSTO) AS I_COSTO_ESTADISTICO,
		CONVERT(DECIMAL(18,2), I_PRECIO_COSTO_PP) AS I_COSTO_PPP,
        0 AS PIS,  -- Por defecto en 0
        GETDATE() AS F_PROC   -- Fecha y hora de procesamiento
		-- Select top 1000 *
    FROM 
        [DCO-DBCORE-P02].[DiarcoEst].[dbo].T702_EST_VTAS_POR_ARTICULO M
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC 
        ON SUC.C_SUCU_EMPR = M.C_SUCU_EMPR
    WHERE 
        CONVERT(DATE, F_VENTA) >= @fecha_desde 
        AND CONVERT(DATE, F_VENTA) < @fecha_hasta
        AND SUC.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]
        );

    -- Insertar movimientos de ventas de Diarco Barrio

    INSERT INTO [data-sync].[dbo].[T_5_MOVIMIENTOS_DIARIOS] (
          C_ARTICULO, C_SUCURSAL, F_MOVIMIENTO, C_MOVIMIENTO, CANTIDAD, I_PRECIO_VTA,I_COSTO_ESTADISTICO, I_COSTO_PPP,PIS, F_PROC
    )
 	--    DECLARE @fecha_desde AS DATETIME = GETDATE() -90; DECLARE @fecha_hasta AS DATETIME = GETDATE()-1;
 
	SELECT
        CONVERT(VARCHAR, C_ARTICULO) AS C_ARTICULO,
        CASE M.C_SUCU_EMPR 
            WHEN 41 THEN '41CD' 
            ELSE DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) 
        END AS C_SUCURSAL,
        CONVERT(DATE, F_VENTA) AS F_MOVIMIENTO,
        '99' AS COD_MOVIMIENTO, -- Tipo Ventas
        CONVERT(DECIMAL(18,2), Q_UNIDADES_VENDIDAS) AS CANTIDAD,
        CONVERT(DECIMAL(18,2), I_PRECIO_VENTA) AS I_PRECIO_VTA,
		CONVERT(DECIMAL(18,2), I_PRECIO_COSTO) AS I_COSTO_ESTADISTICO,
		CONVERT(DECIMAL(18,2), I_PRECIO_COSTO_PP) AS I_COSTO_PPP,
        0 AS PIS,  -- Por defecto en 0
        GETDATE() AS F_PROC   -- Fecha y hora de procesamiento
    		-- Select top 1000 *
	FROM 
        [DCO-DBCORE-P02].[DiarcoEst].[dbo].T702_EST_VTAS_POR_ARTICULO_DBARRIO M
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC 
        ON SUC.C_SUCU_EMPR = M.C_SUCU_EMPR
    WHERE 
        CONVERT(DATE, F_VENTA) >= @fecha_desde 
        AND CONVERT(DATE, F_VENTA) < @fecha_hasta
        AND SUC.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]
        );
END;

GO
