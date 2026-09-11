-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [dbo].[SP_CNX_T_11_1_PROMOCIONES]
AS
BEGIN
    SET NOCOUNT ON;
    
	TRUNCATE TABLE T_11_1_PROMOCIONES;

    -- Definir la fecha de referencia para las promociones
    DECLARE @fecha AS DATE = GETDATE() - 1;

    -- Insertar los datos en la tabla T_11_1_PROMOCIONES con las fechas correspondientes
    INSERT INTO T_11_1_PROMOCIONES (COD_ARTICULO, NOMBRE_ARTICULO, TIPO_PROMOCION, ESTADO_PROMOCION, F_DATO, F_PROC)
    
    -- Primera consulta
    SELECT DISTINCT  
        CAST(ART_SUC.C_ARTICULO AS VARCHAR(5)) AS COD_ARTICULO,
        DBO.[NORMALIZA_STRING](ART.N_ARTICULO) AS NOMBRE_ARTICULO,
        '2' AS TIPO_PROMOCION,
        '0' AS ESTADO_PROMOCION,
        GETDATE() AS F_DATO, -- Fecha y hora de vigencia del dato
        GETDATE() AS F_PROC  -- Fecha y hora de procesamiento
    FROM  
        [DIARCOP001].[DIARCOP].dbo.[T051_ARTICULOS_SUCURSAL] ART_SUC
    INNER JOIN 
        [DIARCOP001].[DiarcoP].dbo.t050_articulos ART 
        ON ART_SUC.C_ARTICULO = ART.C_ARTICULO
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC 
        ON SUC.C_SUCU_EMPR = ART_SUC.C_SUCU_EMPR
    WHERE 
        SUC.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR 
            FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]
        )
        AND M_OFERTA_SUCU = 'S'

    -- Segunda consulta
    UNION ALL
    SELECT DISTINCT
        CONVERT(VARCHAR, promo_venci.C_ARTICULO) AS COD_ARTICULO,
        DBO.[NORMALIZA_STRING](ART.N_ARTICULO) AS NOMBRE_ARTICULO,
        '2' AS TIPO_PROMOCION,
        '1' AS ESTADO_PROMOCION,
        GETDATE() AS F_DATO, -- Fecha y hora de vigencia del dato
        GETDATE() AS F_PROC  -- Fecha y hora de procesamiento
    FROM  
        [DIARCOP001].[DIARCOP].DBO.T230_facturador_negocios_especiales_por_cantidad promo_venci
    INNER JOIN 
        [DIARCOP001].[DiarcoP].dbo.t050_articulos ART 
        ON ART.C_ARTICULO = promo_venci.C_ARTICULO
    WHERE 
        REPLACE(CONVERT(VARCHAR, @fecha, 111), '/', '-') BETWEEN F_DESDE AND F_HASTA 
        AND q_unidades_kilos_saldo > 0

    -- Tercera consulta
    UNION ALL
    SELECT DISTINCT
        CONVERT(VARCHAR, promo_venci.C_ARTICULO) AS COD_ARTICULO,
        DBO.[NORMALIZA_STRING](ART.N_ARTICULO) AS NOMBRE_ARTICULO,
        '2' AS TIPO_PROMOCION,
        '1' AS ESTADO_PROMOCION,
        GETDATE() AS F_DATO, -- Fecha y hora de vigencia del dato
        GETDATE() AS F_PROC  -- Fecha y hora de procesamiento
    FROM  
        [DIARCO-BARRIO].[DIARCOBARRIO].DBO.T230_facturador_negocios_especiales_por_cantidad promo_venci
    INNER JOIN 
        [DIARCOP001].[DiarcoP].dbo.t050_articulos ART 
        ON ART.C_ARTICULO = promo_venci.C_ARTICULO
    WHERE 
        REPLACE(CONVERT(VARCHAR, @fecha, 111), '/', '-') BETWEEN F_DESDE AND F_HASTA 
        AND q_unidades_kilos_saldo > 0;

END;

GO
