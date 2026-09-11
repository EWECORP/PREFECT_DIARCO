-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [dbo].[SP_CNX_T_8_STOCK]
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE T_8_STOCK;

    -- Definir la fecha de referencia
    DECLARE @fecha AS DATE = GETDATE() - 1;

    -- Calcular las unidades pendientes de entrega por sucursal y artículo
    SELECT 
        OC_CABE.C_SUCU_COMPRA AS SUCU_COMPRA,
        OC_DETA.C_ARTICULO AS C_ARTICULO,
        SUM((OC_DETA.Q_BULTOS_PROV_PED * OC_DETA.Q_FACTOR_PROV_PED) - OC_DETA.Q_UNID_CUMPLIDAS) AS Pendientes
    INTO #pedidos_pendientes
    FROM 
        [DIARCOP001].[DIARCOP].DBO.T080_OC_CABE OC_CABE
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T081_OC_DETA OC_DETA 
        ON OC_DETA.C_OC = OC_CABE.C_OC 
        AND OC_DETA.U_PREFIJO_OC = OC_CABE.U_PREFIJO_OC 
        AND OC_DETA.U_SUFIJO_OC = OC_CABE.U_SUFIJO_OC
    WHERE 
        ((OC_DETA.Q_BULTOS_PROV_PED * OC_DETA.Q_FACTOR_PROV_PED) - OC_DETA.Q_UNID_CUMPLIDAS) <> 0
        AND OC_CABE.C_SITUAC = 1
        AND OC_CABE.C_SUCU_COMPRA <> 300
    GROUP BY 
        OC_CABE.C_SUCU_COMPRA,
        OC_DETA.C_ARTICULO;

    -- Obtener las promociones vigentes por vencimiento
    SELECT 
        C_SUCU_EMPR AS C_SUCU_EMPR, 
        C_ARTICULO AS C_ARTICULO, 
        1 AS FLAG
    INTO #promo_venci
    FROM 
        [DIARCOP001].[DIARCOP].DBO.T230_facturador_negocios_especiales_por_cantidad
    WHERE 
        REPLACE(CONVERT(VARCHAR, @fecha, 111), '/', '-') BETWEEN F_DESDE AND F_HASTA 
        AND q_unidades_kilos_saldo > 0
    UNION ALL
    SELECT 
        C_SUCU_EMPR AS C_SUCU_EMPR, 
        C_ARTICULO AS C_ARTICULO, 
        1 AS FLAG
    FROM 
        [DIARCO-BARRIO].[DIARCOBARRIO].DBO.T230_facturador_negocios_especiales_por_cantidad 
    WHERE 
        REPLACE(CONVERT(VARCHAR, @fecha, 111), '/', '-') BETWEEN F_DESDE AND F_HASTA 
        AND q_unidades_kilos_saldo > 0;

    -- Insertar los datos en la tabla T_8_STOCK
    INSERT INTO T_8_STOCK (
        C_ARTICULO, C_SUCU_EMPR, FECHA_VIGENCIA, Q_UNID_PESO_ARTICULO, QTY_PENDIENTE, PRECIO_COSTO, PRECIO_VENTA, FLAG_OFERTA, FLAG_PROMO, F_DATO, F_PROC
    )
    SELECT 
        CONVERT(VARCHAR, STK.C_ARTICULO) AS C_ARTICULO,
        CASE STK.C_SUCU_EMPR 
            WHEN 41 THEN '41CD' 
            ELSE DBO.[NORMALIZA_STRING](STK.C_SUCU_EMPR) 
        END AS C_SUCU_EMPR,
        REPLACE(CONVERT(VARCHAR, @fecha, 111), '/', '-') AS FECHA_VIGENCIA,
        CASE 
            WHEN ART.M_VENDE_POR_PESO = 'N' THEN STK.Q_UNID_ARTICULO 
            ELSE STK.Q_PESO_ARTICULO 
        END AS Q_UNID_PESO_ARTICULO,
        ISNULL(PP.Pendientes, 0) AS QTY_PENDIENTE,
        COSTO.I_LISTA_CALCULADO AS PRECIO_COSTO,
        ART_SUC.I_PRECIO_VTA AS PRECIO_VENTA,
        CASE 
            WHEN ART_SUC.M_OFERTA_SUCU = 'N' THEN '0' 
            ELSE '1' 
        END AS FLAG_OFERTA,
        CASE 
            WHEN PROMO_VENCI.FLAG = 1 THEN '1' 
            ELSE '0' 
        END AS FLAG_PROMO,
        GETDATE() AS F_DATO,  -- Fecha y hora de vigencia del dato
        GETDATE() AS F_PROC   -- Fecha y hora de procesamiento
    FROM 
        [DIARCOP001].[DIARCOP].DBO.T060_STOCK STK
    LEFT JOIN 
        #pedidos_pendientes PP 
        ON PP.C_ARTICULO = STK.C_ARTICULO 
        AND PP.SUCU_COMPRA = STK.C_SUCU_EMPR
    LEFT JOIN 
        #promo_venci PROMO_VENCI 
        ON PROMO_VENCI.C_SUCU_EMPR = STK.C_SUCU_EMPR 
        AND PROMO_VENCI.C_ARTICULO = STK.C_ARTICULO
    LEFT JOIN 
        [DIARCOP001].[DIARCOP].DBO.T055_ARTICULOS_CONDCOMPRA_COSTOS COSTO 
        ON COSTO.C_ARTICULO = STK.C_ARTICULO 
        AND COSTO.C_SUCU_EMPR = STK.C_SUCU_EMPR
    LEFT JOIN 
        [DIARCOP001].[DIARCOP].DBO.T051_ARTICULOS_SUCURSAL ART_SUC 
        ON ART_SUC.C_ARTICULO = STK.C_ARTICULO 
        AND ART_SUC.C_SUCU_EMPR = STK.C_SUCU_EMPR
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T050_ARTICULOS ART 
        ON ART.C_ARTICULO = STK.C_ARTICULO  
        AND ART.C_PROVEEDOR_PRIMARIO = COSTO.C_PROVEEDOR
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC 
        ON STK.C_SUCU_EMPR = SUC.C_SUCU_EMPR
    WHERE 
        SUC.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR 
            FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]
        );

    -- Eliminar las tablas temporales
    DROP TABLE #pedidos_pendientes, #promo_venci;

END;

GO
