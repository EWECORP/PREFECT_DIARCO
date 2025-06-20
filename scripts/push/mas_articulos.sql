-- Versión refactorizada del Stored Procedure SP_CNX_T_4_PRODUCTOS_SUCU
-- Mejora: uso de CTEs, mayor legibilidad, limpieza de lógica repetida, y parametrización

USE [data-sync]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[SP_CNX_T_4_PRODUCTOS_SUCURSAL]
    @C_SUCU_EMPR INT = NULL,
    @C_FAMILIA INT = NULL,
    @INCLUIR_NO_HABILITADOS BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    -- CTE 1: Artículos con stock (surtido base)
    WITH CTE_Surtido AS (
        SELECT DISTINCT C_ARTICULO
        FROM [DIARCOP001].[DIARCOP].DBO.T060_STOCK
    ),

    -- CTE 2: Habilitaciones en entorno "barrio"
    CTE_MarcaBarrio AS (
        SELECT C_SUCU_EMPR, C_ARTICULO, M_HABILITADO_SUCU
        FROM [diarco-barrio].[diarcobarrio].dbo.T051_ARTICULOS_SUCURSAL
        WHERE C_ARTICULO IN (SELECT C_ARTICULO FROM CTE_Surtido)
    ),

    -- CTE 3: Fechas de vigencia por sucursal-artículo
    CTE_Vigencia AS (
        SELECT
            suc.C_SUCU_EMPR,
            suc.C_ARTICULO,
            ALTA = CONVERT(VARCHAR, 
                CASE 
                    WHEN MAX(CASE M_LISTO_PARA_VENTA_ACT WHEN 'S' THEN F_ALTA_SIST END) > MAX(suc.F_ALTA) 
                        THEN MAX(CASE M_LISTO_PARA_VENTA_ACT WHEN 'S' THEN F_ALTA_SIST END)
                    ELSE MAX(suc.F_ALTA)
                END, 23),
            BAJA = ISNULL(
                CASE 
                    WHEN (
                        CASE 
                            WHEN MAX(CASE M_LISTO_PARA_VENTA_ACT WHEN 'S' THEN F_ALTA_SIST END) > MAX(suc.F_ALTA)
                                THEN MAX(CASE M_LISTO_PARA_VENTA_ACT WHEN 'S' THEN F_ALTA_SIST END)
                            ELSE MAX(suc.F_ALTA)
                        END
                    ) > MAX(CASE M_LISTO_PARA_VENTA_ACT WHEN 'N' THEN F_ALTA_SIST END)
                        THEN NULL
                    ELSE MAX(CASE M_LISTO_PARA_VENTA_ACT WHEN 'N' THEN F_ALTA_SIST END)
                END,
                CASE WHEN MAX(M_HABILITADO_SUCU) = 'N' THEN MAX(suc.F_ALTA) ELSE NULL END
            )
        FROM [DIARCOP001].[DIARCOP].dbo.T051_ARTICULOS_SUCURSAL suc
        INNER JOIN [DIARCOP001].[DiarcoP].dbo.t050_articulos art ON art.C_ARTICULO = suc.C_ARTICULO
        LEFT JOIN [DIARCOP001].[DIARCOP].dbo.T804_HIST_MARCA_LISTO_PARA_VENTA hist
            ON hist.C_SUCU_EMPR = suc.C_SUCU_EMPR AND hist.C_ARTICULO = suc.C_ARTICULO
        WHERE suc.C_SUCU_EMPR <> 300 AND art.M_BAJA = 'N'
        GROUP BY suc.C_SUCU_EMPR, suc.C_ARTICULO
    )

    -- Selección final
    SELECT 
        C_SUCU_EMPR = CASE suc.C_SUCU_EMPR WHEN 41 THEN '41CD' ELSE DBO.NORMALIZA_STRING(suc.C_SUCU_EMPR) END,
        C_ARTICULO = CONVERT(VARCHAR, suc.C_ARTICULO),
        ABASTECIMIENTO = CONVERT(VARCHAR, C_SISTEMATICA),
        COD_CD = CASE WHEN suc.C_SUCU_EMPR < 300 THEN '41CD' ELSE '82' END,
        LINEA = CASE WHEN ((suc.C_SUCU_EMPR < 300 AND suc.M_HABILITADO_SUCU = 'N') OR 
                            (suc.C_SUCU_EMPR > 300 AND mb.M_HABILITADO_SUCU = 'N')) THEN '0' ELSE '1' END,
        FECHA_REGISTRO = v.ALTA,
        FECHA_SALIDA_LINEA = v.BAJA,
        UNID_TRANSFERENCIA = '0',
        Q_UNID_TRANSFERENCIA = '1',
        PEDIDO_MIN = ISNULL(CONVERT(VARCHAR, Q_BULTOS_KILOS_COMPRA_MINIMA), '0'),
        FRENTE_LINEAL = '1',
        CAPACID_GONDOLA = '1',
        STOCK_MINIMO = '1',
        COD_COMPRADOR = CONVERT(VARCHAR, art.C_COMPRADOR),
        PROMOCION = CASE suc.M_OFERTA_SUCU WHEN 'N' THEN '0' ELSE '1' END,
        ACTIVE_FOR_PURCHASE = CASE WHEN (art.C_FAMILIA = 4 OR art.M_A_DAR_DE_BAJA = 'S' OR 
                                         (suc.C_SUCU_EMPR < 300 AND suc.M_HABILITADO_SUCU = 'N') OR 
                                         (suc.C_SUCU_EMPR > 300 AND mb.M_HABILITADO_SUCU = 'N')) THEN '0' ELSE '1' END,
        ACTIVE_FOR_SALE = CASE suc.M_LISTO_PARA_VENTA_SUCU WHEN 'N' THEN '0' ELSE '1' END,
        ACTIVE_ON_MIX = CASE WHEN art.C_FAMILIA = 4 OR art.M_A_DAR_DE_BAJA = 'S' THEN '0' ELSE '1' END,
        DELIVERED_ID = CASE C_SISTEMATICA 
                           WHEN 0 THEN CASE WHEN suc.C_SUCU_EMPR < 300 THEN '41CD' ELSE '82' END
                           WHEN 1 THEN CONVERT(VARCHAR, prov.C_PROVEEDOR) 
                       END,
        PRODUCT_BASE_ID = '',
        OWN_PRODUCTION = '0',
        FULL_CAPACITY_PALLET = CONVERT(VARCHAR, U_PISO_PALETIZADO * U_ALTURA_PALETIZADO),
        NUMBER_OF_LAYERS = CONVERT(VARCHAR, U_ALTURA_PALETIZADO),
        NUMBER_OF_BOXES_PER_BALLAST = CASE 
            WHEN art.M_VENDE_POR_PESO = 'N' THEN CONVERT(VARCHAR, U_PISO_PALETIZADO)
            ELSE CONVERT(VARCHAR, prov.Q_FACTOR_PROVEEDOR) 
        END
    FROM [DIARCOP001].[DIARCOP].dbo.T051_ARTICULOS_SUCURSAL suc
    INNER JOIN [DIARCOP001].[DiarcoP].dbo.t050_articulos art ON art.C_ARTICULO = suc.C_ARTICULO
    LEFT JOIN CTE_MarcaBarrio mb ON mb.C_ARTICULO = suc.C_ARTICULO AND mb.C_SUCU_EMPR = suc.C_SUCU_EMPR
    LEFT JOIN [DIARCOP001].[DiarcoP].dbo.T052_ARTICULOS_PROVEEDOR prov ON prov.C_ARTICULO = art.C_ARTICULO
    LEFT JOIN [DIARCOP001].[DIARCOP].dbo.T020_Proveedor_Dias_Entrega_Deta entrega 
        ON entrega.C_PROVEEDOR = prov.C_PROVEEDOR AND entrega.C_SUCU_EMPR = suc.C_SUCU_EMPR
    INNER JOIN [DIARCOP001].[DIARCOP].dbo.T100_EMPRESA_SUC SUC_MAE 
        ON SUC_MAE.C_SUCU_EMPR = suc.C_SUCU_EMPR AND SUC_MAE.M_SUCU_VIRTUAL = 'N'
    INNER JOIN CTE_Vigencia v ON v.C_SUCU_EMPR = suc.C_SUCU_EMPR AND v.C_ARTICULO = suc.C_ARTICULO
    WHERE
        suc.C_ARTICULO IN (SELECT C_ARTICULO FROM CTE_Surtido)
        AND suc.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND suc.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].dbo.T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB)
        AND (@C_SUCU_EMPR IS NULL OR suc.C_SUCU_EMPR = @C_SUCU_EMPR)
        AND (@C_FAMILIA IS NULL OR art.C_FAMILIA = @C_FAMILIA)
        AND (
            @INCLUIR_NO_HABILITADOS = 1 OR 
            ((art.C_FAMILIA <> 4) AND (art.M_A_DAR_DE_BAJA <> 'S') AND suc.M_HABILITADO_SUCU = 'S')
        );

END
GO
