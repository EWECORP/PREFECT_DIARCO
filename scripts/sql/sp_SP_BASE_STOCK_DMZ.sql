/****** Object: StoredProcedure dbo.SP_BASE_STOCK_DMZ **/

USE [data-sync]
GO

/****** Object:  StoredProcedure [dbo].[SP_BASE_STOCK_DMZ]    Script Date: 09/06/2026 12:13:40 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE   PROCEDURE [dbo].[SP_BASE_STOCK_DMZ]
    @C_SUCU_EMPR INT = NULL,
    @C_FAMILIA INT = NULL,
    @INCLUIR_NO_HABILITADOS BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FECHA_FUTURA DATETIME = '2099-12-31';
    DECLARE @fecha DATE = DATEADD(DAY, -1, GETDATE());

    ----------------------------------------------------------------------
    -- CTE: Pedidos pendientes AGREGADOS por sucursal destino + artículo
    ----------------------------------------------------------------------
    WITH CTE_PEDIDOS_PENDIENTES AS (
        SELECT
            C_SUCU_DESTINO      AS C_SUCU_EMPR,
            C_ARTICULO,
            SUM(Pendientes)     AS Pendientes
        FROM repl.T080_OC_PENDIENTES
        GROUP BY
            C_SUCU_DESTINO,
            C_ARTICULO
    ),

    ----------------------------------------------------------------------
    -- CTE: Promociones activas (DISTINCT para evitar varias filas)
    ----------------------------------------------------------------------
    CTE_PROMO_VENCI AS (
        SELECT DISTINCT
            C_SUCU_EMPR,
            C_ARTICULO
        FROM [repl].[T230_FACTURADOR_NEGOCIOS_ESPECIALES_POR_CANTIDAD]
        WHERE
            @fecha BETWEEN F_DESDE AND F_HASTA
            AND Q_UNIDADES_KILOS_SALDO > 0
    ),

    ----------------------------------------------------------------------
    -- CTE: Condiciones de compra / costos
    -- Se fuerza a UNA fila por (artículo, sucursal, proveedor primario)
    ----------------------------------------------------------------------
    CTE_COSTO AS (
        SELECT
            C_ARTICULO,
            C_SUCU_EMPR,
            C_PROVEEDOR,
            Q_DIAS_STOCK,
            Q_DIAS_SOBRE_STOCK,
            I_LISTA_CALCULADO,
            ROW_NUMBER() OVER (
                PARTITION BY C_ARTICULO, C_SUCU_EMPR, C_PROVEEDOR
                ORDER BY C_ARTICULO  -- ajustar a columna de vigencia si existiera
            ) AS rn
        FROM repl.T055_ARTICULOS_CONDCOMPRA_COSTOS
    ),

    ----------------------------------------------------------------------
    -- CTE: Condiciones de entrega por proveedor/sucursal
    -- Se fuerza a UNA fila por (proveedor, sucursal)
    ----------------------------------------------------------------------
    CTE_PRV_ENT AS (
        SELECT
            C_PROVEEDOR,
            C_SUCU_EMPR,
            I_COMPRA_MINIMA,
            Q_BULTOS_KILOS_COMPRA_MINIMA,
            Q_DIAS_PREPARACION,
            ROW_NUMBER() OVER (
                PARTITION BY C_PROVEEDOR, C_SUCU_EMPR
                ORDER BY C_PROVEEDOR  -- ajustar a columna de vigencia si existiera
            ) AS rn
        FROM repl.T020_PROVEEDOR_DIAS_ENTREGA_DETA
    )

    ----------------------------------------------------------------------
    -- SELECT FINAL
    ----------------------------------------------------------------------
    SELECT DISTINCT
        STK.C_ARTICULO AS Codigo_Articulo,
        STK.C_SUCU_EMPR AS Codigo_Sucursal,
        ART.C_PROVEEDOR_PRIMARIO AS Codigo_Proveedor,

        ART_SUC.I_PRECIO_VTA AS Precio_Venta,
        ART_SUC.I_COSTO_ESTADISTICO AS Precio_Costo,
        ART_SUC.Q_FACTOR_VTA_SUCU AS Factor_Venta,
        
        ART_SUC.Q_ULT_ING_STOCK AS Ultimo_Ingreso,
        ART_SUC.F_ULT_ING_STOCK AS Fecha_Ultimo_Ingreso,
        ART_SUC.F_ULTIMA_VTA AS Fecha_Ultima_Venta,

        ART.M_VENDE_POR_PESO AS M_Vende_Por_Peso,

        (R.Q_VENTA_15_DIAS * ART_SUC.Q_FACTOR_VTA_SUCU) AS Venta_Unidades_1Q,
        (R.Q_VENTA_30_DIAS * ART_SUC.Q_FACTOR_VTA_SUCU) AS Venta_Unidades_2Q,
        ((R.Q_VENTA_30_DIAS + R.Q_VENTA_15_DIAS) * ART_SUC.Q_FACTOR_VTA_SUCU) AS Venta_Mes_Unidades,
        ((R.Q_VENTA_30_DIAS + R.Q_VENTA_15_DIAS) * ART_SUC.Q_FACTOR_VTA_SUCU * ART_SUC.I_COSTO_ESTADISTICO) AS Venta_Mes_Valorizada,

        CASE 
            WHEN (ISNULL(R.Q_VENTA_30_DIAS, 0) + ISNULL(R.Q_VENTA_15_DIAS, 0))
                 * ISNULL(ART_SUC.Q_FACTOR_VTA_SUCU, 0)
                 * ISNULL(ART_SUC.I_COSTO_ESTADISTICO, 0) = 0
            THEN NULL
            ELSE 
                ROUND(
                    ((ISNULL(STK.Q_UNID_ARTICULO, 0) + ISNULL(STK.Q_PESO_ARTICULO, 0))
                     * ISNULL(ART_SUC.I_COSTO_ESTADISTICO, 0)) /
                    NULLIF(
                        (ISNULL(R.Q_VENTA_30_DIAS, 0) + ISNULL(R.Q_VENTA_15_DIAS, 0))
                        * ISNULL(ART_SUC.Q_FACTOR_VTA_SUCU, 0)
                        * ISNULL(ART_SUC.I_COSTO_ESTADISTICO, 0),
                        0
                    ), 
                    0
                ) * 30
        END AS Dias_Stock,

        @fecha AS Fecha_Stock,
        CASE 
            WHEN ART.M_VENDE_POR_PESO = 'N' THEN DBO.NORMALIZA_STRING(STK.Q_UNID_ARTICULO) 
            ELSE DBO.NORMALIZA_STRING(STK.Q_PESO_ARTICULO) 
        END AS Stock,

        (ART_SUC.Q_TRANSF_PEND * 1) AS Transfer_Pendiente,
        ISNULL(DBO.NORMALIZA_STRING(PP.Pendientes), 0) AS Pedido_Pendiente,

        CASE WHEN ART_SUC.M_OFERTA_SUCU = 'S' THEN 1 ELSE 0 END AS Promocion,
        '' AS Lote,
        @FECHA_FUTURA AS Validez_Lote,
        0 AS Stock_Reserva,
        CASE WHEN PROMO_VENCI.C_ARTICULO IS NOT NULL THEN 1 ELSE 0 END AS Validez_Promocion,

        COSTO.Q_DIAS_STOCK,
        COSTO.Q_DIAS_SOBRE_STOCK,
        COSTO.I_LISTA_CALCULADO,
        (R.Q_REPONER_INCLUIDO_SOBRE_STOCK * ART_SUC.Q_FACTOR_VTA_SUCU) AS Pedido_SGM,

        PRV_ENT.I_COMPRA_MINIMA AS Importe_Minimo,
        PRV_ENT.Q_BULTOS_KILOS_COMPRA_MINIMA AS Bultos_Minimo,
        PRV_ENT.Q_DIAS_PREPARACION AS Dias_Preparacion

    FROM repl.T060_STOCK STK
    INNER JOIN repl.T050_ARTICULOS ART 
        ON ART.C_ARTICULO = STK.C_ARTICULO
    INNER JOIN repl.T100_EMPRESA_SUC SUC 
        ON STK.C_SUCU_EMPR = SUC.C_SUCU_EMPR

    LEFT JOIN repl.T051_ARTICULOS_SUCURSAL ART_SUC 
        ON ART_SUC.C_ARTICULO = STK.C_ARTICULO
       AND ART_SUC.C_SUCU_EMPR = STK.C_SUCU_EMPR
       AND (
            @INCLUIR_NO_HABILITADOS = 1
            OR (ART.C_FAMILIA <> 4 AND ART_SUC.M_HABILITADO_SUCU = 'S')
       )

    LEFT JOIN CTE_COSTO COSTO
        ON COSTO.C_ARTICULO = STK.C_ARTICULO
       AND COSTO.C_SUCU_EMPR = STK.C_SUCU_EMPR
       AND COSTO.C_PROVEEDOR = ART.C_PROVEEDOR_PRIMARIO
       AND COSTO.rn = 1

    LEFT JOIN repl.T710_ESTADIS_REPOSICION R 
        ON R.C_ARTICULO = STK.C_ARTICULO
       AND R.C_SUCU_EMPR = STK.C_SUCU_EMPR

    LEFT JOIN CTE_PEDIDOS_PENDIENTES PP 
        ON PP.C_ARTICULO = STK.C_ARTICULO
       AND PP.C_SUCU_EMPR = STK.C_SUCU_EMPR  -- stock en sucursal destino

    LEFT JOIN CTE_PROMO_VENCI PROMO_VENCI 
        ON PROMO_VENCI.C_SUCU_EMPR = STK.C_SUCU_EMPR
       AND PROMO_VENCI.C_ARTICULO = STK.C_ARTICULO

    LEFT JOIN CTE_PRV_ENT PRV_ENT 
        ON ART.C_PROVEEDOR_PRIMARIO = PRV_ENT.C_PROVEEDOR
       AND STK.C_SUCU_EMPR = PRV_ENT.C_SUCU_EMPR
       AND PRV_ENT.rn = 1

    WHERE 
        SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT [C_SUCU_EMPR] FROM repl.SUCURSALES_EXCLUIDAS
        )
        AND (@C_SUCU_EMPR IS NULL OR STK.C_SUCU_EMPR = @C_SUCU_EMPR)
        AND (@C_FAMILIA IS NULL OR ART.C_FAMILIA = @C_FAMILIA);
END
GO


