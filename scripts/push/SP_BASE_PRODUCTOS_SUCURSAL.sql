--- OPTIMIZADO
/* ============================================================
   1) Materializar surtido una sola vez
   ============================================================ */
IF OBJECT_ID('tempdb..#Surtido') IS NOT NULL DROP TABLE #Surtido;

SELECT DISTINCT C_ARTICULO
INTO #Surtido
FROM repl.T060_STOCK;

CREATE UNIQUE CLUSTERED INDEX IX_Surtido_C_ARTICULO
ON #Surtido (C_ARTICULO);



/* ============================================================
   2) CTE Marca Barrio (JOIN en vez de IN)
   ============================================================ */
WITH CTE_MarcaBarrio AS (
    SELECT B.C_SUCU_EMPR, B.C_ARTICULO, B.M_HABILITADO_SUCU
    FROM repl.T051_ARTICULOS_SUCURSAL_BARRIO B
    INNER JOIN #Surtido S ON S.C_ARTICULO = B.C_ARTICULO
),

/* ============================================================
   3) CTE Vigencia (JOIN en vez de IN)
   ============================================================ */
CTE_Vigencia AS (
    SELECT
        suc.C_SUCU_EMPR,
        suc.C_ARTICULO,

        ALTA =
            CASE
                WHEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END) 
                     > MAX(suc.F_ALTA)
                THEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END)
                ELSE MAX(suc.F_ALTA)
            END,

        BAJA =
            CASE
                WHEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'N' THEN hist.F_ALTA_SIST END) IS NOT NULL
                     AND MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'N' THEN hist.F_ALTA_SIST END) >=
                         (CASE
                            WHEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END) 
                                 > MAX(suc.F_ALTA)
                            THEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END)
                            ELSE MAX(suc.F_ALTA)
                          END)
                THEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'N' THEN hist.F_ALTA_SIST END)
                ELSE NULL
            END
    FROM repl.T051_ARTICULOS_SUCURSAL suc
    INNER JOIN #Surtido S ON S.C_ARTICULO = suc.C_ARTICULO
    INNER JOIN repl.T050_ARTICULOS art ON art.C_ARTICULO = suc.C_ARTICULO
    LEFT JOIN repl.T804_HIST_MARCA_LISTO_PARA_VENTA hist
        ON hist.C_SUCU_EMPR = suc.C_SUCU_EMPR
       AND hist.C_ARTICULO  = suc.C_ARTICULO
    WHERE suc.C_SUCU_EMPR <> 300
      AND art.M_BAJA = 'N'
    GROUP BY suc.C_SUCU_EMPR, suc.C_ARTICULO
)



/* ============================================================
   4) SELECT final (JOIN en vez de IN)
   ============================================================ */
SELECT DISTINCT
    C_SUCU_EMPR = CONVERT(VARCHAR, suc.C_SUCU_EMPR),
    C_ARTICULO = CONVERT(VARCHAR, suc.C_ARTICULO),
    C_PROVEEDOR_PRIMARIO = CONVERT(VARCHAR, art.C_PROVEEDOR_PRIMARIO),
    ABASTECIMIENTO = CONVERT(VARCHAR, suc.C_SISTEMATICA),

    COD_CD =
      CASE
        WHEN suc.C_SISTEMATICA = 0 THEN
          CASE WHEN suc.C_SUCU_EMPR < 300 THEN '41CD' ELSE '82CD' END
        WHEN suc.C_SISTEMATICA = 1 THEN FORMAT(suc.C_SUCU_EMPR, '000')
        WHEN suc.C_SISTEMATICA = 2 THEN 'XDOC'
        WHEN suc.C_SISTEMATICA = 3 THEN '82CD'
        ELSE NULL
      END,

    HABILITADO =
        CASE
            WHEN (
                (suc.C_SUCU_EMPR < 300 AND suc.M_HABILITADO_SUCU = 'N')
                OR (suc.C_SUCU_EMPR > 300 AND mb.M_HABILITADO_SUCU = 'N')
            )
            THEN '0' ELSE '1'
        END,

    FECHA_REGISTRO = v.ALTA,
    FECHA_BAJA = ISNULL(v.BAJA, @FECHA_FUTURA),

    Q_PESO_UNIT_ART = CONVERT(VARCHAR, art.Q_PESO_UNIT_ART),
    M_VENDE_POR_PESO = CASE WHEN art.M_VENDE_POR_PESO = 'S' THEN '1' ELSE '0' END,

    UNID_TRANSFERENCIA = '0',
    Q_UNID_TRANSFERENCIA = '1',

    PEDIDO_MIN = '1',
    FRENTE_LINEAL = '1',
    CAPACID_GONDOLA = '1',
    STOCK_MINIMO = '1',

    COD_COMPRADOR = CONVERT(VARCHAR, art.C_COMPRADOR),

    PROMOCION = CASE suc.M_OFERTA_SUCU WHEN 'N' THEN '0' ELSE '1' END,

    ACTIVE_FOR_PURCHASE =
        CASE
            WHEN (
                art.M_A_DAR_DE_BAJA = 'S'
                OR art.C_CLASIFICACION_COMPRA = 4
                OR (suc.C_SUCU_EMPR < 300 AND suc.M_HABILITADO_SUCU = 'N')
                OR (suc.C_SUCU_EMPR > 300 AND mb.M_HABILITADO_SUCU = 'N')
            )
            THEN '0' ELSE '1'
        END,

    ACTIVE_FOR_SALE = CASE suc.M_LISTO_PARA_VENTA_SUCU WHEN 'N' THEN '0' ELSE '1' END,

    ACTIVE_ON_MIX = CASE WHEN art.C_FAMILIA = 4 OR art.M_A_DAR_DE_BAJA = 'S' THEN '0' ELSE '1' END,

    DELIVERED_ID =
        CASE suc.C_SISTEMATICA
            WHEN 0 THEN CASE WHEN suc.C_SUCU_EMPR < 300 THEN '41CD' ELSE '82' END
            WHEN 1 THEN CONVERT(VARCHAR, prov.C_PROVEEDOR)
        END,

    PRODUCT_BASE_ID = '',
    OWN_PRODUCTION = '0',

    Q_FACTOR_COMPRA = CONVERT(VARCHAR, prov.Q_FACTOR_PROVEEDOR),
    FULL_CAPACITY_PALET = CONVERT(VARCHAR, prov.U_PISO_PALETIZADO * prov.U_ALTURA_PALETIZADO),
    NUMBER_OF_LAYERS = CONVERT(VARCHAR, prov.U_ALTURA_PALETIZADO),

    NUMBER_OF_BOXES_PER_LAYER =
        CASE
            WHEN art.M_VENDE_POR_PESO = 'N' THEN CONVERT(VARCHAR, prov.U_PISO_PALETIZADO)
            ELSE CONVERT(VARCHAR, prov.Q_FACTOR_PROVEEDOR)
        END

FROM repl.T051_ARTICULOS_SUCURSAL suc
INNER JOIN #Surtido S ON S.C_ARTICULO = suc.C_ARTICULO
INNER JOIN repl.T050_ARTICULOS art ON art.C_ARTICULO = suc.C_ARTICULO
LEFT JOIN CTE_MarcaBarrio mb
    ON mb.C_ARTICULO = suc.C_ARTICULO
   AND mb.C_SUCU_EMPR = suc.C_SUCU_EMPR
LEFT JOIN repl.T052_ARTICULOS_PROVEEDOR prov
    ON prov.C_ARTICULO = art.C_ARTICULO
   AND prov.C_PROVEEDOR = art.C_PROVEEDOR_PRIMARIO
LEFT JOIN repl.T020_PROVEEDOR_DIAS_ENTREGA_DETA entrega
    ON entrega.C_PROVEEDOR = prov.C_PROVEEDOR
   AND entrega.C_SUCU_EMPR = suc.C_SUCU_EMPR
INNER JOIN repl.T100_EMPRESA_SUC SUC_MAE
    ON SUC_MAE.C_SUCU_EMPR = suc.C_SUCU_EMPR
   AND SUC_MAE.M_SUCU_VIRTUAL = 'N'
INNER JOIN CTE_Vigencia v
    ON v.C_SUCU_EMPR = suc.C_SUCU_EMPR
   AND v.C_ARTICULO  = suc.C_ARTICULO
WHERE
    suc.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [data-sync].[dbo].[SUCURSALES_EXCLUIDAS])
    AND (@C_SUCU_EMPR IS NULL OR suc.C_SUCU_EMPR = @C_SUCU_EMPR)
    AND (@C_FAMILIA IS NULL OR art.C_FAMILIA = @C_FAMILIA)
    AND (
        @INCLUIR_NO_HABILITADOS = 1 OR
        ((art.M_A_DAR_DE_BAJA <> 'S') AND (suc.M_HABILITADO_SUCU = 'S'))
    );
GO