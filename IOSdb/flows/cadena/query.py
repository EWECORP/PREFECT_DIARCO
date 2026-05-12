GET_STOCK = """
WITH 

-- Sucursales válidas mayorista
SucursalesMayorista AS (
    SELECT C_SUCU_EMPR
    FROM [DIARCOP001].[DiarcoP].[dbo].[T100_EMPRESA_SUC]
    WHERE C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR
            FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA]
            WHERE C_SUCU_EMPR NOT IN (4, 83)
        )
        AND C_SUCU_EMPR NOT IN (63, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 8)
        AND C_SUCU_EMPR < 300
),

-- Sucursales válidas barrio
SucursalesBarrio AS (
    SELECT C_SUCU_EMPR
    FROM [DIARCO-BARRIO].[DiarcoBarrio].[dbo].[T100_EMPRESA_SUC]
    WHERE C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR
            FROM [DIARCO-BARRIO].[DiarcoBarrio].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA]
            WHERE C_SUCU_EMPR NOT IN (321, 324, 325, 327, 329, 331, 338, 343, 349, 350, 352, 358, 365, 366, 389, 392, 408, 410, 412)
        )
),

-- Ventas 30 días mayorista
Ventas30Mayorista AS (
    SELECT 
        C_ARTICULO,
        SUM(Q_UNIDADES_VENDIDAS * I_PRECIO_COSTO) AS ventas_30d_valorizado
    FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO]
    WHERE F_VENTA >= DATEADD(DAY, -30, GETDATE())
      AND I_PRECIO_COSTO > 1 
      AND Q_UNIDADES_VENDIDAS > 0
      AND C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesMayorista)
    GROUP BY C_ARTICULO
),

-- Ventas 30 días barrio
Ventas30Barrio AS (
    SELECT 
        C_ARTICULO,
        SUM(Q_UNIDADES_VENDIDAS * I_PRECIO_COSTO) AS ventas_30d_valorizado
    FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO_DBARRIO]
    WHERE F_VENTA >= DATEADD(DAY, -30, GETDATE())
      AND I_PRECIO_COSTO > 1 
      AND Q_UNIDADES_VENDIDAS > 0
      AND C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesBarrio)
    GROUP BY C_ARTICULO
),

-- Ventas 30 días combinadas mayorista + barrio
Ventas30 AS (
    SELECT C_ARTICULO, SUM(ventas_30d_valorizado) AS ventas_30d_valorizado
    FROM (
        SELECT C_ARTICULO, ventas_30d_valorizado FROM Ventas30Mayorista
        UNION ALL
        SELECT C_ARTICULO, ventas_30d_valorizado FROM Ventas30Barrio
    ) V
    GROUP BY C_ARTICULO
),

-- Fechas de venta mayorista
FechasVentaMayorista AS (
    SELECT
        C_ARTICULO,
        MAX(F_VENTA) AS last_sale_date,
        MIN(F_VENTA) AS first_sale_date
    FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO]
    WHERE C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesMayorista)
    GROUP BY C_ARTICULO
),

-- Fechas de venta barrio
FechasVentaBarrio AS (
    SELECT
        C_ARTICULO,
        MAX(F_VENTA) AS last_sale_date,
        MIN(F_VENTA) AS first_sale_date
    FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO_DBARRIO]
    WHERE C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesBarrio)
    GROUP BY C_ARTICULO
),

-- Fechas de venta combinadas
FechasVenta AS (
    SELECT
        C_ARTICULO,
        MAX(last_sale_date)  AS last_sale_date,
        MIN(first_sale_date) AS first_sale_date
    FROM (
        SELECT C_ARTICULO, last_sale_date, first_sale_date FROM FechasVentaMayorista
        UNION ALL
        SELECT C_ARTICULO, last_sale_date, first_sale_date FROM FechasVentaBarrio
    ) F
    GROUP BY C_ARTICULO
),

-- Restock mayorista
RestockMayorista AS (
    SELECT
        C_ARTICULO,
        MAX(F_ULT_ING_STOCK) AS last_restock_date
    FROM [DIARCOP001].[DiarcoP].[dbo].T051_ARTICULOS_SUCURSAL
    WHERE C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesMayorista)
    GROUP BY C_ARTICULO
),

-- Restock barrio
RestockBarrio AS (
    SELECT
        C_ARTICULO,
        MAX(F_ULT_ING_STOCK) AS last_restock_date
    FROM [DIARCO-BARRIO].[DiarcoBarrio].[dbo].T051_ARTICULOS_SUCURSAL
    WHERE C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesBarrio)
    GROUP BY C_ARTICULO
),

-- Restock combinado
Restock AS (
    SELECT
        C_ARTICULO,
        MAX(last_restock_date) AS last_restock_date
    FROM (
        SELECT C_ARTICULO, last_restock_date FROM RestockMayorista
        UNION ALL
        SELECT C_ARTICULO, last_restock_date FROM RestockBarrio
    ) R
    GROUP BY C_ARTICULO
),

-- Stock valorizado mayorista (cada sucursal con su propio costo)
StockValorizadoMayorista AS (
    SELECT
        T2.C_ARTICULO,
        SUM(T2.Q_UNID_ARTICULO)  AS Q_UNID_ARTICULO,
        SUM(T2.Q_PESO_ARTICULO)  AS Q_PESO_ARTICULO,
        SUM((T2.Q_UNID_ARTICULO + T2.Q_PESO_ARTICULO) * T3.I_COSTO_ESTADISTICO) AS stock_valorizado
    FROM [DIARCOP001].[DiarcoP].[dbo].T060_STOCK T2
    JOIN [DIARCOP001].[DiarcoP].[dbo].T051_ARTICULOS_SUCURSAL T3
        ON T2.C_ARTICULO = T3.C_ARTICULO AND T2.C_SUCU_EMPR = T3.C_SUCU_EMPR
    WHERE T2.C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesMayorista)
    GROUP BY T2.C_ARTICULO
),

-- Stock valorizado barrio (cada sucursal con su propio costo)
StockValorizadoBarrio AS (
    SELECT
        T2.C_ARTICULO,
        SUM(T2.Q_UNID_ARTICULO)  AS Q_UNID_ARTICULO,
        SUM(T2.Q_PESO_ARTICULO)  AS Q_PESO_ARTICULO,
        SUM((T2.Q_UNID_ARTICULO + T2.Q_PESO_ARTICULO) * T3.I_COSTO_ESTADISTICO) AS stock_valorizado
    FROM [DIARCO-BARRIO].[DiarcoBarrio].[dbo].T060_STOCK T2
    JOIN [DIARCO-BARRIO].[DiarcoBarrio].[dbo].T051_ARTICULOS_SUCURSAL T3
        ON T2.C_ARTICULO = T3.C_ARTICULO AND T2.C_SUCU_EMPR = T3.C_SUCU_EMPR
    WHERE T2.C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesBarrio)
    GROUP BY T2.C_ARTICULO
),

-- Stock total combinado
StockTotal AS (
    SELECT
        C_ARTICULO,
        SUM(Q_UNID_ARTICULO)  AS Q_UNID_ARTICULO,
        SUM(Q_PESO_ARTICULO)  AS Q_PESO_ARTICULO,
        SUM(stock_valorizado) AS stock_valorizado
    FROM (
        SELECT C_ARTICULO, Q_UNID_ARTICULO, Q_PESO_ARTICULO, stock_valorizado FROM StockValorizadoMayorista
        UNION ALL
        SELECT C_ARTICULO, Q_UNID_ARTICULO, Q_PESO_ARTICULO, stock_valorizado FROM StockValorizadoBarrio
    ) S
    GROUP BY C_ARTICULO
),

-- Pendiente valorizado mayorista (cada sucursal con su propio costo)
PendienteMayorista AS (
    SELECT
        C_ARTICULO,
        SUM(
            (ISNULL(Q_BULTOS_PENDIENTE_OC, 0) * ISNULL(Q_FACTOR_VTA_SUCU, 0) * I_COSTO_ESTADISTICO) +
            (ISNULL(Q_PESO_PENDIENTE_OC, 0) * I_COSTO_ESTADISTICO)
        ) AS pendiente_valorizado
    FROM [DIARCOP001].[DiarcoP].[dbo].T051_ARTICULOS_SUCURSAL
    WHERE C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesMayorista)
    GROUP BY C_ARTICULO
),

-- Pendiente valorizado barrio (cada sucursal con su propio costo)
PendienteBarrio AS (
    SELECT
        C_ARTICULO,
        SUM(
            (ISNULL(Q_BULTOS_PENDIENTE_OC, 0) * ISNULL(Q_FACTOR_VTA_SUCU, 0) * I_COSTO_ESTADISTICO) +
            (ISNULL(Q_PESO_PENDIENTE_OC, 0) * I_COSTO_ESTADISTICO)
        ) AS pendiente_valorizado
    FROM [DIARCO-BARRIO].[DiarcoBarrio].[dbo].T051_ARTICULOS_SUCURSAL
    WHERE C_SUCU_EMPR IN (SELECT C_SUCU_EMPR FROM SucursalesBarrio)
    GROUP BY C_ARTICULO
),

-- Pendiente total combinado
PendienteTotal AS (
    SELECT
        C_ARTICULO,
        SUM(pendiente_valorizado) AS pendiente_valorizado
    FROM (
        SELECT C_ARTICULO, pendiente_valorizado FROM PendienteMayorista
        UNION ALL
        SELECT C_ARTICULO, pendiente_valorizado FROM PendienteBarrio
    ) P
    GROUP BY C_ARTICULO
),

-- Datos de sucursal 1 (precio, costo, oferta)
Sucu1 AS (
    SELECT
        C_ARTICULO,
        I_COSTO_ESTADISTICO,
        I_PRECIO_VTA,
        M_OFERTA_SUCU
    FROM [DIARCOP001].[DiarcoP].[dbo].T051_ARTICULOS_SUCURSAL
    WHERE C_SUCU_EMPR = 1
),

-- Configuración de compra de sucursal 1
ConfiguracionCompra AS (
    SELECT C_ARTICULO, Q_DIAS_STOCK
    FROM (
        SELECT C_ARTICULO, Q_DIAS_STOCK,
               ROW_NUMBER() OVER (PARTITION BY C_ARTICULO ORDER BY F_EMISION_OC_ULT_COMP DESC) AS rn
        FROM [DIARCOP001].[DiarcoP].[dbo].T055_ARTICULOS_CONDCOMPRA_COSTOS
        WHERE C_SUCU_EMPR = 1
    ) T WHERE rn = 1
)

SELECT 
    T1.C_ARTICULO                                                   AS id,
    T1.M_VENDE_POR_PESO,

    -- Stock total de todas las sucursales
    CASE 
        WHEN T1.M_VENDE_POR_PESO = 'N' THEN ST.Q_UNID_ARTICULO 
        ELSE ST.Q_PESO_ARTICULO 
    END                                                             AS stock,

    -- Units / units_weighted
    CASE WHEN T1.M_VENDE_POR_PESO = 'N' THEN ST.Q_UNID_ARTICULO ELSE NULL END  AS units,
    CASE WHEN T1.M_VENDE_POR_PESO = 'S' THEN ST.Q_PESO_ARTICULO ELSE NULL END  AS units_weighted,

    -- Días de stock (stock de cada sucursal valorizado con su propio costo / ventas cadena)
    CASE 
        WHEN ST.stock_valorizado < 0                            THEN 999
        WHEN ISNULL(V30.ventas_30d_valorizado, 0) = 0          THEN 999
        ELSE ROUND((ST.stock_valorizado / V30.ventas_30d_valorizado) * 30, 0)
    END                                                             AS days_of_stock,

    -- Días de stock con pendiente
    CASE 
        WHEN ST.stock_valorizado < 0                            THEN 999
        WHEN ISNULL(V30.ventas_30d_valorizado, 0) = 0          THEN 999
        ELSE ROUND(
            ((ST.stock_valorizado + ISNULL(PT.pendiente_valorizado, 0)) 
             / V30.ventas_30d_valorizado) * 30, 0)
    END                                                             AS days_of_stock_pending,

    -- Días de stock recomendados de sucursal 1
    CONF.Q_DIAS_STOCK                                               AS recommended_days_of_stock,

    -- Precios de sucursal 1
    S1.I_COSTO_ESTADISTICO                                          AS price,
    T5.K_COEF_IVA                                                   AS tax,
    (S1.I_COSTO_ESTADISTICO * (1 + T5.K_COEF_IVA))                 AS price_with_tax,
    S1.I_PRECIO_VTA                                                 AS price_retail,
    (S1.I_PRECIO_VTA * (1 + T5.K_COEF_IVA))                        AS price_retail_with_tax,

    -- Fechas
    FV.last_sale_date,
    FV.first_sale_date,
    RK.last_restock_date,

    -- Proveedor
    PROV.N_PROVEEDOR                                                AS supplier_name,
    PROV.C_PROVEEDOR                                                AS supplier_code,

    -- Tags
    '[' + 
        CASE 
            WHEN T1.C_CLASIFICACION_COMPRA = 1   THEN 'SENSIBLES'
            WHEN T1.C_CLASIFICACION_COMPRA = 2   THEN 'RESTO TOP'
            WHEN T1.C_CLASIFICACION_COMPRA = 3   THEN 'VARIEDAD EXTRA'
            WHEN T1.C_CLASIFICACION_COMPRA = 4   THEN 'A REMOVER'
            WHEN T1.C_CLASIFICACION_COMPRA = 5   THEN 'SIN VENTA'
            WHEN T1.C_CLASIFICACION_COMPRA = 6   THEN 'SENSIBLES PM'
            WHEN T1.C_CLASIFICACION_COMPRA = 100 THEN 'SIN CLASIFICAR'
            ELSE 'OTRO' 
        END + 
        CASE WHEN S1.M_OFERTA_SUCU = 'S' THEN ', OFERTA' ELSE '' END + 
    ']'                                                             AS tags

FROM [DIARCOP001].[DiarcoP].[dbo].T050_ARTICULOS T1
JOIN StockTotal          ST    ON T1.C_ARTICULO = ST.C_ARTICULO
JOIN Sucu1               S1    ON T1.C_ARTICULO = S1.C_ARTICULO
JOIN [DIARCOP001].[DiarcoP].[dbo].T114_RUBROS T4   ON T1.C_FAMILIA        = T4.C_RUBRO
JOIN [DIARCOP001].[DiarcoP].[dbo].T110_IVA   T5   ON T1.C_IVA_VENTAS_CF  = T5.C_IVA
LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].T020_PROVEEDOR PROV ON T1.C_PROVEEDOR_PRIMARIO = PROV.C_PROVEEDOR
LEFT JOIN Ventas30            V30   ON T1.C_ARTICULO = V30.C_ARTICULO
LEFT JOIN FechasVenta         FV    ON T1.C_ARTICULO = FV.C_ARTICULO
LEFT JOIN Restock             RK    ON T1.C_ARTICULO = RK.C_ARTICULO
LEFT JOIN PendienteTotal      PT    ON T1.C_ARTICULO = PT.C_ARTICULO
LEFT JOIN ConfiguracionCompra CONF  ON T1.C_ARTICULO = CONF.C_ARTICULO

WHERE T1.M_BAJA = 'N'
  AND T4.M_EXCLUIDA_EN_VALORIZ = 'N'
  AND T1.C_ARTICULO NOT IN (
      SELECT C_ARTICULO 
      FROM [DIARCOP001].[DiarcoP].[dbo].T050_ARTICULOS_DIFERENCIAS_DE_PRECIOS
  )

ORDER BY T1.C_ARTICULO;
"""