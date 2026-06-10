USE [data-sync]
GO

/****** Object:  StoredProcedure [dbo].[SP_BASE_PRODUCTOS_DMZ]    Script Date: 09/06/2026 12:07:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






/* ============================================================
   SP_BASE_PRODUCTOS_DMZ  (v3)
		Materializar el universo de artículos vigentes en una tabla temporal #Surtido.
		Materializar previamente el historial agregado por sucursal-artículo en #HistVigencia.
		Calcular vigencia una sola vez en #Vigencia.
		Eliminar el JOIN a T020_PROVEEDOR_DIAS_ENTREGA_DETA si no se usa.
		Reemplazar FORMAT.
		Quitar DISTINCT si ya no hay multiplicaciones.
		Agregar índices específicos.

        Esto GENERA en Postgres la tabla base_productos_vigentes
   ============================================================ */

CREATE   PROCEDURE [dbo].[SP_BASE_PRODUCTOS_DMZ]
    @C_SUCU_EMPR INT = NULL,
    @C_FAMILIA INT = NULL,
    @INCLUIR_NO_HABILITADOS BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FECHA_FUTURA DATETIME = '2099-12-31';

    ------------------------------------------------------------
    -- 1) Universo de artículos presentes en stock
    ------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Surtido') IS NOT NULL DROP TABLE #Surtido;

    SELECT DISTINCT
        st.C_ARTICULO
    INTO #Surtido
    FROM repl.T060_STOCK st;

    CREATE UNIQUE CLUSTERED INDEX IX_#Surtido
        ON #Surtido (C_ARTICULO);


    ------------------------------------------------------------
    -- 2) Historial preagregado
    ------------------------------------------------------------
    IF OBJECT_ID('tempdb..#HistVigencia') IS NOT NULL DROP TABLE #HistVigencia;

    SELECT
        hist.C_SUCU_EMPR,
        hist.C_ARTICULO,
        MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S'
                 THEN hist.F_ALTA_SIST END) AS FECHA_ALTA_HIST,
        MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'N'
                 THEN hist.F_ALTA_SIST END) AS FECHA_BAJA_HIST
    INTO #HistVigencia
    FROM repl.T804_HIST_MARCA_LISTO_PARA_VENTA hist
    INNER JOIN #Surtido s
        ON s.C_ARTICULO = hist.C_ARTICULO
    GROUP BY
        hist.C_SUCU_EMPR,
        hist.C_ARTICULO;

    CREATE UNIQUE CLUSTERED INDEX IX_#HistVigencia
        ON #HistVigencia (C_SUCU_EMPR, C_ARTICULO);


    ------------------------------------------------------------
    -- 3) Vigencia sucursal-artículo
    ------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Vigencia') IS NOT NULL DROP TABLE #Vigencia;

    SELECT
        suc.C_SUCU_EMPR,
        suc.C_ARTICULO,

        ALTA =
            CASE
                WHEN hv.FECHA_ALTA_HIST IS NOT NULL
                 AND hv.FECHA_ALTA_HIST > suc.F_ALTA
                    THEN hv.FECHA_ALTA_HIST
                ELSE suc.F_ALTA
            END,

        BAJA =
            CASE
                WHEN hv.FECHA_BAJA_HIST IS NOT NULL
                 AND hv.FECHA_BAJA_HIST >=
                    CASE
                        WHEN hv.FECHA_ALTA_HIST IS NOT NULL
                         AND hv.FECHA_ALTA_HIST > suc.F_ALTA
                            THEN hv.FECHA_ALTA_HIST
                        ELSE suc.F_ALTA
                    END
                    THEN hv.FECHA_BAJA_HIST
                ELSE NULL
            END
    INTO #Vigencia
    FROM repl.T051_ARTICULOS_SUCURSAL suc
    INNER JOIN #Surtido sur
        ON sur.C_ARTICULO = suc.C_ARTICULO
    INNER JOIN repl.T050_ARTICULOS art
        ON art.C_ARTICULO = suc.C_ARTICULO
       AND art.M_BAJA = 'N'
    LEFT JOIN #HistVigencia hv
        ON hv.C_SUCU_EMPR = suc.C_SUCU_EMPR
       AND hv.C_ARTICULO  = suc.C_ARTICULO
    WHERE suc.C_SUCU_EMPR <> 300
      AND (@C_SUCU_EMPR IS NULL OR suc.C_SUCU_EMPR = @C_SUCU_EMPR)
      AND (@C_FAMILIA IS NULL OR art.C_FAMILIA = @C_FAMILIA);

    CREATE UNIQUE CLUSTERED INDEX IX_#Vigencia
        ON #Vigencia (C_SUCU_EMPR, C_ARTICULO);


    ------------------------------------------------------------
    -- 4) Marca barrio filtrada
    ------------------------------------------------------------
    IF OBJECT_ID('tempdb..#MarcaBarrio') IS NOT NULL DROP TABLE #MarcaBarrio;

    SELECT
        mb.C_SUCU_EMPR,
        mb.C_ARTICULO,
        mb.M_HABILITADO_SUCU
    INTO #MarcaBarrio
    FROM repl.T051_ARTICULOS_SUCURSAL_BARRIO mb
    INNER JOIN #Surtido s
        ON s.C_ARTICULO = mb.C_ARTICULO;

    CREATE UNIQUE CLUSTERED INDEX IX_#MarcaBarrio
        ON #MarcaBarrio (C_SUCU_EMPR, C_ARTICULO);


    ------------------------------------------------------------
    -- 5) Resultado final
    ------------------------------------------------------------
    SELECT
        C_SUCU_EMPR = CONVERT(VARCHAR(10), suc.C_SUCU_EMPR),
        C_ARTICULO = CONVERT(VARCHAR(20), suc.C_ARTICULO),
        C_PROVEEDOR_PRIMARIO = CONVERT(VARCHAR(20), art.C_PROVEEDOR_PRIMARIO),
        ABASTECIMIENTO = CONVERT(VARCHAR(10), suc.C_SISTEMATICA),

        COD_CD =
            CASE
                WHEN suc.C_SISTEMATICA = 0 THEN
                    CASE WHEN suc.C_SUCU_EMPR < 300 THEN '41CD' ELSE '82CD' END
                WHEN suc.C_SISTEMATICA = 1 THEN
                    RIGHT('000' + CONVERT(VARCHAR(3), suc.C_SUCU_EMPR), 3)
                WHEN suc.C_SISTEMATICA = 2 THEN 'XDOC'
                WHEN suc.C_SISTEMATICA = 3 THEN '82CD'
                ELSE NULL
            END,

        HABILITADO =
            CASE
                WHEN (
                    (suc.C_SUCU_EMPR < 300 AND suc.M_HABILITADO_SUCU = 'N')
                    OR
                    (suc.C_SUCU_EMPR > 300 AND mb.M_HABILITADO_SUCU = 'N')
                )
                THEN '0' ELSE '1'
            END,

        FECHA_REGISTRO = v.ALTA,
        FECHA_BAJA = ISNULL(v.BAJA, @FECHA_FUTURA),

        Q_PESO_UNIT_ART = CONVERT(VARCHAR(30), art.Q_PESO_UNIT_ART),
        M_VENDE_POR_PESO = CASE WHEN art.M_VENDE_POR_PESO = 'S' THEN '1' ELSE '0' END,

        UNID_TRANSFERENCIA = '0',
        Q_UNID_TRANSFERENCIA = '1',
        PEDIDO_MIN = '1',
        FRENTE_LINEAL = '1',
        CAPACID_GONDOLA = '1',
        STOCK_MINIMO = '1',

        COD_COMPRADOR = CONVERT(VARCHAR(20), art.C_COMPRADOR),

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

        ACTIVE_FOR_SALE =
            CASE suc.M_LISTO_PARA_VENTA_SUCU WHEN 'N' THEN '0' ELSE '1' END,

        ACTIVE_ON_MIX =
            CASE WHEN art.C_FAMILIA = 4 OR art.M_A_DAR_DE_BAJA = 'S'
                 THEN '0' ELSE '1' END,

        DELIVERED_ID =
            CASE suc.C_SISTEMATICA
                WHEN 0 THEN CASE WHEN suc.C_SUCU_EMPR < 300 THEN '41CD' ELSE '82' END
                WHEN 1 THEN CONVERT(VARCHAR(20), prov.C_PROVEEDOR)
            END,

        PRODUCT_BASE_ID = '',
        OWN_PRODUCTION = '0',

        Q_FACTOR_COMPRA = CONVERT(VARCHAR(30), prov.Q_FACTOR_PROVEEDOR),
        FULL_CAPACITY_PALLET = CONVERT(VARCHAR(30), prov.U_PISO_PALETIZADO * prov.U_ALTURA_PALETIZADO),
        NUMBER_OF_LAYERS = CONVERT(VARCHAR(30), prov.U_ALTURA_PALETIZADO),

        NUMBER_OF_BOXES_PER_LAYER =
            CASE
                WHEN art.M_VENDE_POR_PESO = 'N'
                    THEN CONVERT(VARCHAR(30), prov.U_PISO_PALETIZADO)
                ELSE CONVERT(VARCHAR(30), prov.Q_FACTOR_PROVEEDOR)
            END

    FROM repl.T051_ARTICULOS_SUCURSAL suc
    INNER JOIN #Surtido sur
        ON sur.C_ARTICULO = suc.C_ARTICULO
    INNER JOIN repl.T050_ARTICULOS art
        ON art.C_ARTICULO = suc.C_ARTICULO
    LEFT JOIN #MarcaBarrio mb
        ON mb.C_ARTICULO = suc.C_ARTICULO
       AND mb.C_SUCU_EMPR = suc.C_SUCU_EMPR
    LEFT JOIN repl.T052_ARTICULOS_PROVEEDOR prov
        ON prov.C_ARTICULO = art.C_ARTICULO
       AND prov.C_PROVEEDOR = art.C_PROVEEDOR_PRIMARIO
    INNER JOIN repl.T100_EMPRESA_SUC suc_mae
        ON suc_mae.C_SUCU_EMPR = suc.C_SUCU_EMPR
       AND suc_mae.M_SUCU_VIRTUAL = 'N'
    INNER JOIN #Vigencia v
        ON v.C_SUCU_EMPR = suc.C_SUCU_EMPR
       AND v.C_ARTICULO  = suc.C_ARTICULO
    WHERE
        suc.C_SUCU_EMPR NOT IN (
            SELECT ex.C_SUCU_EMPR
            FROM [data-sync].[dbo].[SUCURSALES_EXCLUIDAS] ex
        )
        AND (@C_SUCU_EMPR IS NULL OR suc.C_SUCU_EMPR = @C_SUCU_EMPR)
        AND (@C_FAMILIA IS NULL OR art.C_FAMILIA = @C_FAMILIA)
        AND (
            @INCLUIR_NO_HABILITADOS = 1
            OR (
                art.M_A_DAR_DE_BAJA <> 'S'
                AND suc.M_HABILITADO_SUCU = 'S'
            )
        )
    OPTION (RECOMPILE);

END;
GO
