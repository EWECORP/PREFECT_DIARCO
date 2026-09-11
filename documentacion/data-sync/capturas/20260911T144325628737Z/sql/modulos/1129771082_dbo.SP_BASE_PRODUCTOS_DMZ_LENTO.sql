-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO



/* ============================================================
   SP_BASE_PRODUCTOS_DMZ  (v2)
   Mejoras:
     - FECHA_BAJA más consistente:
         * Si hay evento "listo para venta = N" => esa fecha
         * Si NO hay historial y/o no hay evento N => NULL (luego 2099-12-31)
         * Si se deshabilita sucursal-artículo (M_HABILITADO_SUCU='N') =>
              usar F_BAJA_SIST si existe (si no existe, no “baja” por defecto)
       (evita BAJA = ALTA por deshabilitación sin fecha real)
     - No excluir por falta de historial:
         * CTE_Vigencia siempre produce fila por (C_SUCU_EMPR, C_ARTICULO) de T051
         * Nunca depende de que exista registro en hist
   ============================================================ */

CREATE       PROCEDURE [dbo].[SP_BASE_PRODUCTOS_DMZ]
    @C_SUCU_EMPR INT = NULL,
    @C_FAMILIA INT = NULL,
    @INCLUIR_NO_HABILITADOS BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FECHA_FUTURA DATETIME = '2099-12-31';

    /* ------------------------------------------------------------
       CTE 1: Universo (artículos presentes en stock)
       ------------------------------------------------------------ */
    WITH CTE_Surtido AS (
        SELECT DISTINCT C_ARTICULO
        FROM repl.T060_STOCK
    ),

    /* ------------------------------------------------------------
       CTE 2: Habilitación "barrio" (para >300 según lógica actual)
       ------------------------------------------------------------ */
    CTE_MarcaBarrio AS (
        SELECT C_SUCU_EMPR, C_ARTICULO, M_HABILITADO_SUCU
        FROM  [repl].[T051_ARTICULOS_SUCURSAL_BARRIO]
        WHERE C_ARTICULO IN (SELECT C_ARTICULO FROM CTE_Surtido)
    ),

    /* ------------------------------------------------------------
       CTE 3: Vigencia por sucursal-artículo (robusta sin hist)
       Notas:
         - Se computa ALTA como el mayor de:
             * suc.F_ALTA
             * hist alta cuando M_LISTO_PARA_VENTA_ACT='S' (si existiera)
         - BAJA como:
             * primer "N" posterior al ALTA (en caso de existir)
             * o bien F_BAJA_SIST / F_BAJA si existieran (si su tabla lo tiene)
           Si no hay nada => NULL (se convertirá a 2099-12-31 en SELECT final)
       ------------------------------------------------------------ */
    CTE_Vigencia AS (
        SELECT
            suc.C_SUCU_EMPR,
            suc.C_ARTICULO,

            -- Alta: prioriza marca listo-venta = S si aporta una fecha mayor
            ALTA =
                CASE
                    WHEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END) > MAX(suc.F_ALTA)
                        THEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END)
                    ELSE MAX(suc.F_ALTA)
                END,

            -- Baja: SOLO si hay evidencia temporal de baja.
            -- 1) Evento listo-venta = N (si existe) y es posterior a ALTA
            -- 2) (Opcional) Si tienen campo de baja en T051, úsese como alternativa
            BAJA =
                CASE
                    WHEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'N' THEN hist.F_ALTA_SIST END) IS NOT NULL
                         AND MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'N' THEN hist.F_ALTA_SIST END) >=
                             (CASE
                                WHEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END) > MAX(suc.F_ALTA)
                                    THEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END)
                                ELSE MAX(suc.F_ALTA)
                              END)
                        THEN MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'N' THEN hist.F_ALTA_SIST END)

                    -- Si su T051 tiene campos de baja, se pueden habilitar aquí.
                    -- Descomenten si existen en repl.T051_ARTICULOS_SUCURSAL:
                    -- WHEN MAX(suc.F_BAJA) IS NOT NULL THEN MAX(suc.F_BAJA)
                    -- WHEN MAX(suc.F_BAJA_SIST) IS NOT NULL THEN MAX(suc.F_BAJA_SIST)

                    ELSE NULL
                END
        FROM repl.T051_ARTICULOS_SUCURSAL suc
        INNER JOIN repl.T050_ARTICULOS art ON art.C_ARTICULO = suc.C_ARTICULO
        LEFT JOIN repl.T804_HIST_MARCA_LISTO_PARA_VENTA hist
            ON hist.C_SUCU_EMPR = suc.C_SUCU_EMPR
           AND hist.C_ARTICULO  = suc.C_ARTICULO
        WHERE suc.C_SUCU_EMPR <> 300
          AND art.M_BAJA = 'N'
          AND suc.C_ARTICULO IN (SELECT C_ARTICULO FROM CTE_Surtido)
        GROUP BY suc.C_SUCU_EMPR, suc.C_ARTICULO
    )

    /* ------------------------------------------------------------
       Selección final
       ------------------------------------------------------------ */
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

        -- HABILITADO (flag informativo)
        HABILITADO =
            CASE
                WHEN (
                    (suc.C_SUCU_EMPR < 300 AND suc.M_HABILITADO_SUCU = 'N')
                    OR (suc.C_SUCU_EMPR > 300 AND mb.M_HABILITADO_SUCU = 'N')
                )
                THEN '0' ELSE '1'
            END,

        FECHA_REGISTRO = v.ALTA,

        -- v2: si BAJA es NULL => fecha futura (vigente)
        FECHA_BAJA = ISNULL(v.BAJA, @FECHA_FUTURA),

        Q_PESO_UNIT_ART = CONVERT(VARCHAR, art.Q_PESO_UNIT_ART),
        M_VENDE_POR_PESO = CASE WHEN art.M_VENDE_POR_PESO = 'S' THEN '1' ELSE '0' END,

        UNID_TRANSFERENCIA = '0',
        Q_UNID_TRANSFERENCIA = '1',

        -- v2.1: evitar columna inexistente; mínimo operativo 1        
        --PEDIDO_MIN = COALESCE(NULLIF(entrega.Q_BULTOS_KILOS_COMPRA_MINIMA, 0), 1), --- minimo 1 ESTE ES UN PARAMETRO TOTAL PROVEEDOR y No por Artículo
		PEDIDO_MIN = '1',
        FRENTE_LINEAL = '1',
        CAPACID_GONDOLA = '1',
        STOCK_MINIMO = '1',

        COD_COMPRADOR = CONVERT(VARCHAR, art.C_COMPRADOR),

        PROMOCION = CASE suc.M_OFERTA_SUCU WHEN 'N' THEN '0' ELSE '1' END,

        ACTIVE_FOR_PURCHASE =
            CASE
                WHEN (
                    --- Clasificación A DAR DE BAJA
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
        FULL_CAPACITY_PALLET = CONVERT(VARCHAR, prov.U_PISO_PALETIZADO * prov.U_ALTURA_PALETIZADO),
        NUMBER_OF_LAYERS = CONVERT(VARCHAR, prov.U_ALTURA_PALETIZADO),

        NUMBER_OF_BOXES_PER_LAYER =
            CASE
                WHEN art.M_VENDE_POR_PESO = 'N' THEN CONVERT(VARCHAR, prov.U_PISO_PALETIZADO)
                ELSE CONVERT(VARCHAR, prov.Q_FACTOR_PROVEEDOR)
            END

    FROM repl.T051_ARTICULOS_SUCURSAL suc
    INNER JOIN repl.T050_ARTICULOS art
        ON art.C_ARTICULO = suc.C_ARTICULO
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
        suc.C_ARTICULO IN (SELECT C_ARTICULO FROM CTE_Surtido)
        AND suc.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [data-sync].[dbo].[SUCURSALES_EXCLUIDAS])
        AND (@C_SUCU_EMPR IS NULL OR suc.C_SUCU_EMPR = @C_SUCU_EMPR)
        AND (@C_FAMILIA IS NULL OR art.C_FAMILIA = @C_FAMILIA)
        AND (
            @INCLUIR_NO_HABILITADOS = 1 OR
            ((art.M_A_DAR_DE_BAJA <> 'S') AND (suc.M_HABILITADO_SUCU = 'S'))
        );

END

GO
