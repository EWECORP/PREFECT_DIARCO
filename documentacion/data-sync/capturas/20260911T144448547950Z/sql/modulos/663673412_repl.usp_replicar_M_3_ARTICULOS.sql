-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO





CREATE          PROCEDURE [repl].[usp_replicar_M_3_ARTICULOS]
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Validación de existencia de tabla destino
        IF OBJECT_ID('repl.M_3_ARTICULOS') IS NULL
        BEGIN
            RAISERROR('Tabla destino repl.M_3_ARTICULOS no existe.', 16, 1);
            RETURN;
        END

        DECLARE @F_EJECUCION DATETIME = GETDATE();

        -- Eliminar si existe tabla temporal
        IF OBJECT_ID('tempdb..#TEMP_CONNEXA_DATA') IS NOT NULL
            DROP TABLE #TEMP_CONNEXA_DATA;

        -- Crear tabla temporal de jerarquía categórica
        CREATE TABLE #TEMP_CONNEXA_DATA (
            C_ARTICULO      NUMERIC(6, 0),
            C_FAMILIA       NUMERIC(5, 0),
            C_RUBRO         NUMERIC(5, 0),
            C_SUBRUBRO_1    NUMERIC(5, 0),
            C_SUBRUBRO_2    NUMERIC(5, 0),
            C_SUBRUBRO_3    NUMERIC(5, 0),
            C_SUBRUBRO_4    NUMERIC(5, 0),
            N_FAMILIA       CHAR(100),
            N_RUBRO         CHAR(100),
            N_SUBRUBRO_1    CHAR(100),
            N_SUBRUBRO_2    CHAR(100),
            N_SUBRUBRO_3    CHAR(100),
            N_SUBRUBRO_4    CHAR(100)
        );

        -- Insertar jerarquía base
        INSERT INTO #TEMP_CONNEXA_DATA (
            C_ARTICULO, C_FAMILIA, C_RUBRO, C_SUBRUBRO_1, C_SUBRUBRO_2, C_SUBRUBRO_3, C_SUBRUBRO_4,
            N_FAMILIA, N_RUBRO, N_SUBRUBRO_1, N_SUBRUBRO_2, N_SUBRUBRO_3, N_SUBRUBRO_4
        )
        SELECT
            C_ARTICULO,
            CASE WHEN C_FAMILIA = 0 THEN 999 ELSE C_FAMILIA END,
            CASE WHEN C_RUBRO = 0 THEN 999 ELSE C_RUBRO END,
            CASE WHEN C_SUBRUBRO_1 = 0 THEN 999 ELSE C_SUBRUBRO_1 END,
            CASE WHEN C_SUBRUBRO_2 = 0 THEN 999 ELSE C_SUBRUBRO_2 END,
            CASE WHEN C_SUBRUBRO_3 = 0 THEN 999 ELSE C_SUBRUBRO_3 END,
            CASE WHEN C_SUBRUBRO_4 = 0 THEN 999 ELSE C_SUBRUBRO_4 END,
            'SIN CLASIFICAR', 'SIN CLASIFICAR', 'SIN CLASIFICAR', 'SIN CLASIFICAR', 'SIN CLASIFICAR', 'SIN CLASIFICAR'
        FROM [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS WITH (NOLOCK)
        WHERE C_ARTICULO NOT IN (
            SELECT C_ARTICULO 
            FROM [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS_DIFERENCIAS_DE_PRECIOS WITH (NOLOCK)
        );

        -- Enriquecimiento de jerarquía categórica (1 solo paso unificado)
        UPDATE T
        SET 
            T.N_FAMILIA      = ISNULL(F.D_RUBRO, 'SIN CLASIFICAR'),
            T.N_RUBRO        = ISNULL(R.D_RUBRO, 'SIN CLASIFICAR'),
            T.N_SUBRUBRO_1   = ISNULL(SR1.D_RUBRO, 'SIN CLASIFICAR'),
            T.N_SUBRUBRO_2   = ISNULL(SR2.D_RUBRO, 'SIN CLASIFICAR'),
            T.N_SUBRUBRO_3   = ISNULL(SR3.D_RUBRO, 'SIN CLASIFICAR'),
            T.N_SUBRUBRO_4   = ISNULL(SR4.D_RUBRO, 'SIN CLASIFICAR')
        FROM #TEMP_CONNEXA_DATA T
        LEFT JOIN [DIARCOP001].[DiarcoP].dbo.T114_RUBROS F   WITH (NOLOCK) ON T.C_FAMILIA = F.C_RUBRO     AND F.C_RUBRO_PADRE = 0 AND F.C_RUBRO_NIVEL = 1
        LEFT JOIN [DIARCOP001].[DiarcoP].dbo.T114_RUBROS R   WITH (NOLOCK) ON T.C_RUBRO = R.C_RUBRO
        LEFT JOIN [DIARCOP001].[DiarcoP].dbo.T114_RUBROS SR1 WITH (NOLOCK) ON T.C_SUBRUBRO_1 = SR1.C_RUBRO
        LEFT JOIN [DIARCOP001].[DiarcoP].dbo.T114_RUBROS SR2 WITH (NOLOCK) ON T.C_SUBRUBRO_2 = SR2.C_RUBRO
        LEFT JOIN [DIARCOP001].[DiarcoP].dbo.T114_RUBROS SR3 WITH (NOLOCK) ON T.C_SUBRUBRO_3 = SR3.C_RUBRO
        LEFT JOIN [DIARCOP001].[DiarcoP].dbo.T114_RUBROS SR4 WITH (NOLOCK) ON T.C_SUBRUBRO_4 = SR4.C_RUBRO;

        -- Truncado tabla destino
        TRUNCATE TABLE repl.M_3_ARTICULOS;

        -- Carga final
        INSERT INTO repl.M_3_ARTICULOS (
            C_ARTICULO, N_ARTICULO, C_RUBRO, C_SUBRUBRO_1, C_SUBRUBRO_2, C_SUBRUBRO_3,
            D_CODIGO_ABREV_VTA, Q_FACTOR_VTA_SUCU, D_CODIGO_ABREV_CPRA, Q_FACTOR_CPRA_SUCU,
            CLASIFICACION, PLAZO_VALIDEZ, PLAZO_ACEPTACION, PLAZO_RETIRO_GONDOLA,
            C_PROVEEDOR_PRIMARIO,
            EAN, EAN_ALTERNATIVO_1, EAN_ALTERNATIVO_2, EAN_ALTERNATIVO_3, EAN_ALTERNATIVO_4, DUN14,
            ARTICULO_BASE, PROP_BAJA_BASE,
            COD_ERP_PROD_COMPRA, PRECIO_COMPRA, OTRA_COLUMNA
        )
        SELECT 
            art.C_ARTICULO, DBO.NORMALIZA_STRING(art.N_ARTICULO),
            art.C_RUBRO, art.C_SUBRUBRO_1, art.C_SUBRUBRO_2,  est.C_SUBRUBRO_3,
            DBO.NORMALIZA_STRING(cod.D_CODIGO_ABREV),
            AVG(suc.Q_FACTOR_VTA_SUCU),
            DBO.NORMALIZA_STRING(cod.D_CODIGO_ABREV),
            CASE WHEN art.M_VENDE_POR_PESO = 'N' THEN AVG(prov.Q_FACTOR_PROVEEDOR) ELSE AVG(art.Q_PESO_UNIT_ART) END,
            1, '', '', '',
            art.C_PROVEEDOR_PRIMARIO,
            LTRIM(RTRIM(art.C_EAN)), LTRIM(RTRIM(art.C_EAN_ALTERNATIVO_1)), LTRIM(RTRIM(art.C_EAN_ALTERNATIVO_2)), 
            LTRIM(RTRIM(art.C_EAN_ALTERNATIVO_3)), LTRIM(RTRIM(art.C_EAN_ALTERNATIVO_4)), LTRIM(RTRIM(art.C_DUN14)),
            '', '', 
            art.C_ARTICULO,
            AVG(cost.I_LISTA_CALCULADO),
            ''
        FROM [DIARCOP001].[DiarcoP].dbo.t050_articulos art WITH (NOLOCK)
        INNER JOIN [DIARCOP001].[DiarcoP].dbo.T051_ARTICULOS_SUCURSAL suc WITH (NOLOCK) 
            ON suc.C_ARTICULO = art.C_ARTICULO
        INNER JOIN [DIARCOP001].[DiarcoP].dbo.T052_ARTICULOS_PROVEEDOR prov WITH (NOLOCK) 
            ON art.C_PROVEEDOR_PRIMARIO = prov.C_PROVEEDOR 
            AND art.C_ARTICULO = prov.C_ARTICULO
        INNER JOIN [DIARCOP001].[DiarcoP].dbo.T055_ARTICULOS_CONDCOMPRA_COSTOS cost WITH (NOLOCK) 
            ON art.C_ARTICULO = cost.C_ARTICULO 
            AND cost.C_PROVEEDOR = art.C_PROVEEDOR_PRIMARIO 
            AND cost.C_SUCU_EMPR = suc.C_SUCU_EMPR
        INNER JOIN [DIARCOP001].[DiarcoP].dbo.T001_TABLA_CODIGO cod WITH (NOLOCK) 
            ON art.C_UNIDAD_MEDIDA = cod.C_CODIGO_TABLA AND cod.C_TABLA = 79
        INNER JOIN #TEMP_CONNEXA_DATA est WITH (NOLOCK) 
            ON est.C_ARTICULO = art.C_ARTICULO 
               AND est.C_RUBRO = art.C_RUBRO 
           -- AND est.C_SUBRUBRO_1 = art.C_SUBRUBRO_1 
           -- AND est.C_SUBRUBRO_2 = art.C_SUBRUBRO_2
        GROUP BY
            art.C_ARTICULO, art.N_ARTICULO, art.C_RUBRO, art.C_SUBRUBRO_1, 
			art.C_SUBRUBRO_2, est.C_SUBRUBRO_3, cod.D_CODIGO_ABREV, art.C_PROVEEDOR_PRIMARIO,
            art.C_EAN, art.C_EAN_ALTERNATIVO_1, art.C_EAN_ALTERNATIVO_2, art.C_EAN_ALTERNATIVO_3,
            art.C_EAN_ALTERNATIVO_4, art.C_DUN14, art.M_VENDE_POR_PESO;

        -- Marcar fecha de actualización
        UPDATE repl.M_3_ARTICULOS
        SET 
            F_DATO = @F_EJECUCION,
            F_PROC = @F_EJECUCION,
			FUENTE_ORIGEN='repl.usp_replicar_M_3_ARTICULOS',
			FECHA_EXTRACCION = @F_EJECUCION,
			ESTADO_SINCRONIZACION = 0;

        DROP TABLE #TEMP_CONNEXA_DATA;

    END TRY
    BEGIN CATCH
        DECLARE @ERROR_MESSAGE NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR('Error en [repl].[usp_replicar_M_3_ARTICULOS]: %s', 16, 1, @ERROR_MESSAGE);
    END CATCH
END;

GO
