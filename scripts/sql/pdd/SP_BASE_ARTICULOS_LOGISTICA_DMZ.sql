USE [data-sync]
GO

/*
    Snapshot contractual para la carga SCD2 de
    diarco_data.src.base_articulos_logistica.

    Alcance v1:
    - una configuración DEFAULT por artículo vigente;
    - presentación y palletización provenientes de repl.T052;
    - Q_PESO_UNIT_ART se conserva sólo como candidato en el JSON de linaje;
    - C_EAN se publica sólo con 13 dígitos y C_DUN14 sólo con 14 dígitos;
    - no se incorporan EAN alternativos de T085_ARTICULOS_EAN_EDI;
    - peso, dimensiones y manipulación permanecen NULL/MISSING hasta confirmar
      su semántica o incorporar fuentes Valkimia/GS1 aprobadas.

    Este procedimiento no crea ni actualiza la tabla PostgreSQL final.
*/
CREATE OR ALTER PROCEDURE [dbo].[SP_BASE_ARTICULOS_LOGISTICA_DMZ]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SELECT
        c_articulo = art.C_ARTICULO,
        c_proveedor = CASE WHEN art.C_PROVEEDOR_PRIMARIO > 0
                           THEN art.C_PROVEEDOR_PRIMARIO ELSE NULL END,
        c_configuracion_logistica = CAST('DEFAULT' AS varchar(60)),
        m_configuracion_default = CAST(1 AS bit),
        m_activo = CAST(1 AS bit),

        -- Regla provisional explícita: los pesables usan KG como unidad base;
        -- el resto usa UNIT hasta incorporar una UOM maestra confirmada.
        c_unidad_base = CAST(
            CASE WHEN art.M_VENDE_POR_PESO = 'S' THEN 'KG' ELSE 'UNIT' END
            AS varchar(20)
        ),
        m_vende_por_peso = CAST(
            CASE WHEN art.M_VENDE_POR_PESO = 'S' THEN 1 ELSE 0 END AS bit
        ),
        c_gtin_unidad = CAST(gtin.c_ean_valido AS varchar(14)),

        c_tipo_bulto = CAST(
            CASE WHEN prov.Q_FACTOR_PROVEEDOR > 0 THEN 'CASE' ELSE NULL END
            AS varchar(30)
        ),
        c_gtin_bulto = CAST(gtin.c_dun14_valido AS varchar(14)),
        q_unidades_por_bulto = CASE WHEN prov.Q_FACTOR_PROVEEDOR > 0
                                    THEN prov.Q_FACTOR_PROVEEDOR ELSE NULL END,

        -- No mapear Q_PESO_UNIT_ART hasta confirmar si es neto/bruto y cuál
        -- es su unidad física para artículos pesables y no pesables.
        q_peso_neto_unitario_kg = CAST(NULL AS decimal(18,6)),
        q_peso_bruto_unitario_kg = CAST(NULL AS decimal(18,6)),
        q_peso_bruto_bulto_kg = CAST(NULL AS decimal(18,6)),

        q_largo_bulto_cm = CAST(NULL AS decimal(12,3)),
        q_ancho_bulto_cm = CAST(NULL AS decimal(12,3)),
        q_alto_bulto_cm = CAST(NULL AS decimal(12,3)),
        q_volumen_bulto_m3 = CAST(NULL AS decimal(18,9)),
        c_metodo_volumen = CAST(NULL AS varchar(30)),

        -- El SP histórico ya interpreta estos campos como cajas por piso y
        -- pisos del pallet para artículos no pesables.
        q_bultos_por_capa =
            CASE WHEN art.M_VENDE_POR_PESO <> 'S' AND prov.U_PISO_PALETIZADO > 0
                 THEN prov.U_PISO_PALETIZADO ELSE NULL END,
        q_capas_por_pallet =
            CASE WHEN art.M_VENDE_POR_PESO <> 'S' AND prov.U_ALTURA_PALETIZADO > 0
                 THEN prov.U_ALTURA_PALETIZADO ELSE NULL END,
        q_bultos_por_pallet =
            CASE
                WHEN art.M_VENDE_POR_PESO <> 'S'
                 AND prov.U_PISO_PALETIZADO > 0
                 AND prov.U_ALTURA_PALETIZADO > 0
                THEN prov.U_PISO_PALETIZADO * prov.U_ALTURA_PALETIZADO
                ELSE NULL
            END,
        c_tipo_pallet = CAST(NULL AS varchar(30)),
        q_largo_pallet_cm = CAST(NULL AS decimal(12,3)),
        q_ancho_pallet_cm = CAST(NULL AS decimal(12,3)),
        q_alto_pallet_cargado_cm = CAST(NULL AS decimal(12,3)),
        q_peso_bruto_pallet_kg = CAST(NULL AS decimal(18,6)),

        m_apilable = CAST(NULL AS bit),
        q_max_niveles_apilado = CAST(NULL AS smallint),
        m_fragil = CAST(NULL AS bit),
        m_peligroso = CAST(NULL AS bit),
        c_zona_temperatura = CAST(NULL AS varchar(20)),
        q_temperatura_min_c = CAST(NULL AS decimal(6,2)),
        q_temperatura_max_c = CAST(NULL AS decimal(6,2)),
        c_orientacion = CAST(NULL AS varchar(20)),
        observaciones_manipulacion = CAST(NULL AS nvarchar(max)),

        c_calidad_embalaje = CAST(
            CASE WHEN prov.Q_FACTOR_PROVEEDOR > 0 THEN 'SOURCE' ELSE 'MISSING' END
            AS varchar(15)
        ),
        c_calidad_peso = CAST('MISSING' AS varchar(15)),
        c_calidad_volumen = CAST('MISSING' AS varchar(15)),
        c_calidad_pallet = CAST(
            CASE
                WHEN art.M_VENDE_POR_PESO <> 'S'
                 AND prov.U_PISO_PALETIZADO > 0
                 AND prov.U_ALTURA_PALETIZADO > 0
                THEN 'SOURCE' ELSE 'MISSING'
            END AS varchar(15)
        ),
        observaciones_calidad = CAST(
            'Q_PESO_UNIT_ART pendiente de validacion funcional; no se publica como peso canonico.'
            AS nvarchar(max)
        ),
        verificado_en = CAST(NULL AS datetimeoffset),
        verificado_por = CAST(NULL AS varchar(100)),

        fuente_origen = CAST('DIARCO_DMZ' AS varchar(60)),
        referencia_origen = CAST(
            'repl.T050_ARTICULOS + repl.T052_ARTICULOS_PROVEEDOR'
            AS varchar(160)
        ),
        atributos_adicionales = CAST(
            N'{"mapping_version":"DIARCO_DMZ_V3","gtin_rule":"T050_EAN13_DUN14_MOD10_NO_PLACEHOLDER","weight_semantics":"PENDING","q_peso_unit_art_candidate":'
            + COALESCE(CONVERT(varchar(30), art.Q_PESO_UNIT_ART), 'null')
            + N'}'
            AS nvarchar(max)
        ),
        fecha_extraccion = SYSDATETIMEOFFSET(),
        cdc_lsn = CAST(NULL AS varbinary(10)),
        estado_sincronizacion = CAST(1 AS smallint)
    FROM repl.T050_ARTICULOS AS art
    LEFT JOIN repl.T052_ARTICULOS_PROVEEDOR AS prov
        ON prov.C_ARTICULO = art.C_ARTICULO
       AND prov.C_PROVEEDOR = art.C_PROVEEDOR_PRIMARIO
    OUTER APPLY (
        SELECT
            c_ean = NULLIF(LTRIM(RTRIM(CONVERT(varchar(50), art.C_EAN))), ''),
            c_dun14 = NULLIF(LTRIM(RTRIM(CONVERT(varchar(50), art.C_DUN14))), '')
    ) AS codes
    OUTER APPLY (
        SELECT
            c_ean_valido = CASE
                WHEN LEN(codes.c_ean) = 13
                 AND codes.c_ean NOT LIKE '%[^0-9]%'
                 AND REPLACE(LEFT(codes.c_ean, 12), LEFT(codes.c_ean, 1), '') <> ''
                 AND TRY_CONVERT(int, RIGHT(codes.c_ean, 1)) =
                     (10 - (
                         TRY_CONVERT(int, SUBSTRING(codes.c_ean, 1, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_ean, 2, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_ean, 3, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_ean, 4, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_ean, 5, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_ean, 6, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_ean, 7, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_ean, 8, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_ean, 9, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_ean, 10, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_ean, 11, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_ean, 12, 1))
                     ) % 10) % 10
                THEN codes.c_ean
                ELSE NULL
            END,
            c_dun14_valido = CASE
                WHEN LEN(codes.c_dun14) = 14
                 AND codes.c_dun14 NOT LIKE '%[^0-9]%'
                 AND REPLACE(LEFT(codes.c_dun14, 13), LEFT(codes.c_dun14, 1), '') <> ''
                 AND TRY_CONVERT(int, RIGHT(codes.c_dun14, 1)) =
                     (10 - (
                         3 * TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 1, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 2, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 3, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 4, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 5, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 6, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 7, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 8, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 9, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 10, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 11, 1))
                         + TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 12, 1))
                         + 3 * TRY_CONVERT(int, SUBSTRING(codes.c_dun14, 13, 1))
                     ) % 10) % 10
                THEN codes.c_dun14
                ELSE NULL
            END
    ) AS gtin
    WHERE art.M_BAJA = 'N'
      AND EXISTS (
          SELECT 1
          FROM repl.T060_STOCK AS stock
          WHERE stock.C_ARTICULO = art.C_ARTICULO
      );
END
GO
