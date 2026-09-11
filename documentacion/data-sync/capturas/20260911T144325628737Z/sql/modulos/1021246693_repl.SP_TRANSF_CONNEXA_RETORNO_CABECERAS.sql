-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO



CREATE     PROCEDURE [repl].[SP_TRANSF_CONNEXA_RETORNO_CABECERAS]
    @SoloCerrables BIT = 0,                      -- 1 = solo cabeceras sin pendientes/en_proceso (SGM ni VK)
    @Desde DATETIME = NULL,                      -- opcional: desde cuándo mirar
    @IncluirEstadosOK VARCHAR(200) = 'PROCESADO'  -- extensible: 'PROCESADO,DUPLICADO'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OK VARCHAR(220) = ',' + REPLACE(@IncluirEstadosOK, ' ', '') + ',';

    ;WITH base AS (
        SELECT
            t.connexa_header_uuid,
            t.estado,
            ISNULL(t.estado_vk, 'PENDIENTE') AS estado_vk_norm,
            t.mensaje_error,
            t.mensaje_error_vk,
            t.f_procesado,
            t.f_procesado_vk,
            CASE 
                WHEN t.f_procesado_vk IS NULL THEN t.f_procesado
                WHEN t.f_procesado IS NULL THEN t.f_procesado_vk
                WHEN t.f_procesado_vk > t.f_procesado THEN t.f_procesado_vk
                ELSE t.f_procesado
            END AS last_row_ts
        FROM repl.TRANSF_CONNEXA_IN t
        WHERE t.connexa_header_uuid IS NOT NULL
          AND (
                @Desde IS NULL
                OR (t.f_procesado    >= @Desde)
                OR (t.f_procesado_vk >= @Desde)
              )
    ),
    agg AS (
        SELECT
            b.connexa_header_uuid,

            -- Totales
            COUNT(*) AS cnt_total,

            -- SGM
            SUM(CASE WHEN ISNULL(b.estado,'PENDIENTE') IN ('PENDIENTE','EN_PROCESO') THEN 1 ELSE 0 END) AS cnt_abiertas_sgm,
            SUM(CASE WHEN b.estado = 'ERROR' THEN 1 ELSE 0 END) AS cnt_error_sgm,
            SUM(CASE WHEN CHARINDEX(',' + ISNULL(b.estado,'') + ',', @OK) > 0 THEN 1 ELSE 0 END) AS cnt_ok_sgm,

            -- VK
            SUM(CASE WHEN b.estado_vk_norm IN ('PENDIENTE','EN_PROCESO') THEN 1 ELSE 0 END) AS cnt_abiertas_vk,
            SUM(CASE WHEN b.estado_vk_norm = 'ERROR' THEN 1 ELSE 0 END) AS cnt_error_vk,
            SUM(CASE WHEN CHARINDEX(',' + b.estado_vk_norm + ',', @OK) > 0 THEN 1 ELSE 0 END) AS cnt_ok_vk,

            -- Última marca de procesamiento (SGM o VK)
            MAX(b.last_row_ts) AS last_processed_at
        FROM base b
        GROUP BY b.connexa_header_uuid
    ),
    last_error AS (
        SELECT TOP (1) WITH TIES
            b.connexa_header_uuid,
            CASE 
                WHEN b.estado = 'ERROR' THEN LEFT(ISNULL(b.mensaje_error,''),255)
                WHEN b.estado_vk_norm = 'ERROR' THEN LEFT(ISNULL(b.mensaje_error_vk,''),255)
                ELSE ''
            END AS last_error_message,
            b.last_row_ts
        FROM base b
        WHERE (b.estado = 'ERROR' OR b.estado_vk_norm = 'ERROR')
        ORDER BY ROW_NUMBER() OVER (PARTITION BY b.connexa_header_uuid ORDER BY b.last_row_ts DESC)
    )
    SELECT
        a.connexa_header_uuid,
        a.cnt_total,

        a.cnt_ok_sgm,
        a.cnt_error_sgm,
        a.cnt_abiertas_sgm,

        a.cnt_ok_vk,
        a.cnt_error_vk,
        a.cnt_abiertas_vk,

        a.last_processed_at,

        CASE 
            WHEN (a.cnt_error_sgm + a.cnt_error_vk) > 0 THEN 'ERROR'
            WHEN (a.cnt_abiertas_sgm + a.cnt_abiertas_vk) > 0 THEN 'PENDIENTE'
            ELSE 'OK'
        END AS resultado,

        ISNULL(e.last_error_message, '') AS mensaje_error
    FROM agg a
    LEFT JOIN last_error e
      ON e.connexa_header_uuid = a.connexa_header_uuid
    WHERE (
        @SoloCerrables = 0
        OR ((a.cnt_abiertas_sgm + a.cnt_abiertas_vk) = 0)
    )
    ORDER BY a.last_processed_at DESC;
END

GO
