-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [repl].[SP_TRANSF_CONNEXA_OBTENER_RETORNO_ESTADOS]
    @Desde DATETIME2(0) = NULL,     -- opcional: filtrar por fecha de procesado
    @SoloCerrables BIT = 1          -- 1 = solo cabeceras sin pendientes/en_proceso
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH agg AS (
        SELECT
            t.connexa_header_uuid,
            SUM(CASE WHEN t.estado IN ('PENDIENTE','EN_PROCESO') THEN 1 ELSE 0 END) AS cnt_abiertas,
            SUM(CASE WHEN t.estado = 'ERROR' THEN 1 ELSE 0 END) AS cnt_error,
            SUM(CASE WHEN t.estado IN ('PROCESADO','DUPLICADO') THEN 1 ELSE 0 END) AS cnt_ok,
            COUNT(*) AS cnt_total,
            MAX(t.f_procesado) AS last_processed_at
        FROM repl.TRANSF_CONNEXA_IN t
        WHERE t.connexa_header_uuid IS NOT NULL
          AND (@Desde IS NULL OR t.f_procesado >= @Desde)
        GROUP BY t.connexa_header_uuid
    ),
    last_err AS (
        SELECT
            t.connexa_header_uuid,
            -- último mensaje de error por cabecera (si hubo)
            MAX(CONVERT(VARCHAR(255), t.mensaje_error)) AS last_error_message
        FROM repl.TRANSF_CONNEXA_IN t
        WHERE t.connexa_header_uuid IS NOT NULL
          AND t.estado = 'ERROR'
          AND (@Desde IS NULL OR t.f_procesado >= @Desde)
        GROUP BY t.connexa_header_uuid
    )
    SELECT
        a.connexa_header_uuid,
        a.cnt_total,
        a.cnt_ok,
        a.cnt_error,
        a.cnt_abiertas,
        a.last_processed_at,
        CASE WHEN a.cnt_error > 0 THEN 'ERROR' ELSE 'OK' END AS resultado,
        ISNULL(e.last_error_message, '') AS mensaje_error
    FROM agg a
    LEFT JOIN last_err e
      ON e.connexa_header_uuid = a.connexa_header_uuid
    WHERE (@SoloCerrables = 0 OR a.cnt_abiertas = 0)
    ORDER BY a.last_processed_at DESC;
END

GO
