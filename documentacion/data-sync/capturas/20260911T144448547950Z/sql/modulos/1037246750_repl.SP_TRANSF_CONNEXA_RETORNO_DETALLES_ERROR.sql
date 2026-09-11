-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [repl].[SP_TRANSF_CONNEXA_RETORNO_DETALLES_ERROR]
    @Desde DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        t.connexa_header_uuid,
        t.connexa_detail_uuid,
        t.id AS id_staging,
        t.c_articulo,
        t.c_sucu_orig,
        t.c_sucu_dest,
        t.q_bultos,
        t.q_factor,
        t.estado,
        t.mensaje_error,
        t.u_id_sincro,
        t.f_procesado
    FROM repl.TRANSF_CONNEXA_IN t
    WHERE t.estado = 'ERROR'
      AND t.connexa_header_uuid IS NOT NULL
      AND t.connexa_detail_uuid IS NOT NULL
      AND (@Desde IS NULL OR t.f_procesado >= @Desde)
    ORDER BY t.f_procesado DESC, t.id DESC;
END

GO
