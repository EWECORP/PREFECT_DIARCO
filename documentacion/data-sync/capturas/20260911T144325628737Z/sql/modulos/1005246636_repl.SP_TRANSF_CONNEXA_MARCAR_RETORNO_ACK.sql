-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE repl.SP_TRANSF_CONNEXA_MARCAR_RETORNO_ACK
    @connexa_header_uuid UNIQUEIDENTIFIER,
    @resultado VARCHAR(10),
    @mensaje_error VARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    MERGE repl.TRANSF_CONNEXA_RETORNO_ACK AS tgt
    USING (SELECT @connexa_header_uuid AS connexa_header_uuid) AS src
       ON tgt.connexa_header_uuid = src.connexa_header_uuid
    WHEN MATCHED THEN
        UPDATE SET informado_at = SYSDATETIME(), resultado=@resultado, mensaje_error=@mensaje_error
    WHEN NOT MATCHED THEN
        INSERT (connexa_header_uuid, resultado, mensaje_error)
        VALUES (@connexa_header_uuid, @resultado, @mensaje_error);
END

GO
