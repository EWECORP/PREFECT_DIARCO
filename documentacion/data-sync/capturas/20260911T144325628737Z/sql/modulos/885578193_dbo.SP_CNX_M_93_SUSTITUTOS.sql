-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE PROCEDURE [dbo].[SP_CNX_M_93_SUSTITUTOS]
AS
BEGIN
    SET NOCOUNT ON;
 
	TRUNCATE TABLE M_93_SUSTITUTOS;

    -- Insertar los datos en la tabla M_93_SUSTITUTOS con la fecha de procesamiento
    INSERT INTO M_93_SUSTITUTOS (COD_PRD, COD_PROD_SUSTITUTO,F_DATO, F_PROC)
    SELECT 
        CAST(C_ARTICULO AS VARCHAR(10)) AS COD_PRD,
        CAST(C_ARTICULO_SUSTITUTO AS VARCHAR(10)) AS COD_PROD_SUSTITUTO,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM 
        [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS_SUSTITUTOS WITH (NOLOCK);

END;

GO
