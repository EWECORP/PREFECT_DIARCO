-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO




CREATE   PROCEDURE [dbo].[SP_CNX_M_3_2_FAMILIA_ARTICULO]
AS 
BEGIN
    SET NOCOUNT ON;
	
	TRUNCATE TABLE M_3_2_FAMILIA_ARTICULO;

    -- Insertar los datos en la tabla M_3_2_FAMILIA_ARTICULO con la fecha de procesamiento
    INSERT INTO M_3_2_FAMILIA_ARTICULO (COD_FAMILIA, COD_ARTICULO,F_DATO, F_PROC)
    SELECT DISTINCT
        CAST(C_CLASIFICACION_COMPRA AS VARCHAR(10)) AS COD_FAMILIA,
        CAST(C_ARTICULO AS VARCHAR(10)) AS COD_ARTICULO,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM [DIARCOP001].[DIARCOP].dbo.T050_ARTICULOS WITH (NOLOCK)
    WHERE C_ARTICULO NOT IN (
        SELECT C_ARTICULO FROM [DIARCOP001].[DIARCOP].dbo.T050_ARTICULOS_DIFERENCIAS_DE_PRECIOS WITH (NOLOCK)
    )
    AND M_BAJA = 'N';

END;

GO
