-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO



CREATE PROCEDURE [dbo].[SP_CNX_M_95_SENSIBLES]
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE M_95_SENSIBLES;

    -- Insertar los datos en la tabla M_95_SENSIBLES con la fecha de procesamiento
    INSERT INTO M_95_SENSIBLES (COD_PRD, F_DATO, F_PROC)
    SELECT 
        CAST(C_ARTICULO AS VARCHAR(10)) AS COD_PRD,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM 
        [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS WITH (NOLOCK)
    WHERE 
        C_CLASIFICACION_COMPRA IN (1, 6) -- Filtrar artículos sensibles
        AND M_BAJA = 'N'; -- Filtrar solo productos que no están dados de baja
END;

GO
