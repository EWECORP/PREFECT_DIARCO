-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE PROCEDURE [dbo].[SP_CNX_M_94_ALTERNATIVOS]
AS
BEGIN
    SET NOCOUNT ON;

	TRUNCATE TABLE M_94_ALTERNATIVOS;
    -- Insertar los datos en la tabla M_94_ALTERNATIVOS con la fecha de procesamiento
    INSERT INTO M_94_ALTERNATIVOS (COD_PRD, COD_PROD_ALTERNATIVO, F_DATO, F_PROC)
    SELECT 
        CAST(C_ARTICULO AS VARCHAR(10)) AS COD_PRD,
        CAST(C_ARTICULO_ALTERNATIVO AS VARCHAR(10)) AS COD_PROD_ALTERNATIVO,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM 
        [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS WITH (NOLOCK)
    WHERE 
        M_BAJA = 'N' -- Filtrar solo productos que no están dados de baja
        AND C_ARTICULO_ALTERNATIVO <> 0; -- Excluir artículos sin alternativos

END;

GO
