-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO




CREATE PROCEDURE [dbo].[SP_CNX_M_9_COMPRADORES]
AS
BEGIN
    SET NOCOUNT ON;

	TRUNCATE TABLE M_9_COMPRADORES;

    -- Insertar el valor 'SIN COMPRADOR'
    INSERT INTO M_9_COMPRADORES (COD_COMPRADOR, N_COMPRADOR,F_DATO, F_PROC)
    SELECT '0', 'SIN COMPRADOR', GETDATE() , GETDATE();

    -- Insertar los datos desde la tabla T117_COMPRADORES
    INSERT INTO M_9_COMPRADORES (COD_COMPRADOR, N_COMPRADOR,F_DATO, F_PROC)
    SELECT 
        CONVERT(VARCHAR, C_COMPRADOR) AS COD_COMPRADOR,
        DBO.[NORMALIZA_STRING](N_COMPRADOR) AS N_COMPRADOR,
        GETDATE(), GETDATE()  -- Fecha de procesamiento en formato YYYY-MM-DD
    FROM 
        [DIARCOP001].[DiarcoP].dbo.T117_COMPRADORES
    WHERE 
        M_BAJA = 'N'; -- Filtrar solo los compradores activos

END;

GO
