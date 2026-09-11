-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE PROCEDURE [dbo].[SP_CNX_M_92_DEPOSITOS]
AS
BEGIN
    SET NOCOUNT ON;
		
    TRUNCATE TABLE M_92_DEPOSITOS;

    -- Insertar los datos en la tabla M_92_DEPOSITOS con la fecha de procesamiento
    INSERT INTO M_92_DEPOSITOS (ID, DC_NOMBRE, F_DATO,  F_PROC)
    SELECT 
        CASE 
            WHEN C_SUCU_EMPR = 41 THEN '41CD'
            WHEN C_SUCU_EMPR = 82 THEN '82CD'
            ELSE DBO.[NORMALIZA_STRING](C_SUCU_EMPR) 
        END AS ID,
        DBO.[NORMALIZA_STRING](N_SUCURSAL) AS DC_NOMBRE,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIMEecha de procesamiento en formato YYYY-MM-DD
    FROM 
        [DIARCOP001].[DIARCOP].dbo.T100_EMPRESA_SUC WITH (NOLOCK)
    WHERE 
        C_SUCU_EMPR IN (41, 82); -- Filtrar solo para las tiendas 41 y 82

END;

GO
