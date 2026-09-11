-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE     PROCEDURE [dbo].[SP_CNX_M_91_SUCURSALES]
AS
BEGIN
    SET NOCOUNT ON;
	   
	TRUNCATE TABLE M_91_SUCURSALES;

    -- Insertar los datos en la tabla M_91_SUCURSALES con la fecha de procesamiento
    INSERT INTO M_91_SUCURSALES (ID_TIENDA, SUC_NOMBRE, F_DATO, F_PROC, SUC_ABREV)
    SELECT 
        CASE 
            WHEN C_SUCU_EMPR = 41 THEN '41CD'
            WHEN C_SUCU_EMPR = 82 THEN '82CD'
            ELSE DBO.[NORMALIZA_STRING](C_SUCU_EMPR) 
        END AS ID_TIENDA,
        DBO.[NORMALIZA_STRING](N_SUCURSAL) AS SUC_NOMBRE,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC, -- Fecha y hora actual en formato DATETIME
		DBO.[NORMALIZA_STRING](N_SUCURSAL_ABREV) AS SUC_ABREV
    FROM 
        [DIARCOP001].[DIARCOP].dbo.T100_EMPRESA_SUC WITH (NOLOCK)
    WHERE 
        C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88) -- Excluir tiendas no necesarias
    AND 
        M_SUCU_VIRTUAL = 'N' -- Excluir tiendas virtuales
    AND 
        C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].dbo.T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB) -- Excluir sucursales cerradas por gerencia



    -- Insertar los datos en la tabla replica M_91_SUCURSALES con la fecha de procesamiento
	TRUNCATE TABLE repl.M_91_SUCURSALES;
    INSERT INTO repl.M_91_SUCURSALES (ID_TIENDA, SUC_NOMBRE, F_DATO, F_PROC, SUC_ABREV)
	SELECT * FROM dbo.M_91_SUCURSALES

END;

GO
