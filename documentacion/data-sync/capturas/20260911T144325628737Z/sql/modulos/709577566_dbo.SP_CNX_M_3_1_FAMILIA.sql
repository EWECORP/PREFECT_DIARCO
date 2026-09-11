-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE PROCEDURE [dbo].[SP_CNX_M_3_1_FAMILIA]
AS
BEGIN
    SET NOCOUNT ON;
	
	TRUNCATE TABLE M_3_1_FAMILIA;

    -- Crear tabla temporal para almacenar datos de familias
    CREATE TABLE #TEMP_CONNEXA_DATA (
        C_FAMILIA DECIMAL(5, 0) NULL DEFAULT 0,
        N_FAMILIA CHAR(30) NOT NULL DEFAULT ''
    );

    -- Insertar los códigos de clasificación de compra de los artículos
    INSERT INTO #TEMP_CONNEXA_DATA (C_FAMILIA)
    SELECT DISTINCT C_CLASIFICACION_COMPRA
    FROM [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS WITH (NOLOCK);

    -- Actualizar los nombres de familia usando la tabla de códigos
    UPDATE T1
    SET T1.N_FAMILIA = T2.D_CODIGO
    FROM #TEMP_CONNEXA_DATA T1
    INNER JOIN [DIARCOP001].[DiarcoP].dbo.T001_TABLA_CODIGO T2 WITH (NOLOCK)
        ON T1.C_FAMILIA = T2.C_CODIGO_TABLA
    WHERE T2.C_TABLA = 119;

    -- Insertar los datos finales en la tabla M_3_1_FAMILIA
    INSERT INTO M_3_1_FAMILIA (COD_FAMILIA, N_FAMILIA, F_DATO, F_PROC)
    SELECT DISTINCT
        CAST(C_FAMILIA AS VARCHAR(4)) AS COD_FAMILIA,
        DBO.[NORMALIZA_STRING](N_FAMILIA) AS NOME_FAMILIA,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM #TEMP_CONNEXA_DATA;

    -- Eliminar la tabla temporal
    DROP TABLE #TEMP_CONNEXA_DATA;
END;

GO
