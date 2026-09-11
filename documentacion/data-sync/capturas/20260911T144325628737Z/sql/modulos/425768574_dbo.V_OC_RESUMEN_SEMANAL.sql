-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE       VIEW [dbo].[V_OC_RESUMEN_SEMANAL]
AS
SELECT 
    cab.[C_PROVEEDOR],
    cab.[C_COMPRADOR],
    -- Agrupamos por año y semana de la emisión
	CONCAT(
		DATEPART(YEAR, cab.[F_EMISION]), 
		'-', 
		RIGHT('00' + CAST(DATEPART(ISO_WEEK, cab.[F_EMISION]) AS VARCHAR(2)), 2)
	) AS Semana_Ano,

    COUNT(DISTINCT cab.[C_OC_SGM]) AS Total_OC,   -- cantidad de órdenes distintas
    SUM(det.[Q_BULTOS_PEDIDOS]) AS Total_Bultos_Pedidos
FROM [data-sync].[dbo].[V_OC_CABECERA_SGM] cab
INNER JOIN [data-sync].[dbo].[V_OC_DETALLE_SGM] det
    ON cab.[C_OC] = det.[C_OC]
   AND cab.[U_PREFIJO_OC] = det.[U_PREFIJO_OC]
   AND cab.[U_SUFIJO_OC] = det.[U_SUFIJO_OC]
GROUP BY 
    cab.[C_PROVEEDOR],
    cab.[C_COMPRADOR],
    CONCAT( DATEPART(YEAR, cab.[F_EMISION]), '-', 
		RIGHT('00' + CAST(DATEPART(ISO_WEEK, cab.[F_EMISION]) AS VARCHAR(2)), 2));

GO
