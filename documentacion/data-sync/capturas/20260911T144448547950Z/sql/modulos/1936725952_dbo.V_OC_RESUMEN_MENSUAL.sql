-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO







CREATE       VIEW [dbo].[V_OC_RESUMEN_MENSUAL]
AS
SELECT 
    cab.[C_PROVEEDOR],
    cab.[C_COMPRADOR],
    -- Agrupamos por año y mes de la emisión
    YEAR(cab.[F_EMISION]) AS Anio_Emision,
    MONTH(cab.[F_EMISION]) AS Mes_Emision,
	FORMAT([F_EMISION], 'yyyy-MM') AS MES,
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
    YEAR(cab.[F_EMISION]),
    MONTH(cab.[F_EMISION]),
	FORMAT([F_EMISION], 'yyyy-MM');


GO
