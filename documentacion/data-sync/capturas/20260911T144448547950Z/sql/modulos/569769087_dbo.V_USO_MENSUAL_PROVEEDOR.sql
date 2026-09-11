-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE VIEW [dbo].[V_USO_MENSUAL_PROVEEDOR] AS
SELECT  
      COALESCE(CNX.C_PROVEEDOR, SGM.C_PROVEEDOR) AS C_PROVEEDOR,
      COALESCE(CNX.MES, SGM.MES) AS MES,

      -- Métricas Connexa
      CNX.Total_Pedidos_CNX,
      CNX.Total_OC_CNX,
      CNX.Total_BULTOS_CNX,

      -- Métricas SGM
      SGM.Total_OC_SGM,
      SGM.Total_BULTOS_SGM

FROM (
      SELECT 
            C_PROVEEDOR,
            MES,
            COUNT(DISTINCT C_PEDIDO_CONNEXA) AS Total_Pedidos_CNX,
            SUM(Total_OC_SGM) AS Total_OC_CNX,
            SUM(Total_BULTOS) AS Total_BULTOS_CNX
      FROM dbo.V_OC_CONNEXA_MENSUAL
      GROUP BY C_PROVEEDOR, MES
) CNX

FULL OUTER JOIN (
      SELECT 
            C_PROVEEDOR,
            MES,
            SUM(Total_OC) AS Total_OC_SGM,
            SUM(Total_Bultos_Pedidos) AS Total_BULTOS_SGM
      FROM dbo.V_OC_RESUMEN_MENSUAL
      GROUP BY C_PROVEEDOR, MES
) SGM
ON CNX.C_PROVEEDOR = SGM.C_PROVEEDOR
AND CNX.MES = SGM.MES;



GO
