-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO






CREATE       VIEW [dbo].[V_USO_SEMANAL_PROVEEDOR] AS
SELECT  
      COALESCE(CNX.C_PROVEEDOR, SGM.C_PROVEEDOR) AS C_PROVEEDOR,
	  COALESCE(CNX.C_COMPRADOR, SGM.C_COMPRADOR) AS C_COMPRADOR,
      COALESCE(CNX.Semana_Ano, SGM.Semana_Ano) AS SEMANA,

      -- Métricas Connexa
      CNX.Total_Pedidos_CNX,
      CNX.Total_OC_CNX,
      CNX.Total_BULTOS_CNX,

      -- Métricas SGM
      SGM.Total_OC_SGM,
      SGM.Total_BULTOS_SGM

FROM (
      SELECT 
            C_PROVEEDOR,[C_COMPRADOR],
            Semana_Ano,
            COUNT(DISTINCT C_PEDIDO_CONNEXA) AS Total_Pedidos_CNX,
            SUM(Total_OC_SGM) AS Total_OC_CNX,
            SUM(Total_BULTOS) AS Total_BULTOS_CNX
      FROM dbo.V_OC_CONNEXA_SEMANAL
      GROUP BY C_PROVEEDOR,[C_COMPRADOR], Semana_Ano
) CNX

FULL OUTER JOIN (
      SELECT 
            C_PROVEEDOR,[C_COMPRADOR],
            Semana_Ano,
            SUM(Total_OC) AS Total_OC_SGM,
            SUM(Total_Bultos_Pedidos) AS Total_BULTOS_SGM
      FROM dbo.V_OC_RESUMEN_SEMANAL
      GROUP BY C_PROVEEDOR,[C_COMPRADOR], Semana_Ano
) SGM
ON CNX.C_PROVEEDOR = SGM.C_PROVEEDOR
AND CNX.Semana_Ano = SGM.Semana_Ano;



GO
