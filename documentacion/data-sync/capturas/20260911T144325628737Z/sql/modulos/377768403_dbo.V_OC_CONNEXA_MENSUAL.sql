-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE                 VIEW [dbo].[V_OC_CONNEXA_MENSUAL]
AS
SELECT 
	[C_COMPRADOR]
	,[MES]
	,[C_PROVEEDOR]
	,[C_COMPRA_KIKKER]   as C_PEDIDO_CONNEXA
	,COUNT(DISTINCT [C_SUCU_EMPR]) AS Total_SUCU
	,COUNT(DISTINCT [U_SUFIJO_OC]) AS Total_OC_SGM
	,SUM(CAST([Q_BULTOS_KILOS_DIARCO] AS INT)) AS Total_BULTOS
  FROM [data-sync].[dbo].[V_T874_OC_PRECARGA_KIKKER_HIST]
  WHERE M_PROCESADO = 'S'
  GROUP BY 
	[C_COMPRADOR]
	,[MES]
	,[C_PROVEEDOR]
	,[C_COMPRA_KIKKER];


GO
