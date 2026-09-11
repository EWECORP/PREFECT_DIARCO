-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO





CREATE     VIEW [dbo].[V_OC_DETALLE_SGM]
AS
SELECT [C_OC]
      ,[U_PREFIJO_OC]
      ,[U_SUFIJO_OC]
      ,[C_ARTICULO]	  
      ,CAST([Q_BULTOS_PROV_PED] AS INT) AS Q_BULTOS_PEDIDOS 
      ,CAST(([Q_UNID_CUMPLIDAS] / [Q_FACTOR_PROV_PED]) AS INT) AS Q_BULTOS_CUMPLIDOS
	        ,[M_CUMPLIDA_PARCIAL]  
	  ,CAST(([Q_BULTOS_PROV_PED] - ([Q_UNID_CUMPLIDAS] / [Q_FACTOR_PROV_PED])) AS INT) AS Q_BULTOS_PENDIENTES
      ,[C_USUARIO_CUMPLIO_PARCIAL]
      ,[F_CUMPLIDA_PARCIAL]
  FROM [data-sync].[repl].[T081_OC_DETA]

GO
