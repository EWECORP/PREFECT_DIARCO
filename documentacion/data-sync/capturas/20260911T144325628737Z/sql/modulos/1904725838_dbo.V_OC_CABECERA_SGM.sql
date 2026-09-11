-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO





CREATE     VIEW [dbo].[V_OC_CABECERA_SGM]
AS
SELECT [C_PROVEEDOR]
	  ,CONCAT([U_PREFIJO_OC], '-', [U_SUFIJO_OC]) AS [C_OC_SGM]
      ,[F_EMISION]
      ,[C_SUCU_DESTINO]
	  ,[U_DIAS_LIMITE_ENTREGA]
      ,[F_ENTREGA]
      ,[C_COMPRADOR]
      ,[C_USUARIO_OPERADOR]
      ,[C_SITUAC]
      ,[F_SITUAC]  
	  ,[C_OC]
      ,[U_PREFIJO_OC]
      ,[U_SUFIJO_OC]
  FROM [data-sync].[repl].[T080_OC_CABE]
  WHERE [F_EMISION] >= '2025-06-01'

GO
