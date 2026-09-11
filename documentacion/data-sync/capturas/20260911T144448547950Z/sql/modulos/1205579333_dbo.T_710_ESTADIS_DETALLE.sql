-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE VIEW T710_ESTADIS_DETALLE AS 
	SELECT CONVERT(varchar,[F_DIA],121) AS 'F_DIA'
      ,CONVERT(varchar,[C_SUCU_EMPR]) AS 'C_SUCU_EMPR'
      ,CONVERT(varchar,[C_ARTICULO]) AS 'C_ARTICULO'
      ,CONVERT(varchar,[M_DOMINGO]) AS 'M_DOMINGO'
      ,CONVERT(varchar,[M_FOLDER]) AS 'M_FOLDER'
      ,CONVERT(varchar,[M_OFERTA]) AS 'M_OFERTA'
      ,CONVERT(varchar,[Q_VENTA]) AS 'Q_VENTA'
      ,CONVERT(varchar,[Q_STOCK]) AS 'Q_STOCK'
      ,CONVERT(varchar,[M_SEPA]) AS 'M_SEPA'
	  FROM [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_DETALLE]
	  WHERE [F_DIA] = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
GO
