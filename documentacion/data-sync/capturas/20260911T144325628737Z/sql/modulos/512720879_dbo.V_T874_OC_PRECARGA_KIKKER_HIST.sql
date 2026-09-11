-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO






CREATE         VIEW [dbo].[V_T874_OC_PRECARGA_KIKKER_HIST]
AS
SELECT [C_PROVEEDOR]
      ,[C_ARTICULO]
      ,[C_SUCU_EMPR]
      ,[Q_BULTOS_KILOS_DIARCO]
      ,[F_ALTA_SIST]
      ,[C_USUARIO_GENERO_OC]
      ,[C_TERMINAL_GENERO_OC]
      ,[F_GENERO_OC]
      ,[C_USUARIO_BLOQUEO]
      ,[M_PROCESADO]
      ,[F_PROCESADO]
      ,[U_PREFIJO_OC]
      ,[U_SUFIJO_OC]
      ,[C_COMPRA_KIKKER]
      ,[C_USUARIO_MODIF]
      ,[C_COMPRADOR]
	  ,FORMAT([F_ALTA_SIST], 'yyyy-MM') AS MES
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >'2025-11-01'

GO
