-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO







CREATE         VIEW [repl].[V_VALKIMIA_ESTADO_TRANFERENCIAS]
AS
SELECT [INIId],[INIFecEnt],[INIDepId],[INIEntId],[INIObs],[INIArtId],[INIArtC],[INIUxB],[INICnt1]
	,[INICnt2],[INIEst],[INIFecReg],[INIUsuReg],[EmpId],[INIFecEst],[INIIdSincro],[INIMRecibido]
	,[INIMotPed],[INICnt2Rem],[INICnt1Rem],[INICnt2Pre],[INICnt1Pre],[INILinPrio]
  FROM [DIARCO-VKMSQL\SQL2008R2].[VALKIMIA].[dbo].[IntNecIN]
  WHERE [INIFecEnt] >'2026-03-01'


GO
