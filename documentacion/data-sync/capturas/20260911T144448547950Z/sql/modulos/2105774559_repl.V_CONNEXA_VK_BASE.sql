-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   VIEW repl.V_CONNEXA_VK_BASE
AS
SELECT
    t.connexa_header_uuid,
    t.connexa_detail_uuid,
    t.u_id_sincro,

    v.INIId,
    v.INIIdSincro,
    v.INIEst,
    v.INIFecEnt,
    v.INIFecReg,
    v.INIFecEst,
    v.INIDepId,
    v.INIEntId,
    v.INIArtId,
    v.INIArtC,
    v.INIUxB,
    v.INICnt1,
    v.INICnt2,
    v.INIMRecibido,
    v.INIMotPed,
    v.INICnt2Rem,
    v.INICnt1Rem,
    v.INICnt2Pre,
    v.INICnt1Pre,
    v.INILinPrio,
    v.EmpId
FROM repl.TRANSF_CONNEXA_IN t
INNER JOIN repl.V_VALKIMIA_ESTADO_TRANFERENCIAS v
    ON v.INIIdSincro = t.u_id_sincro
WHERE t.connexa_header_uuid IS NOT NULL
  AND t.connexa_detail_uuid IS NOT NULL
  AND t.u_id_sincro IS NOT NULL;

GO
