-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   VIEW repl.V_CONNEXA_VK_ULTIMO_ESTADO_LINEA
AS
WITH ranked AS (
    SELECT
        b.connexa_header_uuid,
        b.connexa_detail_uuid,
        b.u_id_sincro,

        b.INIId,
        b.INIIdSincro,
        b.INIEst,
        b.INIFecEnt,
        b.INIFecReg,
        b.INIFecEst,
        b.INIDepId,
        b.INIEntId,
        b.INIArtId,
        b.INIArtC,
        b.INIUxB,
        b.INICnt1,
        b.INICnt2,
        b.INIMRecibido,
        b.INIMotPed,
        b.INICnt2Rem,
        b.INICnt1Rem,
        b.INICnt2Pre,
        b.INICnt1Pre,
        b.INILinPrio,
        b.EmpId,

        ROW_NUMBER() OVER (
            PARTITION BY b.connexa_detail_uuid
            ORDER BY
                ISNULL(b.INIFecEst, b.INIFecReg) DESC,
                b.INIId DESC
        ) AS rn
    FROM repl.V_CONNEXA_VK_BASE b
)
SELECT
    connexa_header_uuid,
    connexa_detail_uuid,
    u_id_sincro,

    INIId,
    INIIdSincro,
    INIEst,
    INIFecEnt,
    INIFecReg,
    INIFecEst,
    INIDepId,
    INIEntId,
    INIArtId,
    INIArtC,
    INIUxB,
    INICnt1,
    INICnt2,
    INIMRecibido,
    INIMotPed,
    INICnt2Rem,
    INICnt1Rem,
    INICnt2Pre,
    INICnt1Pre,
    INILinPrio,
    EmpId
FROM ranked
WHERE rn = 1;

GO
