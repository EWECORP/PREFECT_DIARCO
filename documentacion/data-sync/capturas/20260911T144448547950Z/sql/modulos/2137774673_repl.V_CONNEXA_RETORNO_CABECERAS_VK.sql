-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   VIEW repl.V_CONNEXA_RETORNO_CABECERAS_VK
AS
WITH resumen_cabecera AS (
    SELECT
        l.connexa_header_uuid,
        COUNT(*) AS total_lineas,
        SUM(CASE WHEN l.INIEst = 'ACO' THEN 1 ELSE 0 END) AS cant_aco,
        SUM(CASE WHEN l.INIEst = 'PRE' THEN 1 ELSE 0 END) AS cant_pre,
        SUM(CASE WHEN l.INIEst = 'REM' THEN 1 ELSE 0 END) AS cant_rem,
        SUM(CASE WHEN l.INIEst = 'ETR' THEN 1 ELSE 0 END) AS cant_etr,
        SUM(CASE WHEN l.INIEst NOT IN ('ACO', 'PRE', 'REM', 'ETR') OR l.INIEst IS NULL THEN 1 ELSE 0 END) AS cant_otro
    FROM repl.V_CONNEXA_VK_ULTIMO_ESTADO_LINEA l
    GROUP BY l.connexa_header_uuid
)
SELECT
    connexa_header_uuid,
    total_lineas,
    cant_aco,
    cant_pre,
    cant_rem,
    cant_etr,
    cant_otro,
    CASE
        WHEN cant_otro > 0 THEN 'PENDIENTE'
        WHEN cant_aco > 0 OR cant_pre > 0 THEN 'PENDIENTE'
        WHEN cant_rem = total_lineas THEN 'OK'
        WHEN cant_etr > 0 AND cant_aco = 0 AND cant_pre = 0 THEN 'ERROR'
        ELSE 'PENDIENTE'
    END AS resultado,
    CASE
        WHEN cant_otro > 0 THEN 0
        WHEN cant_aco > 0 OR cant_pre > 0 THEN 0
        WHEN cant_rem = total_lineas THEN 1
        WHEN cant_etr > 0 AND cant_aco = 0 AND cant_pre = 0 THEN 1
        ELSE 0
    END AS cerrable
FROM resumen_cabecera;

GO
