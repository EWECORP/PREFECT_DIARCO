-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE repl.usp_purge_huerfanas_T702
    @dias INT = 45,
    @max_rows INT = 50000,        -- límite de seguridad por corrida
    @dry_run BIT = 1,             -- 1 = no borra (solo muestra y cuenta), 0 = borra
    @reason NVARCHAR(200) = N'Origen no tiene la fila (huérfana)'
AS
BEGIN
    SET NOCOUNT ON;

    -- 1) Construimos el conjunto de huérfanas dentro de la ventana
    IF OBJECT_ID('tempdb..#huérfanas') IS NOT NULL DROP TABLE #huérfanas;

    SELECT TOP (@max_rows)
        R.F_VENTA, R.C_SUCU_EMPR, R.C_ARTICULO, R.I_PRECIO_VENTA,
        R.I_VENDIDO, R.Q_UNIDADES_VENDIDAS, R.C_FAMILIA
    INTO #huérfanas
    FROM repl.T702_EST_VTAS_POR_ARTICULO AS R
    LEFT JOIN [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO] AS S
      ON  R.F_VENTA        = S.F_VENTA
      AND R.C_SUCU_EMPR    = S.C_SUCU_EMPR
      AND R.C_ARTICULO     = S.C_ARTICULO
      AND R.I_PRECIO_VENTA = S.I_PRECIO_VENTA   -- MONEY con MONEY
    WHERE R.F_VENTA >= DATEADD(DAY, -@dias, CAST(GETDATE() AS DATE))
      AND S.F_VENTA IS NULL
    ORDER BY R.F_VENTA DESC;

    DECLARE @to_delete INT = (SELECT COUNT(*) FROM #huérfanas);

    -- 2) Reporte en dry-run
    IF @dry_run = 1
    BEGIN
        SELECT @to_delete AS filas_huerfanas_a_borrar, * FROM #huérfanas;
        RETURN;
    END

    -- 3) Borrado físico con auditoría en transacción
    BEGIN TRAN;

    BEGIN TRY
        -- Auditoría (OUTPUT de lo borrado)
        DELETE R
        OUTPUT
            SYSUTCDATETIME(),             -- deleted_at_utc
            @reason,                      -- reason
            deleted.F_VENTA,
            deleted.C_SUCU_EMPR,
            deleted.C_ARTICULO,
            deleted.I_PRECIO_VENTA,
            deleted.I_VENDIDO,
            deleted.Q_UNIDADES_VENDIDAS,
            deleted.C_FAMILIA,
            NULL                          -- extra_json (opcional)
        INTO repl.audit_T702_deletes (
            deleted_at_utc, reason, F_VENTA, C_SUCU_EMPR, C_ARTICULO, I_PRECIO_VENTA,
            I_VENDIDO, Q_UNIDADES_VENDIDAS, C_FAMILIA, extra_json
        )
        FROM repl.T702_EST_VTAS_POR_ARTICULO AS R
        INNER JOIN #huérfanas H
          ON  R.F_VENTA        = H.F_VENTA
          AND R.C_SUCU_EMPR    = H.C_SUCU_EMPR
          AND R.C_ARTICULO     = H.C_ARTICULO
          AND R.I_PRECIO_VENTA = H.I_PRECIO_VENTA;

        DECLARE @deleted INT = @@ROWCOUNT;

        COMMIT;
        SELECT @deleted AS filas_borradas, @to_delete AS filas_detectadas_en_auditoria;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH
END

GO
