USE [data-sync];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [repl].[SP_PUBLICAR_TRANSF_CONNEXA_VK]
    @BatchSize       INT = 500,
    @MaxSeconds      INT = 120,
    @M_CONECTION     VARCHAR(1) = 'S',
    @DefaultUsuario  VARCHAR(10) = 'CONNEXA',
    @DefaultTerminal VARCHAR(10) = 'API'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @StartTime  DATETIME2(0) = SYSDATETIME(),
        @Now        DATETIME2(0),
        @Processed  INT = 0,
        @Claimed    INT = 0;

    -- IDs reclamados en este batch
    DECLARE @Work TABLE (id INT NOT NULL PRIMARY KEY);

    -------------------------------------------------------------------------
    -- 1) CLAIM atómico (solo lo que ya está PROCESADO en SGM y falta VK)
    -------------------------------------------------------------------------
    ;WITH cte AS (
        SELECT TOP (@BatchSize) t.id
        FROM [repl].[TRANSF_CONNEXA_IN] t WITH (READPAST, UPDLOCK, ROWLOCK)
        WHERE t.estado = 'PROCESADO'
          AND ISNULL(t.estado_vk, 'PENDIENTE') IN ('PENDIENTE', 'ERROR')  -- reintento si falló VK
          AND t.u_id_sincro IS NOT NULL
          AND t.u_id_sincro > 0
        ORDER BY t.id
    )
    UPDATE t
       SET estado_vk = 'EN_PROCESO',
           mensaje_error_vk = '',
           f_procesado_vk = NULL
      OUTPUT inserted.id INTO @Work(id)
    FROM [repl].[TRANSF_CONNEXA_IN] t
    INNER JOIN cte ON cte.id = t.id;

    SELECT @Claimed = COUNT(1) FROM @Work;

    IF @Claimed = 0
    BEGIN
        SELECT
            claimed   = 0,
            processed = 0,
            elapsed_s = DATEDIFF(SECOND, @StartTime, SYSDATETIME());
        RETURN 0;
    END

    -------------------------------------------------------------------------
    -- 2) Cursor de trabajo
    -------------------------------------------------------------------------
    DECLARE
        @id               INT,
        @c_articulo       DECIMAL(6,0),
        @c_sucu_dest      DECIMAL(3,0),
        @c_sucu_orig      DECIMAL(3,0),
        @q_bultos         DECIMAL(13,3),
        @q_factor         DECIMAL(6,0),
        @f_alta           DATETIME,
        @m_alta_prioridad VARCHAR(1),
        @vchUsuario       VARCHAR(10),
        @vchTerminal      VARCHAR(10),
        @forzarTransf     VARCHAR(1),
        @u_id_sincro      INT,

        -- Efectivos
        @m_alta_prioridad_eff VARCHAR(1),
        @forzarTransf_eff     VARCHAR(1),
        @vchUsuario_eff       VARCHAR(10),
        @vchTerminal_eff      VARCHAR(10),

        -- VK-specific
        @q_factor_proveedor   DECIMAL(3,0),
        @Q_PESO_UNIT_ART      DECIMAL(6,2),
        @M_VENDE_POR_PESO     VARCHAR(1),

        @vchMensaje       VARCHAR(255),
        @RC               INT;

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            t.id,
            t.c_articulo,
            t.c_sucu_dest,
            t.c_sucu_orig,
            t.q_bultos,
            t.q_factor,
            t.f_alta,
            t.m_alta_prioridad,
            t.vchUsuario,
            t.vchTerminal,
            t.forzarTransf,
            t.u_id_sincro
        FROM [repl].[TRANSF_CONNEXA_IN] t
        INNER JOIN @Work w ON w.id = t.id
        ORDER BY t.id;

    OPEN cur;

    FETCH NEXT FROM cur INTO
        @id, @c_articulo, @c_sucu_dest, @c_sucu_orig,
        @q_bultos, @q_factor, @f_alta,
        @m_alta_prioridad, @vchUsuario, @vchTerminal, @forzarTransf,
        @u_id_sincro;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Now = SYSDATETIME();

        IF DATEDIFF(SECOND, @StartTime, @Now) >= @MaxSeconds
            BREAK;

        SET @vchMensaje  = '';
        SET @RC          = 0;

        -- Normalizaciones
        SET @m_alta_prioridad_eff = ISNULL(NULLIF(@m_alta_prioridad, ''), 'N');
        SET @forzarTransf_eff     = ISNULL(NULLIF(@forzarTransf, ''), 'N');
        SET @vchUsuario_eff       = ISNULL(NULLIF(@vchUsuario, ''), @DefaultUsuario);
        SET @vchTerminal_eff      = ISNULL(NULLIF(@vchTerminal, ''), @DefaultTerminal);

        -- VK defaults (etapa 1: no por peso)
        SET @M_VENDE_POR_PESO = 'N';
        SET @Q_PESO_UNIT_ART = 0;

        -- q_factor_proveedor: usar q_factor (ojo: VK lo define DECIMAL(3,0))
        SET @q_factor_proveedor = TRY_CONVERT(DECIMAL(3,0), @q_factor);

        BEGIN TRY
            -- Validaciones defensivas
            IF @q_factor_proveedor IS NULL
            BEGIN
                SET @RC = 99;
                SET @vchMensaje = 'q_factor fuera de rango para VK (no entra en DECIMAL(3,0)).';
            END
            ELSE IF @c_articulo IS NULL OR @c_sucu_dest IS NULL OR @c_sucu_orig IS NULL
               OR @q_bultos IS NULL OR @q_factor IS NULL OR @u_id_sincro IS NULL
            BEGIN
                SET @RC = 99;
                SET @vchMensaje = 'Datos incompletos para publicar VK (NULL).';
            END
            ELSE IF @c_articulo = 0 OR @c_sucu_dest = 0 OR @c_sucu_orig = 0 OR @u_id_sincro <= 0
            BEGIN
                SET @RC = 99;
                SET @vchMensaje = 'Datos inválidos para publicar VK (códigos=0 o u_id_sincro inválido).';
            END
            ELSE
            BEGIN
                EXEC @RC = [10.54.200.88].[VALKIMIA].[dbo].[SD03_TRANSF_ALTA_DETALLE_VK]
                    @M_CONECTION         = @M_CONECTION,
                    @c_accion            = 'A',
                    @c_articulo          = @c_articulo,
                    @c_sucu_dest         = @c_sucu_dest,
                    @c_sucu_orig         = @c_sucu_orig,
                    @q_bultos            = @q_bultos,
                    @q_factor            = @q_factor,
                    @Q_PESO_UNIT_ART     = @Q_PESO_UNIT_ART,
                    @q_factor_proveedor  = @q_factor_proveedor,
                    @Q_BULTOS_ORIG       = 0,
                    @Q_FACTOR_ORIG       = 0,
                    @f_alta              = @f_alta,
                    @M_ALTA_PRIORIDAD    = @m_alta_prioridad_eff,
                    @M_VENDE_POR_PESO    = @M_VENDE_POR_PESO,
                    @vchUsuario          = @vchUsuario_eff,
                    @vchTerminal         = @vchTerminal_eff,
                    @U_ID_SINCRO         = @u_id_sincro,
                    @vchMensaje          = @vchMensaje OUTPUT,
                    @ForzarTransf        = @forzarTransf_eff;
            END

            IF @RC = 0
            BEGIN
                UPDATE [repl].[TRANSF_CONNEXA_IN]
                   SET estado_vk       = 'PROCESADO',
                       mensaje_error_vk = '',
                       f_procesado_vk  = SYSDATETIME()
                 WHERE id = @id;

                SET @Processed += 1;
            END
            ELSE
            BEGIN
                UPDATE [repl].[TRANSF_CONNEXA_IN]
                   SET estado_vk       = 'ERROR',
                       mensaje_error_vk = LEFT(ISNULL(NULLIF(@vchMensaje,''), 'Error sin mensaje'), 255),
                       f_procesado_vk  = SYSDATETIME()
                 WHERE id = @id;
            END
        END TRY
        BEGIN CATCH
            UPDATE [repl].[TRANSF_CONNEXA_IN]
               SET estado_vk        = 'ERROR',
                   mensaje_error_vk = LEFT(CONCAT('EXCEPTION: ', ERROR_MESSAGE()), 255),
                   f_procesado_vk   = SYSDATETIME()
             WHERE id = @id;
        END CATCH

        FETCH NEXT FROM cur INTO
            @id, @c_articulo, @c_sucu_dest, @c_sucu_orig,
            @q_bultos, @q_factor, @f_alta,
            @m_alta_prioridad, @vchUsuario, @vchTerminal, @forzarTransf,
            @u_id_sincro;
    END

    CLOSE cur;
    DEALLOCATE cur;

    -------------------------------------------------------------------------
    -- 3) Si cortó por timeout, liberar EN_PROCESO a PENDIENTE (reintento)
    -------------------------------------------------------------------------
    UPDATE t
       SET estado_vk = 'PENDIENTE'
    FROM [repl].[TRANSF_CONNEXA_IN] t
    INNER JOIN @Work w ON w.id = t.id
    WHERE t.estado_vk = 'EN_PROCESO'
      AND t.f_procesado_vk IS NULL;

    SELECT
        claimed   = @Claimed,
        processed = @Processed,
        elapsed_s = DATEDIFF(SECOND, @StartTime, SYSDATETIME());

    RETURN 0;
END
GO


