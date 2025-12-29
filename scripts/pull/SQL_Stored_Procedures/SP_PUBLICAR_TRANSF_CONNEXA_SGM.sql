CREATE OR ALTER PROCEDURE [repl].[SP_PUBLICAR_TRANSF_CONNEXA_SGM]
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

    DECLARE @Work TABLE (id INT NOT NULL PRIMARY KEY);

    ;WITH cte AS (
        SELECT TOP (@BatchSize) t.id
        FROM [repl].[TRANSF_CONNEXA_IN] t WITH (READPAST, UPDLOCK, ROWLOCK)
        WHERE t.estado IN ('PENDIENTE','ERROR')  -- (A) permitir reintento
          AND (t.f_procesado IS NULL OR t.f_procesado < DATEADD(MINUTE, -10, SYSDATETIME()))  -- cooldown opcional
        ORDER BY t.id
    )
    UPDATE t
       SET estado = 'EN_PROCESO',
           mensaje_error = '',
           f_procesado = NULL
      OUTPUT inserted.id INTO @Work(id)
    FROM [repl].[TRANSF_CONNEXA_IN] t
    INNER JOIN cte ON cte.id = t.id;

    SELECT @Claimed = COUNT(1) FROM @Work;

    IF @Claimed = 0
    BEGIN
        SELECT claimed=0, processed=0, elapsed_s=DATEDIFF(SECOND, @StartTime, SYSDATETIME());
        RETURN 0;
    END

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
        @m_alta_prioridad_eff VARCHAR(1),
        @forzarTransf_eff     VARCHAR(1),
        @vchUsuario_eff       VARCHAR(10),
        @vchTerminal_eff      VARCHAR(10),
        @U_ID_SINCRO      INT,
        @vchMensaje       VARCHAR(255),
        @RC               INT;

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT t.id, t.c_articulo, t.c_sucu_dest, t.c_sucu_orig,
               t.q_bultos, t.q_factor, t.f_alta,
               t.m_alta_prioridad, t.vchUsuario, t.vchTerminal, t.forzarTransf
        FROM [repl].[TRANSF_CONNEXA_IN] t
        INNER JOIN @Work w ON w.id = t.id
        ORDER BY t.id;

    OPEN cur;

    FETCH NEXT FROM cur INTO
        @id, @c_articulo, @c_sucu_dest, @c_sucu_orig,
        @q_bultos, @q_factor, @f_alta,
        @m_alta_prioridad, @vchUsuario, @vchTerminal, @forzarTransf;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Now = SYSDATETIME();
        IF DATEDIFF(SECOND, @StartTime, @Now) >= @MaxSeconds BREAK;

        SET @U_ID_SINCRO = 0;
        SET @vchMensaje  = '';
        SET @RC          = 0;

        SET @m_alta_prioridad_eff = ISNULL(NULLIF(@m_alta_prioridad, ''), 'N');
        SET @forzarTransf_eff     = ISNULL(NULLIF(@forzarTransf, ''), 'N');
        SET @vchUsuario_eff       = ISNULL(NULLIF(@vchUsuario, ''), @DefaultUsuario);
        SET @vchTerminal_eff      = ISNULL(NULLIF(@vchTerminal, ''), @DefaultTerminal);

        BEGIN TRY
            IF @c_articulo IS NULL OR @c_sucu_dest IS NULL OR @c_sucu_orig IS NULL
               OR @q_bultos IS NULL OR @q_factor IS NULL
            BEGIN
                SET @RC = 99;
                SET @vchMensaje = 'Datos incompletos para publicar (NULL).';
            END
            ELSE IF @c_articulo = 0 OR @c_sucu_dest = 0 OR @c_sucu_orig = 0
            BEGIN
                SET @RC = 99;
                SET @vchMensaje = 'Datos inválidos para publicar (códigos = 0).';
            END
            ELSE
            BEGIN
                EXEC @RC = [10.54.200.88].[DiarcoP].[dbo].[SD03_TRANSF_ALTA_DETALLE]
                    @M_CONECTION      = @M_CONECTION,
                    @c_accion         = 'A',
                    @c_articulo       = @c_articulo,
                    @c_sucu_dest      = @c_sucu_dest,
                    @c_sucu_orig      = @c_sucu_orig,
                    @q_bultos         = @q_bultos,
                    @q_factor         = @q_factor,
                    @Q_BULTOS_ORIG    = 0,
                    @Q_FACTOR_ORIG    = 0,
                    @f_alta           = @f_alta,
                    @M_ALTA_PRIORIDAD = @m_alta_prioridad_eff,
                    @vchUsuario       = @vchUsuario_eff,
                    @vchTerminal      = @vchTerminal_eff,
                    @U_ID_SINCRO      = @U_ID_SINCRO OUTPUT,
                    @vchMensaje       = @vchMensaje OUTPUT,
                    @ForzarTransf     = @forzarTransf_eff;
            END

            IF @RC = 0
            BEGIN
                UPDATE [repl].[TRANSF_CONNEXA_IN]
                   SET estado='PROCESADO', u_id_sincro=@U_ID_SINCRO, mensaje_error='', f_procesado=SYSDATETIME()
                 WHERE id=@id;
                SET @Processed += 1;
            END
            ELSE IF @RC = 98  -- (B) diferenciar duplicado/concurrencia
            BEGIN
                UPDATE [repl].[TRANSF_CONNEXA_IN]
                   SET estado='DUPLICADO', u_id_sincro=@U_ID_SINCRO,
                       mensaje_error=LEFT(ISNULL(NULLIF(@vchMensaje,''), 'Duplicado / concurrencia'), 255),
                       f_procesado=SYSDATETIME()
                 WHERE id=@id;
            END
            ELSE
            BEGIN
                UPDATE [repl].[TRANSF_CONNEXA_IN]
                   SET estado='ERROR', u_id_sincro=@U_ID_SINCRO,
                       mensaje_error=LEFT(ISNULL(NULLIF(@vchMensaje,''), 'Error sin mensaje'), 255),
                       f_procesado=SYSDATETIME()
                 WHERE id=@id;
            END
        END TRY
        BEGIN CATCH
            UPDATE [repl].[TRANSF_CONNEXA_IN]
               SET estado='ERROR',
                   u_id_sincro=@U_ID_SINCRO,
                   mensaje_error=LEFT(CONCAT('EXCEPTION: ', ERROR_MESSAGE()), 255),
                   f_procesado=SYSDATETIME()
             WHERE id=@id;
        END CATCH

        FETCH NEXT FROM cur INTO
            @id, @c_articulo, @c_sucu_dest, @c_sucu_orig,
            @q_bultos, @q_factor, @f_alta,
            @m_alta_prioridad, @vchUsuario, @vchTerminal, @forzarTransf;
    END

    CLOSE cur;
    DEALLOCATE cur;

    UPDATE t
       SET estado='PENDIENTE'
    FROM [repl].[TRANSF_CONNEXA_IN] t
    INNER JOIN @Work w ON w.id=t.id
    WHERE t.estado='EN_PROCESO'
      AND t.f_procesado IS NULL;

    SELECT claimed=@Claimed, processed=@Processed, elapsed_s=DATEDIFF(SECOND, @StartTime, SYSDATETIME());
    RETURN 0;
END
GO
