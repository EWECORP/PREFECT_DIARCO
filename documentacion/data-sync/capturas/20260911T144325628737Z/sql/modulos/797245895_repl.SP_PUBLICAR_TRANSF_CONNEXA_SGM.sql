-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [repl].[SP_PUBLICAR_TRANSF_CONNEXA_SGM]
    @BatchSize       INT = 1500,
    @MaxSeconds      INT = 120,
    @M_CONECTION     VARCHAR(1) = 'N',      -- N => debe nacer M_ENVIADO='N'
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
        WHERE t.estado IN ('PENDIENTE','ERROR')
          AND (t.f_procesado IS NULL OR t.f_procesado < DATEADD(MINUTE, -10, SYSDATETIME()))
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

    -- Variables para la lógica de SUMA (cuando RC=98)
    DECLARE
        @m_enviado_exist CHAR(1),
        @q_factor_exist  DECIMAL(6,0),
        @u_id_exist      INT,
        @sum_rc          INT,
        @sum_msg         VARCHAR(255);

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
                -- 1) Intento de ALTA normal en SGM (T058)
                EXEC @RC = [DIARCOP001].[DiarcoP].[dbo].[SD03_TRANSF_ALTA_DETALLE]
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

                -- 2) Si es duplicado (98) => intentar SUMA controlada en T058
                IF @RC = 98
                BEGIN
                    SET @sum_rc = 0;
                    SET @sum_msg = '';

                    -- Leer estado actual de la fila existente en SGM (con lectura consistente)
                    SELECT
                        @m_enviado_exist = t.M_ENVIADO,
                        @q_factor_exist  = t.Q_FACTOR,
                        @u_id_exist      = t.U_ID_SINCRO
                    FROM [DIARCOP001].[DiarcoP].[dbo].[T058_ARTICULOS_TRANSF_PEND] t WITH (NOLOCK)
                    WHERE t.C_ARTICULO = @c_articulo
                      AND t.C_SUCU_DEST = @c_sucu_dest
                      AND t.C_SUCU_ORIG = @c_sucu_orig;

                    IF @u_id_exist IS NULL OR @u_id_exist = 0
                    BEGIN
                        SET @sum_rc = 97;
                        SET @sum_msg = 'Duplicado 98 pero no se encontró fila en T058 para acumular.';
                    END
                    ELSE IF ISNULL(@m_enviado_exist,'?') <> 'N'
                    BEGIN
                        SET @sum_rc = 96;
                        SET @sum_msg = CONCAT('No acumulable: M_ENVIADO=', ISNULL(@m_enviado_exist,'NULL'), ' (ya enviada).');
                    END
                    ELSE IF @q_factor_exist IS NOT NULL AND @q_factor_exist <> @q_factor
                    BEGIN
                        SET @sum_rc = 95;
                        SET @sum_msg = CONCAT('No acumulable: Q_FACTOR existente=', CAST(@q_factor_exist AS VARCHAR(20)),
                                              ' difiere de nuevo=', CAST(@q_factor AS VARCHAR(20)), '.');
                    END
                    ELSE
                    BEGIN
                        -- SUMA (con lock para evitar carreras)
                        UPDATE t WITH (UPDLOCK, HOLDLOCK)
                           SET Q_BULTOS = t.Q_BULTOS + @q_bultos,
                               Q_FACTOR = COALESCE(t.Q_FACTOR, @q_factor),
                               F_ALTA = GETDATE(),
                               C_USUARIO = LEFT(@vchUsuario_eff, 10),
                               C_TERMINAL = LEFT(@vchTerminal_eff, 10),
                               -- mantener semántica: sigue pendiente de enviar
                               M_ENVIADO = 'N',
                               -- opcional: si quieren, pueden priorizar si alguno viene con prioridad
                               M_TRANSF_PRIORIDAD = CASE WHEN ISNULL(t.M_TRANSF_PRIORIDAD,'N')='S' OR @forzarTransf_eff='S' THEN 'S' ELSE ISNULL(t.M_TRANSF_PRIORIDAD,'N') END
                        FROM [DIARCOP001].[DiarcoP].[dbo].[T058_ARTICULOS_TRANSF_PEND] t
                        WHERE t.C_ARTICULO = @c_articulo
                          AND t.C_SUCU_DEST = @c_sucu_dest
                          AND t.C_SUCU_ORIG = @c_sucu_orig
                          AND t.M_ENVIADO = 'N';

                        IF @@ROWCOUNT = 1
                        BEGIN
                            SET @sum_rc = 0;
                            SET @sum_msg = 'ACUMULADO OK (se sumó Q_BULTOS).';
                            SET @U_ID_SINCRO = @u_id_exist; -- trazabilidad: usar el id existente
                            SET @RC = 0;                    -- tratar como éxito
                            SET @vchMensaje = @sum_msg;
                        END
                        ELSE
                        BEGIN
                            SET @sum_rc = 96;
                            SET @sum_msg = 'No se pudo acumular: la fila dejó de estar en M_ENVIADO=N (concurrencia).';
                        END
                    END

                    -- Si no se pudo acumular, mantener RC=98 para que caiga en DUPLICADO
                    IF @sum_rc <> 0
                    BEGIN
                        SET @vchMensaje = LEFT(CONCAT(ISNULL(@vchMensaje,''), ' | SUMA_FAIL: ', @sum_msg), 255);
                        SET @RC = 98;
                    END
                END
            END

            -- Post-proceso en data-sync
            IF @RC = 0
            BEGIN
                UPDATE [repl].[TRANSF_CONNEXA_IN]
                   SET estado='PROCESADO',
                       u_id_sincro=@U_ID_SINCRO,
                       mensaje_error='',
                       f_procesado=SYSDATETIME()
                 WHERE id=@id;

                SET @Processed += 1;
            END
            ELSE IF @RC = 98
            BEGIN
                UPDATE [repl].[TRANSF_CONNEXA_IN]
                   SET estado='DUPLICADO',
                       u_id_sincro=ISNULL(NULLIF(@U_ID_SINCRO,0), u_id_sincro),
                       mensaje_error=LEFT(ISNULL(NULLIF(@vchMensaje,''), 'Duplicado / no acumulable'), 255),
                       f_procesado=SYSDATETIME()
                 WHERE id=@id;
            END
            ELSE
            BEGIN
                UPDATE [repl].[TRANSF_CONNEXA_IN]
                   SET estado='ERROR',
                       u_id_sincro=@U_ID_SINCRO,
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

    -- Cualquier EN_PROCESO que haya quedado colgado vuelve a PENDIENTE
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
