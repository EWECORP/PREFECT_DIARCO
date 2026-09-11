-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [repl].[SP_PUBLICAR_TRANSF_CONNEXA_VK]
    @BatchSize       INT = 500,
    @MaxSeconds      INT = 120,
    @pC_ARTICULO     DECIMAL(6,0) = NULL,     -- opcional: permite usar el filtro nativo
    @pTerminal       CHAR(10) = NULL          -- opcional: permite usar el filtro nativo
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @StartTime  DATETIME2(0) = SYSDATETIME(),
        @Now        DATETIME2(0),
        @Claimed    INT = 0,
        @Processed  INT = 0,
        @vchMensaje VARCHAR(255) = '';

    -------------------------------------------------------------------------
    -- 1) CLAIM: tomar líneas ya publicadas en SGM y pendientes de VK
    -------------------------------------------------------------------------
    DECLARE @Work TABLE (
        id INT NOT NULL PRIMARY KEY,
        c_articulo  DECIMAL(6,0) NOT NULL,
        c_sucu_dest DECIMAL(3,0) NOT NULL,
        c_sucu_orig DECIMAL(3,0) NOT NULL
    );

    ;WITH cte AS (
        SELECT TOP (@BatchSize)
               t.id, t.c_articulo, t.c_sucu_dest, t.c_sucu_orig
        FROM [repl].[TRANSF_CONNEXA_IN] t WITH (READPAST, UPDLOCK, ROWLOCK)
        WHERE t.estado = 'PROCESADO'
          AND ISNULL(t.estado_vk, 'PENDIENTE') IN ('PENDIENTE', 'ERROR')
          AND t.c_articulo  IS NOT NULL AND t.c_articulo  > 0
          AND t.c_sucu_dest IS NOT NULL AND t.c_sucu_dest > 0
          AND t.c_sucu_orig IS NOT NULL AND t.c_sucu_orig > 0
        ORDER BY t.id
    )
    UPDATE t
       SET estado_vk = 'EN_PROCESO',
           mensaje_error_vk = '',
           f_procesado_vk = NULL
      OUTPUT inserted.id, inserted.c_articulo, inserted.c_sucu_dest, inserted.c_sucu_orig
        INTO @Work(id, c_articulo, c_sucu_dest, c_sucu_orig)
    FROM [repl].[TRANSF_CONNEXA_IN] t
    INNER JOIN cte ON cte.id = t.id;

    SELECT @Claimed = COUNT(1) FROM @Work;

    IF @Claimed = 0
    BEGIN
        SELECT claimed=0, processed=0, elapsed_s=DATEDIFF(SECOND, @StartTime, SYSDATETIME());
        RETURN 0;
    END

    -------------------------------------------------------------------------
    -- 2) TIME GUARD
    -------------------------------------------------------------------------
    SET @Now = SYSDATETIME();
    IF DATEDIFF(SECOND, @StartTime, @Now) >= @MaxSeconds
    BEGIN
        UPDATE t
           SET estado_vk='PENDIENTE'
        FROM [repl].[TRANSF_CONNEXA_IN] t
        INNER JOIN @Work w ON w.id=t.id
        WHERE t.estado_vk='EN_PROCESO' AND t.f_procesado_vk IS NULL;

        SELECT claimed=@Claimed, processed=0, elapsed_s=DATEDIFF(SECOND, @StartTime, SYSDATETIME());
        RETURN 0;
    END

    -------------------------------------------------------------------------
    -- 3) DISPARAR EL SP NATIVO EN SGM (SGM -> VK)
    --    Nota: Este SP inserta en VK.IntNecIN y marca T058.M_ENVIADO='S'
    -------------------------------------------------------------------------
    BEGIN TRY
        EXEC [DIARCOP001].[DiarcoP].[dbo].[SD03_GERENCIA_SINCRO_IDA_VK]
            @pC_ARTICULO = @pC_ARTICULO,
            @pTerminal   = @pTerminal,
            @vchMensaje  = @vchMensaje OUTPUT;
    END TRY
    BEGIN CATCH
        UPDATE t
           SET estado_vk='ERROR',
               mensaje_error_vk=LEFT(CONCAT('EXCEPTION disparando SGM: ', ERROR_MESSAGE()), 255),
               f_procesado_vk=SYSDATETIME()
        FROM [repl].[TRANSF_CONNEXA_IN] t
        INNER JOIN @Work w ON w.id=t.id;

        SELECT claimed=@Claimed, processed=0, elapsed_s=DATEDIFF(SECOND, @StartTime, SYSDATETIME());
        RETURN 0;
    END CATCH

    -------------------------------------------------------------------------
    -- 4) CONCILIACIÓN CONTRA SGM:
    --    Si en T058 existe el registro (PK) y está M_ENVIADO='S' => PROCESADO
    --    Nota: el SP nativo filtra ORIG=41; eso ya está en su WHERE.
    -------------------------------------------------------------------------
    DECLARE @Sent TABLE (
        c_articulo  DECIMAL(6,0) NOT NULL,
        c_sucu_dest DECIMAL(3,0) NOT NULL,
        c_sucu_orig DECIMAL(3,0) NOT NULL,
        PRIMARY KEY (c_articulo, c_sucu_dest, c_sucu_orig)
    );

    INSERT INTO @Sent(c_articulo, c_sucu_dest, c_sucu_orig)
    SELECT w.c_articulo, w.c_sucu_dest, w.c_sucu_orig
    FROM @Work w
    WHERE EXISTS (
        SELECT 1
        FROM [DIARCOP001].[DiarcoP].[dbo].[T058_ARTICULOS_TRANSF_PEND] p
        WHERE p.C_ARTICULO  = w.c_articulo
          AND p.C_SUCU_DEST = w.c_sucu_dest
          AND p.C_SUCU_ORIG = w.c_sucu_orig
          AND p.M_ENVIADO = 'S'
    );

    -- Marcar PROCESADO en DMZ para los conciliados
    UPDATE t
       SET estado_vk='PROCESADO',
           mensaje_error_vk='',
           f_procesado_vk=SYSDATETIME()
    FROM [repl].[TRANSF_CONNEXA_IN] t
    INNER JOIN @Work w ON w.id=t.id
    INNER JOIN @Sent s
      ON s.c_articulo=w.c_articulo AND s.c_sucu_dest=w.c_sucu_dest AND s.c_sucu_orig=w.c_sucu_orig;

    -- Los no conciliados: ERROR (o PENDIENTE según política)
    UPDATE t
       SET estado_vk='ERROR',
           mensaje_error_vk=LEFT(
               COALESCE(NULLIF(@vchMensaje,''), 'No quedó marcado como enviado en SGM (T058.M_ENVIADO<>S o no existe).'),
               255
           ),
           f_procesado_vk=SYSDATETIME()
    FROM [repl].[TRANSF_CONNEXA_IN] t
    INNER JOIN @Work w ON w.id=t.id
    LEFT JOIN @Sent s
      ON s.c_articulo=w.c_articulo AND s.c_sucu_dest=w.c_sucu_dest AND s.c_sucu_orig=w.c_sucu_orig
    WHERE s.c_articulo IS NULL;

    SELECT @Processed = COUNT(1) FROM @Sent;

    -------------------------------------------------------------------------
    -- 5) Revertir “colgados” defensivo
    -------------------------------------------------------------------------
    UPDATE t
       SET estado_vk='PENDIENTE'
    FROM [repl].[TRANSF_CONNEXA_IN] t
    INNER JOIN @Work w ON w.id=t.id
    WHERE t.estado_vk='EN_PROCESO'
      AND t.f_procesado_vk IS NULL;

    SELECT claimed=@Claimed, processed=@Processed, elapsed_s=DATEDIFF(SECOND, @StartTime, SYSDATETIME());
    RETURN 0;
END

GO
