-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO




CREATE      PROCEDURE [repl].[usp_replicar_T_COMPETENCIA_DETALLE]
    @FechaDesde datetime = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Si no viene fecha, calcular 1er día de hace 2 meses
    IF @FechaDesde IS NULL  
    BEGIN  
        SET @FechaDesde = DATEFROMPARTS(
                            YEAR(DATEADD(MONTH, -2, GETDATE())),
                            MONTH(DATEADD(MONTH, -2, GETDATE())),
                            1
                          );
    END

    -- Limpiar tabla destino
    IF EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'[dbo].[T_COMPETENCIA_DETALLE]') 
                 AND type = 'U')
        TRUNCATE TABLE [repl].[T_COMPETENCIA_DETALLE];

    -- Insertar datos
    INSERT INTO [repl].[T_COMPETENCIA_DETALLE]
           ([U_ANIO],[U_SEMANA],[C_SUCU_EMPR],[C_ARTICULO],[C_PROVEEDOR_PRIMARIO],
            [N_ARTICULO],[I_COSTO_ESTADISTICO],[I_PRECIO_VTA],[MG%],
            [C_COMPETIDOR],[N_COMPETIDOR],[I_PRECIO_COMPETIDOR],[MG2%],[F_ALTA])
    SELECT 
        A.U_ANIO, A.U_SEMANA, A.C_SUCU_EMPR, A.C_ARTICULO, A.C_PROVEEDOR_PRIMARIO,
        B.N_ARTICULO, A.I_COSTO_ESTADISTICO, A.I_PRECIO_VTA,
        (A.I_PRECIO_VTA - A.I_COSTO_ESTADISTICO) / NULLIF(A.I_COSTO_ESTADISTICO, 0) * 100 AS [MG%],
        A.C_COMPETIDOR, C.N_COMPETIDOR, A.I_PRECIO_COMPETIDOR,
        (A.I_PRECIO_COMPETIDOR - A.I_COSTO_ESTADISTICO) / NULLIF(A.I_COSTO_ESTADISTICO, 0) * 100 AS [MG2%],
        A.F_ALTA
    FROM [DIARCOP001].[DiarcoP].dbo.T091_COMPETENCIA_PRECIOS_DETA A
    INNER JOIN [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS B 
        ON A.C_ARTICULO = B.C_ARTICULO
    INNER JOIN [DIARCOP001].[DiarcoP].dbo.T090_COMPETENCIA C 
        ON A.C_COMPETIDOR = C.C_COMPETIDOR
    WHERE A.F_ALTA > @FechaDesde;
END;

GO
