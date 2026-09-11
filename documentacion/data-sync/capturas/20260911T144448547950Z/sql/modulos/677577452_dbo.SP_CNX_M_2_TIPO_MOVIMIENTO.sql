-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [dbo].[SP_CNX_M_2_TIPO_MOVIMIENTO]
AS 
BEGIN 
    SET NOCOUNT ON;
    
	TRUNCATE TABLE M_2_MOVIMIENTOS;

    -- Insertar los datos directamente en la tabla M_2_MOVIMIENTOS, incluyendo la fecha en F_PROC
    INSERT INTO M_2_MOVIMIENTOS (
        COD_TIPO_MOVIMIENTO,
        DESCRIP_TIPO_MOVIMIENTO,
        TIPO_OPERACION,
        SIGNO,
		F_DATO,
        F_PROC
    )
    SELECT DISTINCT
        CASE 
            WHEN c_tabla = 44 THEN CAST(c_tabla AS VARCHAR(5))  -- 3 - Por devolución
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 75 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(C_CODIGO_TABLA AS VARCHAR(5))  -- 3 - Por devolución
            WHEN c_tabla = 23 THEN CAST(c_tabla AS VARCHAR(5))  -- 4 - Ajuste de Inventario Positivo
            WHEN c_tabla = 24 THEN CAST(c_tabla AS VARCHAR(5))  -- 4 - Ajuste de Inventario Negativo
            WHEN c_tabla = 106 THEN CAST(c_tabla AS VARCHAR(5))  -- 5 - Desperdicio o Merma
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 77 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(C_CODIGO_TABLA AS VARCHAR(5))  -- 5 - Desperdicio o Merma
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 74 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(C_CODIGO_TABLA AS VARCHAR(5))  -- 7 - Transferencia ingreso
            WHEN c_tabla = 0 AND C_CODIGO_TABLA = 10 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(C_CODIGO_TABLA AS VARCHAR(5))  -- 2 - Recibido en la tienda
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 73 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(C_CODIGO_TABLA AS VARCHAR(5))  -- 7 - Transferencia salida
        END AS COD_TIPO_MOVIMIENTO,
        CASE 
            WHEN c_tabla = 44 THEN 'Devolución al Proveedor'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 75 THEN 'Devolución al Proveedor'
            WHEN c_tabla = 23 THEN 'Ajuste de Inventario'
            WHEN c_tabla = 24 THEN 'Ajuste de Inventario'
            WHEN c_tabla = 106 THEN 'Desperdicio o Merma'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 77 THEN 'Desperdicio o Merma'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 74 THEN 'Transferencia'
            WHEN c_tabla = 0 AND C_CODIGO_TABLA = 10 THEN 'Recibido en la tienda'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 73 THEN 'Transferencia'
        END AS DESCRIP_TIPO_MOVIMIENTO,
        CASE 
            WHEN c_tabla = 44 THEN '3'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 75 THEN '3'
            WHEN c_tabla = 23 THEN '4'
            WHEN c_tabla = 24 THEN '4'
            WHEN c_tabla = 106 THEN '5'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 77 THEN '5'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 74 THEN '7'
            WHEN c_tabla = 0 AND C_CODIGO_TABLA = 10 THEN '2'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 73 THEN '7'
        END AS TIPO_OPERACION,
        CASE 
            WHEN c_tabla = 44 THEN '+'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 75 THEN '+'
            WHEN c_tabla = 23 THEN '+'
            WHEN c_tabla = 24 THEN '-'
            WHEN c_tabla = 106 THEN '+'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 77 THEN '+'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 74 THEN '+'
            WHEN c_tabla = 0 AND C_CODIGO_TABLA = 10 THEN '+'
            WHEN c_tabla = 4 AND C_CODIGO_TABLA = 73 THEN '+'
        END AS SIGNO,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
        
    FROM [DIARCOP001].[DiarcoP].dbo.t001_tabla_codigo 
    WHERE c_tabla IN (44, 23, 24, 106, 4, 0)

    UNION ALL

    SELECT '99', 'Venta', '1', '+', GETDATE() , GETDATE()  -- 1 - Por Venta
    UNION ALL
    SELECT '010', 'Recibido en la tienda', '2', '+',  GETDATE() , GETDATE()  -- 2 - Recibido en la tienda
    UNION ALL
    SELECT '4373', 'Transferencia', '7', '+',  GETDATE() , GETDATE()
    UNION ALL
    SELECT '4374', 'Transferencia', '7', '+',  GETDATE() , GETDATE();

END;

GO
