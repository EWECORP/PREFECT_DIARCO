-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE                            PROCEDURE [dbo].[SP_GENERA_BASE_PRODUCTOS_VIGENTES]

AS
BEGIN
    SET NOCOUNT ON;
	/*==========================
	  1) Parámetros de ejecución
	==========================*/
	DECLARE
		@p_c_sucu_empr            INT  = NULL,  -- por sucursal (NULL = todas)
		@p_c_familia              INT  = NULL,  -- por familia (NULL = todas)
		@p_incluir_no_habilitados BIT  = 0;     -- 1 = incluye no habilitados

	/*=============================================
	  2) Recrear tabla de depuración (idempotente)
	=============================================*/
	IF OBJECT_ID('repl.BASE_PRODUCTOS_VIGENTES','U') IS NOT NULL
		DROP TABLE repl.BASE_PRODUCTOS_VIGENTES;

	/*==========================================================
	  3) Crear tabla con esquema compatible con la salida del SP
	==========================================================*/
	CREATE TABLE repl.BASE_PRODUCTOS_VIGENTES
	(
		C_SUCU_EMPR                 INT               NOT NULL,
		C_ARTICULO                  INT               NOT NULL,
		C_PROVEEDOR_PRIMARIO        INT               NULL,

		ABASTECIMIENTO              INT               NULL,           -- proviene de C_SISTEMATICA ('0','1','2','3')
		COD_CD                      NVARCHAR(32)      NULL,           -- '41CD','82CD','XDOC' o '000' (FORMAT)
		HABILITADO                  BIT               NOT NULL,       -- '0'/'1' -> BIT

		FECHA_REGISTRO              DATETIME          NULL,
		FECHA_BAJA                  DATETIME          NULL,

		UNID_TRANSFERENCIA          INT               NULL,           -- '0'
		Q_UNID_TRANSFERENCIA        INT               NULL,           -- '1'
		PEDIDO_MIN                  DECIMAL(18,6)     NULL,           -- numérico en texto
		FRENTE_LINEAL               INT               NULL,           -- '1'
		CAPACID_GONDOLA             INT               NULL,           -- '1'
		STOCK_MINIMO                INT               NULL,           -- '1'
		COD_COMPRADOR               INT               NULL,

		PROMOCION                   BIT               NOT NULL,       -- '0'/'1'
		ACTIVE_FOR_PURCHASE         BIT               NOT NULL,       -- '0'/'1'
		ACTIVE_FOR_SALE             BIT               NOT NULL,       -- '0'/'1'
		ACTIVE_ON_MIX               BIT               NOT NULL,       -- '0'/'1'

		DELIVERED_ID                NVARCHAR(32)      NULL,           -- '41CD','82' o código proveedor
		PRODUCT_BASE_ID             NVARCHAR(100)     NULL,           -- ''
		OWN_PRODUCTION              BIT               NOT NULL,       -- '0'

		Q_FACTOR_COMPRA             DECIMAL(18,6)     NULL,           -- prov.Q_FACTOR_PROVEEDOR
		FULL_CAPACITY_PALLET        DECIMAL(18,6)     NULL,           -- U_PISO * U_ALTURA (texto)
		NUMBER_OF_LAYERS            INT               NULL,           -- U_ALTURA_PALETIZADO
		NUMBER_OF_BOXES_PER_LAYER   DECIMAL(18,6)     NULL,           -- U_PISO o Q_FACTOR_PROVEEDOR

		-- Metadato útil para trazabilidad de la corrida:
		fecha_extraccion            DATETIME2(0)      NOT NULL
			CONSTRAINT DF_BASE_PRODUCTOS_VIGENTES__fecha_extraccion DEFAULT (SYSDATETIME())
	);

	/*========================================
	  5) Ejecutar el SP y volcar resultados
		 (se omite fecha_extraccion para usar DEFAULT)
	========================================*/
	INSERT INTO repl.BASE_PRODUCTOS_VIGENTES
	(
		C_SUCU_EMPR,
		C_ARTICULO,
		C_PROVEEDOR_PRIMARIO,
		ABASTECIMIENTO,
		COD_CD,
		HABILITADO,
		FECHA_REGISTRO,
		FECHA_BAJA,
		UNID_TRANSFERENCIA,
		Q_UNID_TRANSFERENCIA,
		PEDIDO_MIN,
		FRENTE_LINEAL,
		CAPACID_GONDOLA,
		STOCK_MINIMO,
		COD_COMPRADOR,
		PROMOCION,
		ACTIVE_FOR_PURCHASE,
		ACTIVE_FOR_SALE,
		ACTIVE_ON_MIX,
		DELIVERED_ID,
		PRODUCT_BASE_ID,
		OWN_PRODUCTION,
		Q_FACTOR_COMPRA,
		FULL_CAPACITY_PALLET,
		NUMBER_OF_LAYERS,
		NUMBER_OF_BOXES_PER_LAYER
	)
	EXEC dbo.SP_BASE_PRODUCTOS_SUCURSAL
		@C_SUCU_EMPR            = @p_c_sucu_empr,
		@C_FAMILIA              = @p_c_familia,
		@INCLUIR_NO_HABILITADOS = @p_incluir_no_habilitados;

END

GO
