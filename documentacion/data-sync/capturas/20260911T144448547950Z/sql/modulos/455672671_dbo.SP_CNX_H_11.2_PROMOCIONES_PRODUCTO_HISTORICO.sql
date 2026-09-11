-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE PROCEDURE [dbo].[SP_CNX_H_11.2_PROMOCIONES_PRODUCTO_HISTORICO] @anio char(4), @mes char(2)--, @fecha char(10)
AS 

/*CODIGO_PROMOCAO 	Código Promoción
COD_PRODUTO 	Código Producto
COD_LOJA	Código Tienda
VALOR_PROMOCIONAL	Precio producto en promoción*/
SET NOCOUNT ON;

BEGIN

IF OBJECT_ID('tempdb..##precios_historicos') IS NULL  
        BEGIN
         exec [dbo].[SP_KIKKER_H_1_PRECIOS] @anio, @mes --,@fecha
        END;
IF OBJECT_ID('tempdb..##vigencia_promos') IS NULL  
        BEGIN
         exec dbo.[SP_KIKKER_H_11.1_PROMOCIONES_HISTORICO_VIGENTES] @anio, @mes
        END;


	
Select 'CODIGO_PROMOCAO','COD_PRODUTO','COD_LOJA','VALOR_PROMOCIONAL' union all
select 
distinct  
cast(ART_SUC.C_ARTICULO as varchar(5)),
cast(ART_SUC.C_ARTICULO as varchar(5)),
case ART_SUC.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](ART_SUC.C_SUCU_EMPR) end as C_SUCU_EMPR,
cast(PH.L_PRECIOS as varchar(10))
from  ##vigencia_promos ART_SUC
inner join ##precios_historicos PH on PH.C_ARTICULO=ART_SUC.C_ARTICULO and PH.C_SUCU_EMPR=ART_SUC.C_SUCU_EMPR
where ART_SUC.C_ARTICULO in (select C_ARTICULO from ##surtido)


END;

--exec [dbo].[SP_KIKKER_H_11.2_PROMOCIONES_PRODUCTO_HISTORICO] '2022', '07', '2022-07-15'


GO
