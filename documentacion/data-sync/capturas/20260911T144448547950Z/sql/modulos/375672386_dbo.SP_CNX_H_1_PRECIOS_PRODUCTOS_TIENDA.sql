-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
--exec  [dbo].[SP_KIKKER_H_1_MOVIMIENTOS_DIARIOS_HISTORICOS] '2022-08-01', '2022-08-22'
CREATE PROCEDURE [dbo].[SP_CNX_H_1_PRECIOS_PRODUCTOS_TIENDA] @anio char(4), @mes char(2)--, @dia char(10)
AS 

/*DATA	Fecha
COD_PRODUTO 	Código Producto
COD_LOJA 	Código Tienda
PRECO_UNIT_VAREJO  	Precio Unitario al Por menor
PRECO_UNIT_ATACADO  	Precio unitario Mayorista
PRECO_PROMOCIONAL  	Precio Promocional*/

BEGIN
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..##precios_historicos') IS NULL  
        BEGIN
         exec [dbo].[SP_KIKKER_H_1_PRECIOS] @anio, @mes--, @dia 
        END;

select 'DATA','COD_PRODUTO','COD_LOJA','PRECO_UNIT_VAREJO','PRECO_UNIT_ATACADO','PRECO_PROMOCIONAL' union all
select 
F_PRECIO,
CAST(ART_SUC.C_ARTICULO AS VARCHAR (20)) ,--Codigo Articulo
CAST(case ART_SUC.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](ART_SUC.C_SUCU_EMPR) end AS VARCHAR (20)), --Sucursal
CAST(CAST(ART_SUC.L_PRECIOS*(1+isnull(PAR.K_RECAR_VTA_ESP,0)) AS DECIMAL (10,2)) AS VARCHAR(20)),--Precio unitario al por menor. Usa factor por venta por menor volumen
CAST(ART_SUC.L_PRECIOS AS VARCHAR (20)), --Percio Mayorista
CAST(ART_SUC.L_PRECIOS AS VARCHAR (20)) --Precio Promocional
from ##precios_historicos ART_SUC
inner join [DIARCOP001].[DIARCOP].dbo.T050_ARTICULOS ART ON ART.C_ARTICULO=ART_SUC.C_ARTICULO 
left join [DIARCOP001].[DIARCOP].dbo.T230_FACTURADOR_PARAM_FLIA PAR on PAR.C_FAMILIA=ART.C_FAMILIA  AND PAR.C_SUCU_EMPR=ART_SUC.C_SUCU_EMPR
 where ART_SUC.C_SUCU_EMPR<300 and ART_SUC.L_PRECIOS<>0 and ART_SUC.C_ARTICULO in (select C_ARTICULO from ##surtido)
/*where AND PV.M_VIGENTE='S'*/
UNION ALL
select --DIARCO-BARRIO
F_PRECIO,
CAST(ART_SUC.C_ARTICULO AS VARCHAR (20)), --Codigo Articulo
CAST(case ART_SUC.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](ART_SUC.C_SUCU_EMPR) end AS VARCHAR (20)), --Sucursal
CAST(CAST(ART_SUC.L_PRECIOS*(1+isnull(PAR.K_RECAR_VTA_ESP,0)) AS DECIMAL (10,2)) AS VARCHAR(20)),--Precio unitario al por menor. Usa factor por venta por menor volumen
CAST(ART_SUC.L_PRECIOS AS VARCHAR (20)), --Percio Mayorista
CAST(ART_SUC.L_PRECIOS AS VARCHAR (20)) --Precio Promocional
from ##precios_historicos  ART_SUC
inner join [DIARCO-BARRIO].[DIARCOBARRIO].dbo.T050_ARTICULOS ART ON ART.C_ARTICULO=ART_SUC.C_ARTICULO 
left join [DIARCO-BARRIO].[DIARCOBARRIO].dbo.T230_FACTURADOR_PARAM_FLIA PAR on PAR.C_FAMILIA=ART.C_FAMILIA AND PAR.C_SUCU_EMPR=ART_SUC.C_SUCU_EMPR
where  ART_SUC.C_SUCU_EMPR>300 and ART_SUC.L_PRECIOS<>0 and ART_SUC.C_ARTICULO in (select C_ARTICULO from ##surtido)
/*where AND PV.M_VIGENTE='S'*/;
end;


GO
