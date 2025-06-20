USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_T_4_PRODUCTOS_LOJA]    Script Date: 19/06/2025 15:26:41 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_T_4_PRODUCTOS_LOJA]

AS

BEGIN

SET NOCOUNT ON;

/*Obtener artículos del mix*/

/*select distinct c_articulo into #surtido from  [DIARCOP001].[DIARCOP].dbo.T710_estadis_stock
where C_ANIO>=2022and C_MES>=1 and 
C_SUCU_EMPR<>300 */
 select distinct c_articulo into #surtido from [DIARCOP001].[DIARCOP].DBO.t060_STOCK 


select C_SUCU_EMPR,C_ARTICULO,M_HABILITADO_SUCU
into #marca_nada_barrio
from [diarco-barrio].[diarcobarrio].dbo.T051_ARTICULOS_SUCURSAL
where C_ARTICULO in (select C_ARTICULO from #surtido) 

/*Obtener fechas de vigencia de los artículos a nivel sucursales*/
select  distinct suc.C_SUCU_EMPR as C_SUCU_EMPR, 
suc.C_ARTICULO as C_ARTICULO, 
case when 
	(max(case M_LISTO_PARA_VENTA_ACT when 'S' then replace(convert(varchar,F_ALTA_SIST,111),'/','-') end) > max(replace(convert(varchar,suc.F_ALTA,111),'/','-'))) 
		then max(case M_LISTO_PARA_VENTA_ACT when 'S' then replace(convert(varchar,F_ALTA_SIST,111),'/','-') end)  
		else max(replace(convert(varchar,suc.F_ALTA,111),'/','-')) end  as ALTA_VIGENCIA,
isnull(case when ((case when 
	(max(case M_LISTO_PARA_VENTA_ACT when 'S' then replace(convert(varchar,F_ALTA_SIST,111),'/','-') end) > max(replace(convert(varchar,suc.F_ALTA,111),'/','-'))) 
		then max(case M_LISTO_PARA_VENTA_ACT when 'S' then replace(convert(varchar,F_ALTA_SIST,111),'/','-') end)  
		else max(replace(convert(varchar,suc.F_ALTA,111),'/','-')) end) > max(case M_LISTO_PARA_VENTA_ACT when 'N' then replace(convert(varchar,F_ALTA_SIST,111),'/','-') end))
	then '' else max(case M_LISTO_PARA_VENTA_ACT when 'N' then replace(convert(varchar,F_ALTA_SIST,111),'/','-') end) end,case when max(M_HABILITADO_SUCU)='N' then max(replace(convert(varchar,suc.F_ALTA,111),'/','-')) else  '' end) as BAJA_VIGENCIA

--isnull(max(case M_LISTO_PARA_VENTA_ACT when 'N' then replace(convert(varchar,F_ALTA_SIST,111),'/','-') end),'2999-12-31') as BAJA_VIGENCIA
into #vigentes
from [DIARCOP001].[DIARCOP].dbo.T051_ARTICULOS_SUCURSAL suc
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos art on art.C_ARTICULO=SUC.C_ARTICULO
left join [DIARCOP001].[DIARCOP].[dbo].[T804_HIST_MARCA_LISTO_PARA_VENTA] Hist on suc.c_sucu_empr=Hist.c_sucu_empr and suc.c_Articulo=Hist.c_Articulo
where Suc.C_SUCU_EMPR<>300 and  art.M_BAJA='N' 
group by suc.C_SUCU_EMPR,suc.C_ARTICULO
order by 1,2 desc;

SELECT 'COD_LOJA','COD_PRODUTO','ABASTECIMENTO','COD_CD','LINHA','DATA_CADASTRO','DATA_SAIU_LINHA','UNID_TRANSFERENCIA',
'QTDE_UNID_TRANSFERENCIA','PEDIDO_MIN','FRENTE_LINEAR','CAPACID_GANDOLA','ESTOQUE_MINIMO','COD_COMPRADOR','PROMOCAO',
'ACTIVE_FOR_PURCHASE','ACTIVE_FOR_SALE','ACTIVE_ON_MIX','DELIVERED_ID','PRODUCT_BASE_ID','OWN_PRODUCTION','FULL_CAPACITY_PALLET',
'NUMBER_OF_LAYERS','NUMBER_OF_BOXES_PER_BALLAST'
--,'COD_DEPOSTO','NOME_DEPOSITO','CURVA_CLIENTE' 
-- Código do comprador no ERP, que compra esse produto para essa loja
UNION ALL
select 
case SUC.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](SUC.C_SUCU_EMPR) end,
convert(varchar,suc.c_articulo), --COD_PRODUTO
convert(varchar,C_SISTEMATICA), --SISTEMATICA
case when suc.c_sucu_empr<300 then '41CD' when suc.c_sucu_empr>300 then '82' end, -- COD_CD
case when ((suc.c_sucu_empr<300 and suc.m_habilitado_sucu='N') or (suc.c_sucu_empr>300 and mnb.m_habilitado_sucu='N')) then '0' else '1' end, --LINHA
convert(varchar,v.ALTA_VIGENCIA),--DATA_CADASTRO
convert(varchar,v.BAJA_VIGENCIA), --DATA_SAIU_LINHA
'0',--CUANDO ES CROSSDOCKING.
'1', --QTDE_UNID_TRANSFERENCIA
isnull(convert(varchar,Q_BULTOS_KILOS_COMPRA_MINIMA),'0'),
'1',
'1',
'1',
convert(varchar,ART.C_COMPRADOR),
case SUC.M_OFERTA_SUCU when 'N' then '0' else '1' end, -- promoción
/*VIEJO
case SUC.M_HABILITADO_SUCU when 'N' then '0' else '1' end, -- habilitado compra
case SUC.M_LISTO_PARA_VENTA_SUCU when 'N' then '0' else '1' end, --habilitado venta
'1',--MIX*/

case when (ART.C_FAMILIA = 4 or ART.M_A_DAR_DE_BAJA='S' or (suc.c_sucu_empr<300 and suc.m_habilitado_sucu = 'N') or (suc.c_sucu_empr>300 and mnb.m_habilitado_sucu ='N')) then '0' else '1' end, -- habilitado compra
case when SUC.M_LISTO_PARA_VENTA_SUCU='N' then '0' else '1' end, -- Habilitado para la venta
case when (ART.C_FAMILIA=4 or ART.M_A_DAR_DE_BAJA='S')  then '0' else '1' end,--Habilitado en MIX
case C_SISTEMATICA when 0 then (case when suc.c_sucu_empr<300 then '41CD' when suc.c_sucu_empr>300 then '82' end) 
				   when 1 then convert(varchar,/*ART.C_PROVEEDOR_PRIMARIO*/PROV_ART.C_PROVEEDOR) end,
'',
'0',
convert(varchar,U_PISO_PALETIZADO*U_ALTURA_PALETIZADO),
convert(varchar,U_ALTURA_PALETIZADO),
--convert(varchar,U_PISO_PALETIZADO) COMENTADO POR MAXI ARGIRO 09/08/2023 - SE REEMPLAZA PARA CALCULO DE REDONDEO CON PESABLES Y NO PESABLES EN LA SIGUIENTE LINEA.
CASE WHEN ART.M_VENDE_POR_PESO = 'N' THEN convert(varchar,U_PISO_PALETIZADO) ELSE convert (varchar,prov_ART.Q_FACTOR_PROVEEDOR)  END /*SE AGREGA ESTA LINEA PARA IDENTIFICAR LO QUE VIENE POR PESABLE EL FACTOR SON LOS KILOS, SINO ES EL FACTOR PROVEEDOR EN UNIDADES POR BULTO.*/--QTDE_UNID_COMPRA
--case when suc.c_sucu_empr<300 then '41' when suc.c_sucu_empr>300 then '82' end,
--case when SUC.C_SUCU_EMPR>300 then 'QX Deposito DB' else 'BASE II' end,
--''
from [DIARCOP001].[DIARCOP].dbo.T051_ARTICULOS_SUCURSAL suc 
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos art on art.C_ARTICULO=SUC.C_ARTICULO
left join #marca_nada_barrio mnb on mnb.C_ARTICULO=SUC.C_ARTICULO and mnb.C_SUCU_EMPR=SUC.C_SUCU_EMPR
LEFT join [DIARCOP001].[DiarcoP].[dbo].[T052_ARTICULOS_PROVEEDOR] PROV_ART on /*PROV_ART.C_PROVEEDOR=ART.C_PROVEEDOR_PRIMARIO and*/ PROV_ART.C_ARTICULO=ART.C_ARTICULO
LEFT join [DIARCOP001].[DIARCOP].dbo.T020_Proveedor_Dias_Entrega_Deta Prov on /*Prov.C_PROVEEDOR=ART.C_PROVEEDOR_PRIMARIO*/Prov.C_PROVEEDOR=PROV_ART.C_PROVEEDOR and suc.C_SUCU_EMPR=Prov.C_SUCU_EMPR
inner join [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC_MAE on SUC_MAE.C_SUCU_EMPR=suc.C_SUCU_EMPR and suc.C_SUCU_EMPR not in (6,8,14,17,39,40,300, 80, 81, 83, 84, 88) and SUC_MAE.M_SUCU_VIRTUAL = 'N'
	AND      SUC.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB])
inner join #vigentes v on v.C_ARTICULO=suc.C_ARTICULO and v.C_SUCU_EMPR=suc.C_SUCU_EMPR
where suc.C_ARTICULO in (select C_ARTICULO from #surtido) 

Drop table #vigentes,#surtido;

end;
GO


