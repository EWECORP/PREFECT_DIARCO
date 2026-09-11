-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
--exec  [dbo].[SP_KIKKER_H_1_MOVIMIENTOS_DIARIOS_HISTORICOS] '2022-08-01', '2022-08-22'
CREATE PROCEDURE [dbo].[SP_CNX_H_1_STOCK_HISTORICO] @anio char(4), @mes char(2), @dia char(10)
as -- @sucu as integer = null, @fecha as date =getdate AS
/*
COD_PRODUTO - Código do produto
COD_LOJA - Código da loja
DATA_ESTOQUE - Data do estoque (formato AAAA-MM-DD)
ESTOQUE_ATUAL - Estoque atual (unidades de venda)
--PENDENCIA_VENDA - Pendência de venda (unidades de venda) --
PEDIDO_PENDENTE - Pedidos pendentes (quantidade em unidades de venda) --pedidodes de venta de la sucur, para el articulo , sum
CUSTO_UNIT_ULT_ENTRADA - Custo unitário da Última entrada i_costo_estadistico
PRECO_UNIT_VENDA - Preço unitário de venda
PROMOCAO - Venda em promoção - (1 - promoção ou 0 não promoção)
LOTE	Nro Lote para control
VALIDADE_LOTE	Lote para control
*/
BEGIN

set nocount on;

IF OBJECT_ID('tempdb..##stock_historicos') IS NULL  
        BEGIN
         exec [dbo].[SP_KIKKER_H_1_STOCK] @anio, @mes--, @dia 
        END;
IF OBJECT_ID('tempdb..##precios_historicos') IS NULL  
        BEGIN
         exec [dbo].[SP_KIKKER_H_1_PRECIOS] @anio, @mes--, @dia 
        END;
IF OBJECT_ID('tempdb..##costos_historicos') IS NULL  
        BEGIN
         exec [dbo].[SP_KIKKER_H_1_COSTOS] @anio, @mes--, @dia 
        END;
IF OBJECT_ID('tempdb..##vigencia_promos') IS NULL  
        BEGIN
         exec [dbo].[SP_KIKKER_H_11.1_PROMOCIONES_HISTORICO_VIGENTES] @anio, @mes
		END;

-- exec [dbo].[SP_KIKKER_H_1_STOCK] '2022','07' 
--exec [dbo].[SP_KIKKER_H_1_PRECIOS] '2022','07' 
--exec [dbo].[SP_KIKKER_H_1_COSTOS] '2022','07' 
--exec [dbo].[SP_KIKKER_H_11.1_PROMOCIONES_HISTORICO_VIGENTES] '2022','07'
/*calculo de las unidades pendientes de entrega por sucursal y articulo*/
select 
case OC_CABE.C_SUCU_COMPRA when 41 then '41CD' else DBO.[NORMALIZA_STRING](OC_CABE.C_SUCU_COMPRA) end as SUCU_COMPRA,
OC_DETA.C_ARTICULO as C_ARTICULO,
sum((OC_DETA.Q_BULTOS_PROV_PED*OC_DETA.Q_FACTOR_PROV_PED)-OC_DETA.Q_UNID_CUMPLIDAS) as Pendientes
into #pedidos_pendientes
from  [DIARCOP001].[DIARCOP].DBO.T080_OC_CABE OC_CABE 
inner join [DIARCOP001].[DIARCOP].DBO.T081_OC_DETA OC_DETA  on OC_DETA.C_OC = OC_CABE.C_OC 
															AND OC_DETA.U_PREFIJO_OC = OC_CABE.U_PREFIJO_OC 
															AND OC_DETA.U_SUFIJO_OC = OC_CABE.U_SUFIJO_OC 
WHERE ((OC_DETA.Q_BULTOS_PROV_PED*OC_DETA.Q_FACTOR_PROV_PED)  - OC_DETA.Q_UNID_CUMPLIDAS) <> 0	AND  OC_CABE.C_SITUAC = 1
and OC_CABE.C_SUCU_COMPRA<>300
AND	F_ALTA_SIST >='01/01/2021' and F_ALTA_SIST<=@dia 
group by 
OC_CABE.C_SUCU_COMPRA,
OC_DETA.C_ARTICULO

SELECT 'COD_PRODUTO','COD_LOJA','DATA_ESTOQUE','ESTOQUE_ATUAL','PENDENCIA_VENDA','PEDIDO_PENDIENTE','CUSTO_UNIT_ULT_ENTRADA','PRECO_UNIT_VENDA','PROMOCAO','LOTE','VALIDADE_LOTE'
UNION ALL
select 
convert(varchar,STK.C_ARTICULO),
STK.C_SUCU_EMPR,
STK.F_DIA,
--CASE WHEN ART.M_VENDE_POR_PESO='N' then DBO.[NORMALIZA_STRING](STK.Q_UNID_ARTICULO) when ART.M_VENDE_POR_PESO='S' then DBO.[NORMALIZA_STRING](STK.Q_PESO_ARTICULO) end,
isnull(STK.STOCK,'0'),
isnull(convert(varchar,DBO.[NORMALIZA_STRING](PP.Pendientes)),'0'),
'',
convert(varchar,isnull(COSTO.L_PRECIOS,'0')),
convert(varchar,isnull(ART_SUC.L_PRECIOS,'0')),
case when VP.F_BAJA is not null then '1' else '0' end,
'',
''
from ##stock_historicos STK
left join #pedidos_pendientes PP on  PP.C_ARTICULO=STK.C_ARTICULO and convert(varchar,PP.SUCU_COMPRA)=convert(varchar,STK.C_SUCU_EMPR)
inner join  ##costos_historicos COSTO on COSTO.C_ARTICULO=STK.C_ARTICULO and convert(varchar,COSTO.C_SUCU_EMPR)=convert(varchar,STK.C_SUCU_EMPR)  and COSTO.F_PRECIO=STK.F_DIA
inner join  ##precios_historicos ART_SUC ON ART_SUC.C_ARTICULO=STK.C_ARTICULO and convert(varchar,ART_SUC.C_SUCU_EMPR)=convert(varchar,STK.C_SUCU_EMPR)  and ART_SUC.F_PRECIO=STK.F_DIA
inner join [DIARCOP001].[DIARCOP].DBO.T050_ARTICULOS ART on ART.C_ARTICULO=STK.C_ARTICULO  --and  ART.C_PROVEEDOR_PRIMARIO=COSTO.C_PROVEEDOR
left join ##vigencia_promos VP on VP.C_ARTICULO=STK.C_ARTICULO and convert(varchar,VP.C_SUCU_EMPR)=convert(varchar,STK.C_SUCU_EMPR) and STK.F_DIA>=VP.F_ALTA and STK.F_DIA<=VP.F_BAJA
where convert(varchar,STK.C_SUCU_EMPR)<>'300'  and STK.C_ARTICULO in (select C_ARTICULO from ##surtido);

drop table ##precios_historicos,##stock_historicos,##costos_historicos,##vigencia_promos,#pedidos_pendientes

END;

GO
