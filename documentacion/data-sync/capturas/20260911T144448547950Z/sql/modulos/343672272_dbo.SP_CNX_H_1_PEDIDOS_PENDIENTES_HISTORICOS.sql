-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
--exec  [dbo].[SP_KIKKER_H_1_MOVIMIENTOS_DIARIOS_HISTORICOS] '2022-08-01', '2022-08-22'
CREATE PROCEDURE [dbo].[SP_CNX_H_1_PEDIDOS_PENDIENTES_HISTORICOS] @fecha char(10)

AS
/*
8. diario de pedidos pendiente
q_unid_gramos_saldo t081_oc_deta / t081_oc_deta_DBARRIO

DATA_EMISSAO - Data da emissão pedido (formato AAAA-MM-DD)
DATA_ENTREGA - Data prevista para entrega do pedido (formato AAAA-MM-DD)
COD_PRODUTO - Código do produto comprado
COD_LOJA - Código da loja
QTDE_PEDIDA - Quantidade pedida para esse produto (unidade de venda)
NUMERO_DO_PEDIDO - Número do pedido que esse produto faz parte
COD_FORNCEDOR	Código Proveedor
*/
--exec [dbo].[SP_KIKKER_H_1_PEDIDOS_PENDIENTES_HISTORICOS] '22/08/2022'

BEGIN
SET NOCOUNT ON;


   SELECT 'DATA_EMISSAO','DATA_ENTREGA','COD_PRODUTO','COD_LOJA','QTDE_PEDIDA','NUMERO_DO_PEDIDO','COD_FORNCEDOR'
   UNION ALL
	SELECT 
	REPLACE(CONVERT(VARCHAR,CABE.F_EMISION,111),'/','-'),
	REPLACE(CONVERT(VARCHAR,CABE.F_ENTREGA,111),'/','-'),
	CONVERT(VARCHAR,DETA.C_ARTICULO),
	CONVERT(VARCHAR,case CABE.C_SUCU_DESTINO when 41 then '41CD' else CABE.C_SUCU_DESTINO end),
	CONVERT(VARCHAR,(DETA.Q_BULTOS_PROV_PED*DETA.Q_FACTOR_PROV_PED)  - DETA.Q_UNID_CUMPLIDAS),
	right('0000'+CONVERT(VARCHAR,DETA.U_PREFIJO_OC),4) + right('0000'+convert(varchar,deta.U_SUFIJO_OC),5),
	CONVERT(VARCHAR,CABE.C_PROVEEDOR)
	FROM	[DIARCOP001].[DIARCOP].dbo.T080_OC_CABE CABE 
			INNER JOIN [DIARCOP001].[DIARCOP].dbo.t081_oc_deta DETA ON CABE.U_SUFIJO_OC=DETA.U_SUFIJO_OC 
					AND CABE.U_PREFIJO_OC=DETA.U_PREFIJO_OC 
					AND CABE.C_OC=DETA.C_OC 
	WHERE ((DETA.Q_BULTOS_PROV_PED*DETA.Q_FACTOR_PROV_PED)  - DETA.Q_UNID_CUMPLIDAS) <> 0		
	AND  CABE.C_SITUAC = 1  
	AND	F_ALTA_SIST >='2021-01-01' and F_ALTA_SIST<=@fecha ;
	--AND F_ENTREGA > @fecha*/;

END;




GO
