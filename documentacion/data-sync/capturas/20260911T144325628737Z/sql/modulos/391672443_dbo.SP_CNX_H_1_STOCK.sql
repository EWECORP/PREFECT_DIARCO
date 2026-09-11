-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
--exec  [dbo].[SP_KIKKER_H_1_MOVIMIENTOS_DIARIOS_HISTORICOS] '2022-08-01', '2022-08-22'
CREATE PROCEDURE [dbo].[SP_CNX_H_1_STOCK] @anio char(4), @mes char(2)--, @dia char(10)
--[dbo].[SP_KIKKER_H_1_STOCK] '2021', '05'

AS
BEGIN
	SET NOCOUNT ON;
	
SELECT 
C_ARTICULO,
C_SUCU_EMPR,
cast(C_ANIO as varchar(10))+'-'+(case  len(C_MES) when 1 then concat('0',cast(C_MES as varchar(10))) else cast(C_MES as varchar(10)) end) +'-'+(case  len(rtrim(ltrim(substring(C_DIA,6,2)))) when 1 then concat('0',cast(rtrim(ltrim(substring(C_DIA,6,2))) as varchar(10))) else cast(rtrim(ltrim(substring(C_DIA,6,2))) as varchar(10)) end)  as F_DIA,
STOCK
into ##stock_historicos
FROM   
(SELECT 
C_ANIO,
C_MES, 
C_ARTICULO,
case C_SUCU_EMPR when 41 then '41CD' else convert(varchar,DBO.[NORMALIZA_STRING](C_SUCU_EMPR)) end as C_SUCU_EMPR,
convert(varchar,Q_DIA1) as Q_DIA1,
convert(varchar,Q_DIA2) as Q_DIA2,
convert(varchar,Q_DIA3) as Q_DIA3,
convert(varchar,Q_DIA4) as Q_DIA4,
convert(varchar,Q_DIA5) as Q_DIA5,
convert(varchar,Q_DIA6) as Q_DIA6,
convert(varchar,Q_DIA7) as Q_DIA7,
convert(varchar,Q_DIA8) as Q_DIA8,
convert(varchar,Q_DIA9) as Q_DIA9,
convert(varchar,Q_DIA10) as Q_DIA10,
convert(varchar,Q_DIA11) as Q_DIA11,
convert(varchar,Q_DIA12) as Q_DIA12,
convert(varchar,Q_DIA13) as Q_DIA13,
convert(varchar,Q_DIA14) as Q_DIA14,
convert(varchar,Q_DIA15) as Q_DIA15,
convert(varchar,Q_DIA16) as Q_DIA16,
convert(varchar,Q_DIA17) as Q_DIA17,
convert(varchar,Q_DIA18) as Q_DIA18,
convert(varchar,Q_DIA19) as Q_DIA19,
convert(varchar,Q_DIA20) as Q_DIA20,
convert(varchar,Q_DIA21) as Q_DIA21,
convert(varchar,Q_DIA22) as Q_DIA22,
convert(varchar,Q_DIA23) as Q_DIA23,
convert(varchar,Q_DIA24) as Q_DIA24,
convert(varchar,Q_DIA25) as Q_DIA25,
convert(varchar,Q_DIA26) as Q_DIA26,
convert(varchar,Q_DIA27) as Q_DIA27,
convert(varchar,Q_DIA28) as Q_DIA28,
convert(varchar,Q_DIA29) as Q_DIA29,
convert(varchar,Q_DIA30) as Q_DIA30,
convert(varchar,Q_DIA31) as Q_DIA31
from  [DIARCOP001].[DIARCOP].dbo.T710_estadis_stock
where 
C_ARTICULO in (select C_ARTICULO from ##surtido) and
C_ANIO=@anio 
AND C_MES=@mes 
and C_SUCU_EMPR<>300 and (Q_DIA1+Q_DIA2+Q_DIA3+Q_DIA4+Q_DIA5+Q_DIA6+Q_DIA7+Q_DIA8+Q_DIA9+Q_DIA10+Q_DIA11+Q_DIA12+Q_DIA13+Q_DIA14+Q_DIA15+Q_DIA16+Q_DIA17+Q_DIA18+Q_DIA19+Q_DIA20+Q_DIA21+Q_DIA21+Q_DIA22+Q_DIA23+Q_DIA24+Q_DIA25+Q_DIA26+Q_DIA27+q_dia28+q_dia29+Q_dia30+Q_DIA31)<>0
) as PRECIOS
UNPIVOT  
(STOCK FOR C_DIA IN (Q_DIA1,Q_DIA2,Q_DIA3,Q_DIA4,Q_DIA5,Q_DIA6,Q_DIA7,Q_DIA8,Q_DIA9,Q_DIA10,Q_DIA11,Q_DIA12,Q_DIA13,Q_DIA14,Q_DIA15,Q_DIA16,Q_DIA17,
Q_DIA18,Q_DIA19,Q_DIA20,Q_DIA21,Q_DIA22,Q_DIA23,Q_DIA24,Q_DIA25,Q_DIA26,Q_DIA27,Q_DIA28,Q_DIA29,Q_DIA30,Q_DIA31)
) AS unpvt
--where cast(C_ANIO as varchar(10))+'-'+(case  len(C_MES) when 1 then concat('0',cast(C_MES as varchar(10))) else cast(C_MES as varchar(10)) end) +'-'+(case  len(rtrim(ltrim(substring(C_DIA,6,2)))) when 1 then concat('0',cast(rtrim(ltrim(substring(C_DIA,6,2))) as varchar(10))) else cast(rtrim(ltrim(substring(C_DIA,6,2))) as varchar(10)) end) >= @dia

END

GO
