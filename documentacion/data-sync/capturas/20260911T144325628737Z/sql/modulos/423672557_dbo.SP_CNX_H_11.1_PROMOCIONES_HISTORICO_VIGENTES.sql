-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
--exec  [dbo].[SP_KIKKER_H_1_MOVIMIENTOS_DIARIOS_HISTORICOS] '2022-08-01', '2022-08-22'
CREATE PROCEDURE [dbo].[SP_CNX_H_11.1_PROMOCIONES_HISTORICO_VIGENTES] @anio char(4), @mes char(2)
AS 

/*CODIGO_PROMOCAO 	Código de Promoción
NOME	Nombre
TIPO	Tipo
DATA_INICIO 	Feche Inicio
DATA_FINAL	Fecha Final*/

BEGIN
	SET NOCOUNT ON;

/*Armado de temporal con las fechas vigentes de ofertas*/
SELECT 
C_ARTICULO,
C_SUCU_EMPR,
min(cast(C_ANIO as varchar(10))+'-'+(case  len(C_MES) when 1 then concat('0',cast(C_MES as varchar(10))) else cast(C_MES as varchar(10)) end) +'-'+(case  len(rtrim(ltrim(substring(C_DIA,13,2)))) when 1 then concat('0',cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10))) else cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10)) end))  as F_ALTA,
max(cast(C_ANIO as varchar(10))+'-'+(case  len(C_MES) when 1 then concat('0',cast(C_MES as varchar(10))) else cast(C_MES as varchar(10)) end) +'-'+(case  len(rtrim(ltrim(substring(C_DIA,13,2)))) when 1 then concat('0',cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10))) else cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10)) end))  as F_BAJA
into ##vigencia_promos
FROM   
(select 
C_ANIO,
C_MES, 
C_ARTICULO,
case C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](C_SUCU_EMPR) end as C_SUCU_EMPR,
convert(char,M_OFERTA_DIA1) as M_OFERTA_DIA1,
convert(char,M_OFERTA_DIA2) as M_OFERTA_DIA2,
convert(char,M_OFERTA_DIA3) as M_OFERTA_DIA3,
convert(char,M_OFERTA_DIA4) as M_OFERTA_DIA4,
convert(char,M_OFERTA_DIA5) as M_OFERTA_DIA5,
convert(char,M_OFERTA_DIA6) as M_OFERTA_DIA6,
convert(char,M_OFERTA_DIA7) as M_OFERTA_DIA7,
convert(char,M_OFERTA_DIA8) as M_OFERTA_DIA8,
convert(char,M_OFERTA_DIA9) as M_OFERTA_DIA9,
convert(char,M_OFERTA_DIA10) as M_OFERTA_DIA10,
convert(char,M_OFERTA_DIA11) as M_OFERTA_DIA11,
convert(char,M_OFERTA_DIA12) as M_OFERTA_DIA12,
convert(char,M_OFERTA_DIA13) as M_OFERTA_DIA13,
convert(char,M_OFERTA_DIA14) as M_OFERTA_DIA14,
convert(char,M_OFERTA_DIA15) as M_OFERTA_DIA15,
convert(char,M_OFERTA_DIA16) as M_OFERTA_DIA16,
convert(char,M_OFERTA_DIA17) as M_OFERTA_DIA17,
convert(char,M_OFERTA_DIA18) as M_OFERTA_DIA18,
convert(char,M_OFERTA_DIA19) as M_OFERTA_DIA19,
convert(char,M_OFERTA_DIA20) as M_OFERTA_DIA20,
convert(char,M_OFERTA_DIA21) as M_OFERTA_DIA21,
convert(char,M_OFERTA_DIA22) as M_OFERTA_DIA22,
convert(char,M_OFERTA_DIA23) as M_OFERTA_DIA23,
convert(char,M_OFERTA_DIA24) as M_OFERTA_DIA24,
convert(char,M_OFERTA_DIA25) as M_OFERTA_DIA25,
convert(char,M_OFERTA_DIA26) as M_OFERTA_DIA26,
convert(char,M_OFERTA_DIA27) as M_OFERTA_DIA27,
convert(char,M_OFERTA_DIA28) as M_OFERTA_DIA28,
convert(char,M_OFERTA_DIA29) as M_OFERTA_DIA29,
convert(char,M_OFERTA_DIA30) as M_OFERTA_DIA30,
convert(char,M_OFERTA_DIA31) as M_OFERTA_DIA31
from [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_OFERTA_FOLDER] 
where C_ARTICULO in (select C_ARTICULO from ##surtido)
and C_ANIO=@anio AND C_MES=@mes
) as Oferta
UNPIVOT  
   (OFERTA FOR C_DIA IN (M_OFERTA_DIA1,M_OFERTA_DIA2 ,M_OFERTA_DIA3,M_OFERTA_DIA4,M_OFERTA_DIA5,M_OFERTA_DIA6,M_OFERTA_DIA7,M_OFERTA_DIA8,M_OFERTA_DIA9,M_OFERTA_DIA10,M_OFERTA_DIA11,M_OFERTA_DIA12,
   M_OFERTA_DIA13,M_OFERTA_DIA14,M_OFERTA_DIA15,M_OFERTA_DIA16,M_OFERTA_DIA17,M_OFERTA_DIA18,M_OFERTA_DIA19,M_OFERTA_DIA20,M_OFERTA_DIA21,M_OFERTA_DIA22,M_OFERTA_DIA23,M_OFERTA_DIA24,
   M_OFERTA_DIA25,M_OFERTA_DIA26,M_OFERTA_DIA27,M_OFERTA_DIA28,M_OFERTA_DIA29,M_OFERTA_DIA30,M_OFERTA_DIA31)
) AS unpvt
where oferta='S'
group by C_SUCU_EMPR,C_ARTICULO


END;





GO
