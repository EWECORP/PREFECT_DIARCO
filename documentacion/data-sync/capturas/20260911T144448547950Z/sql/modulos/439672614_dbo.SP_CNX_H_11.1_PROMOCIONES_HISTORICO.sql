-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE PROCEDURE [dbo].[SP_CNX_H_11.1_PROMOCIONES_HISTORICO] @anio char(4),@mes char(2)
AS 

/*CODIGO_PROMOCAO 	Código de Promoción
NOME	Nombre
TIPO	Tipo
DATA_INICIO 	Feche Inicio
DATA_FINAL	Fecha Final*/

BEGIN
	SET NOCOUNT ON;

/*Armado de temporal con las fechas vigentes de ofertas pasando por parámtros año y mes promocional*/
IF OBJECT_ID('tempdb..##vigencia_promos') IS NULL  
        BEGIN
         exec dbo.[SP_KIKKER_H_11.1_PROMOCIONES_HISTORICO_VIGENTES] @anio, @mes
        END;


select 'CODIGO_PROMOCAO','NOME','TIPO','DATA_INICIO','DATA_FINAL' union all 
select 
cast(ART.C_ARTICULO as varchar (5)),
dbo.[NORMALIZA_STRING](ART.N_ARTICULO),
'2',
min(VP.F_ALTA),
max(VP.F_BAJA)
from  [DIARCOP001].[DIARCOP].dbo.[T051_ARTICULOS_SUCURSAL] ART_SUC
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on ART_SUC.C_ARTICULO=ART.C_ARTICULO
inner join ##vigencia_promos VP on VP.C_ARTICULO = ART_SUC.C_ARTICULO
and M_OFERTA_SUCU='S' and ART.C_ARTICULO in (select C_ARTICULO from ##surtido)
group by ART.C_ARTICULO,ART.N_ARTICULO


END;



GO
