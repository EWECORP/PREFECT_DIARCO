--- SANDBOX para ver como funciona el CDC

-- ✅ 1. Ver qué tablas CDC existen

SELECT * 
FROM cdc.change_tables;

-- ✅ 2. Declarar variables LSN

DECLARE 
    @from_lsn_cabe BINARY(10),
    @from_lsn_deta BINARY(10),
    @to_lsn        BINARY(10);

--✅ 3. Obtener el LSN máximo (hasta dónde leer)

SET @to_lsn = sys.fn_cdc_get_max_lsn();
SELECT @to_lsn AS TO_LSN;

--✅ 4. Obtener el LSN mínimo de cada tabla (desde dónde leer)

--CABECERA
SET @from_lsn_cabe = sys.fn_cdc_get_min_lsn('dbo_T080_OC_CABE');
SELECT @from_lsn_cabe AS MIN_LSN_CABE;

--DETALLE
SET @from_lsn_deta = sys.fn_cdc_get_min_lsn('dbo_T081_OC_DETA');
SELECT @from_lsn_deta AS MIN_LSN_DETA;

--✅ 5. Ver cambios de CABECERA

SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_T080_OC_CABE(
    @from_lsn_cabe,
    @to_lsn,
    'all'
);

-- ✅ 6. Ver cambios de DETALLE

SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_T081_OC_DETA(
    @from_lsn_deta,
    @to_lsn,
    'all'
);


/**** 🧠 Cómo interpretar los resultados
En ambas consultas vas a ver columnas especiales:

Columna	Significado
__$start_lsn	Identificador del cambio (orden cronológico)
__$operation	Tipo de operación (1=delete, 2=insert, 3=update-before, 4=update-after)
__$seqval	    Orden dentro del mismo LSN
__$update_mask	Qué columnas cambiaron

🎯 Ejemplo de interpretación rápida
Si ves: Código
__$operation = 2 → INSERT
__$operation = 1 → DELETE
__$operation = 3 → UPDATE (antes)
__$operation = 4 → UPDATE (después)
***/