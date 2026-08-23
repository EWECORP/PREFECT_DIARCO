 /******************************
  CONTROL DE CONSISTENCIA REGISTROS DE VENTA
 *******************************/
 --- POSTGRES:
 --- BASE DE DATOS: diarco_data
 --- CONTROL TABLA DE VENTA DIARCO REPLICADA EN SRC

SELECT 
    F_VENTA,
    COUNT(*) AS REGISTROS,
    replace(to_char(SUM(Q_UNIDADES_VENDIDAS), 'FM9999999990.99'), '.', ',') AS UNIDADES
FROM src.t702_est_vtas_por_articulo
WHERE F_VENTA >= '2026-08-01'
GROUP BY F_VENTA
ORDER BY F_VENTA;

 --- VENTA DIARCO BARRIO
SELECT 
    F_VENTA,
    COUNT(*) AS REGISTROS,
	replace(to_char(SUM(Q_UNIDADES_VENDIDAS), 'FM9999999990.99'), '.', ',') AS UNIDADES
FROM  src.t702_est_vtas_por_articulo_dbarrio
WHERE F_VENTA >= '2026-08-01'
GROUP BY F_VENTA
ORDER BY F_VENTA;


--- ACTUALIZACIÓN
--- Para lograr la actualización hay que eliminar los registros posteriores a la fecha definida 
--- para que el sistema vuelva a cargar los datos mayores a la úlltima fecha válida.

---DELETE FROM SRC.T702_EST_VTAS_POR_ARTICULO
WHERE
	F_VENTA >= '2026-08-02';   --- DELETE 3254916
	
---DELETE FROM SRC.T702_EST_VTAS_POR_ARTICULO_DBARRIO
WHERE
	F_VENTA >= '2026-08-02';   --- DELETE 1648530

Luego Ejecutar PREFECT
FORECAST_PUSH_VENTAS_STOCK_DIARIO_PROD
Flow -> actualizar_bases_ventas.py
Work Pool
dmz-diarco
Work Queue
push-forecast

 /******************************
  CONTROL DE CONSISTENCIA REGISTROS DE VENTAS EXTENDIDAS
 *******************************/
ANALYZE src.base_ventas_extendida;

/************************************************************
 HACER LOS CONTROLES MAXIMO POR PERÏODOS MENSUALES PORQUE SON 
 MUCHOS MILLONES DE REGISTROS
 *****************************/

 --- VENTA DIARCO 
SELECT 
    fecha,
    COUNT(*) AS REGISTROS,
    replace(to_char(SUM(unidades), 'FM9999999990.99'), '.', ',') AS UNIDADES
FROM src.base_ventas_extendida
WHERE fecha >= '2026-08-01'
  AND fecha < '2026-09-01'
  AND sucursal < 300
GROUP BY fecha
ORDER BY fecha;


 --- VENTA DIARCO BARRIO
 SELECT 
    fecha,
    COUNT(*) AS REGISTROS,
	replace(to_char(SUM(unidades), 'FM9999999990.99'), '.', ',') AS UNIDADES
FROM src.base_ventas_extendida
WHERE fecha >= '2026-08-01'
  AND fecha < '2026-09-01'
  AND sucursal >= 300
GROUP BY fecha
ORDER BY fecha;

--- PARA ACTUALIZAR
--- EJECUTAR PYTHON: (venv) PS E:\ETL> python ETL_DIARCO/scripts/push/actualizar_base_ventas_extendida.py 14 true 2026-05-01 2026-06-26 replace_rango

 /******************************
  CONTROL DE CONSISTENCIA REGISTROS DE VENTAS ENRIQUESIDAS
 *******************************/

--- VENTA DATAMART DIARCO
SELECT 
    fecha,
    COUNT(*) AS REGISTROS,
    replace(to_char(SUM(unidades), 'FM9999999990.99'), '.', ',') AS UNIDADES
FROM datamart.dm_bve_ventas_enriquecidas
WHERE fecha >= '2026-08-01'
 -- AND fecha < '2026-01-01'
  AND sucursal < 300
GROUP BY fecha
ORDER BY fecha;

--- VENTA DATAMART BARRIO
SELECT 
    fecha,
    COUNT(*) AS REGISTROS,
    replace(to_char(SUM(unidades), 'FM9999999990.99'), '.', ',') AS UNIDADES
FROM datamart.dm_bve_ventas_enriquecidas
WHERE fecha >= '2026-08-01'
--  AND fecha < '2026-01-01'
  AND sucursal >= 300
GROUP BY fecha
ORDER BY fecha;


/*** Para REGENERAR VENTA ENRIQUECIDA 
 En esta misma terminal
 Abrir SQL y ejecutar:  D:\OneDrive\SQL\PG Admin\PG_Generar_VENTA_ENRIQUECIDA.sql


 REVISAR VENTA POR X DIA DEL DATAMART


 ****/
-- ========================================================
-- 3. CONTROLAR VENTAS DIARIAS
-- ========================================================
 --- VENTA DATAMART VENTAS DIARIAS DIARCO
SELECT 
    fecha,
    replace(to_char(SUM(unidades_total), 'FM9999999990.99'), '.', ',') AS UNIDADES
FROM datamart.dm_ventas_diarias
WHERE fecha >= '2026-08-01'
 -- AND fecha < '2026-01-01'
  AND sucursal < 300
GROUP BY fecha
ORDER BY fecha;

--- VENTA DATAMART VENTAS DIARIAS BARRIO
SELECT 
    fecha,
    replace(to_char(SUM(unidades_total), 'FM9999999990.99'), '.', ',') AS UNIDADES
FROM datamart.dm_ventas_diarias
WHERE fecha >= '2026-08-01'
  AND fecha < '2026-10-01'
  AND sucursal >= 300
GROUP BY fecha
ORDER BY fecha;



/**** GENERAR VENTAS DIARIAS ****/
-- ========================================================
-- 4. REPROCESAR RANGO VENTAD DIARIAS
-- ========================================================

DO $$
DECLARE
    v_desde date := DATE '2026-08-10';
    v_hasta date := DATE '2026-08-21';
	v_borrados integer;
	
BEGIN
    IF v_desde IS NULL OR v_hasta IS NULL THEN
        RAISE EXCEPTION 'v_desde y v_hasta no pueden ser NULL';
    END IF;

    IF v_desde >= v_hasta THEN
        RAISE EXCEPTION 'Rango inválido: v_desde (%) debe ser menor que v_hasta (%)',
            v_desde, v_hasta;
    END IF;

    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Procesando ventas desde % hasta % (EXCLUIDA)', v_desde, v_hasta;
    RAISE NOTICE '================================================================';

    RAISE NOTICE 'Eliminando datos diarios previos...';

    DELETE FROM datamart.dm_ventas_diarias
    WHERE fecha >= v_desde
      AND fecha <  v_hasta;

	GET DIAGNOSTICS v_borrados = ROW_COUNT;
    RAISE NOTICE 'Datos Diarios eliminados: % registros', v_borrados;

    RAISE NOTICE 'Generando ventas diarias...';

    INSERT INTO datamart.dm_ventas_diarias (
        fecha, codigo_articulo, sucursal, unidades_total, importe_total
    )
    SELECT
        fecha::date,
        codigo_articulo,
        sucursal,
        SUM(unidades),
        SUM(importe_vendido)
    FROM src.base_ventas_extendida
    WHERE fecha >= v_desde
      AND fecha <  v_hasta
    GROUP BY fecha::date, codigo_articulo, sucursal
    ON CONFLICT (fecha, codigo_articulo, sucursal)
    DO UPDATE SET
        unidades_total = EXCLUDED.unidades_total,
        importe_total = EXCLUDED.importe_total;

	RAISE NOTICE 'Optimizando tabla ventas diarias...';

    ANALYZE datamart.dm_ventas_diarias;

    RAISE NOTICE 'ANALYZE terminado OK';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Proceso finalizado correctamente';
    RAISE NOTICE '============================================================';
END $$;




