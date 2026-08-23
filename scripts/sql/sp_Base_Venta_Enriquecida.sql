-- PROCEDURE: datamart.sp_procesar_promos_mes(date, date, boolean)

-- DROP PROCEDURE IF EXISTS datamart.sp_procesar_promos_mes(date, date, boolean);

CREATE OR REPLACE PROCEDURE datamart.sp_procesar_promos_mes(
	IN p_desde date,
	IN p_hasta date,
	IN p_actualizar_base_original boolean DEFAULT true)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_log_id bigint;
    v_inicio timestamp := clock_timestamp();
    v_fin timestamp;
    v_desde_efectivo date;
    v_mes_hasta date;
    v_etapa text := 'INIT';
    v_etapa_inicio timestamp;
    v_etapa_count bigint := 0;

    v_baseline_count bigint := 0;
    v_enriquecida_count bigint := 0;
    v_promo_reset_count bigint := 0;
    v_promo_count bigint := 0;

    v_error text;
BEGIN
    IF p_desde IS NULL OR p_hasta IS NULL THEN
        RAISE EXCEPTION 'p_desde y p_hasta no pueden ser NULL';
    END IF;

    IF p_hasta <= p_desde THEN
        RAISE EXCEPTION 'p_hasta debe ser mayor que p_desde';
    END IF;

    -- El baseline es mensual: una reparacion dentro de un mes obliga a
    -- reconstruirlo desde su primer dia. p_hasta conserva semantica exclusiva.
    v_desde_efectivo := date_trunc('month', p_desde)::date;
    v_mes_hasta := date_trunc('month', p_hasta - 1)::date;

    RAISE LOG '[PDD_BVE] stage=INIT status=STARTED pid=% desde=% hasta=% actualizar_base=%',
        pg_backend_pid(), v_desde_efectivo, p_hasta, p_actualizar_base_original;

    INSERT INTO datamart.dm_bve_proceso_log
    (
        proceso,
        mes_desde,
        mes_hasta,
        estado,
        inicio,
        actualizar_base_original,
        mensaje
    )
    VALUES
    (
        'sp_procesar_promos_mes',
        p_desde,
        p_hasta,
        'RUNNING',
        v_inicio,
        p_actualizar_base_original,
        'Inicio del proceso'
    )
    RETURNING id INTO v_log_id;

    v_etapa := 'DELETE_BASELINE';
    v_etapa_inicio := clock_timestamp();
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=STARTED', v_log_id, v_etapa;
    DELETE FROM datamart.dm_bve_baseline_mensual
    WHERE mes >= v_desde_efectivo
      AND mes <= v_mes_hasta;
    GET DIAGNOSTICS v_etapa_count = ROW_COUNT;
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=COMPLETED rows=% elapsed_s=%',
        v_log_id, v_etapa, v_etapa_count,
        round(extract(epoch FROM (clock_timestamp() - v_etapa_inicio))::numeric, 3);

    v_etapa := 'DELETE_ENRICHED';
    v_etapa_inicio := clock_timestamp();
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=STARTED', v_log_id, v_etapa;
    DELETE FROM datamart.dm_bve_ventas_enriquecidas
    WHERE fecha >= v_desde_efectivo
      AND fecha <  p_hasta;
    GET DIAGNOSTICS v_etapa_count = ROW_COUNT;
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=COMPLETED rows=% elapsed_s=%',
        v_log_id, v_etapa, v_etapa_count,
        round(extract(epoch FROM (clock_timestamp() - v_etapa_inicio))::numeric, 3);

    v_etapa := 'BUILD_BASELINE';
    v_etapa_inicio := clock_timestamp();
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=STARTED', v_log_id, v_etapa;
    INSERT INTO datamart.dm_bve_baseline_mensual
    (
        mes,
        codigo_articulo,
        sucursal,
        precio_mediano,
        precio_p25,
        precio_p75,
        unidades_mediana,
        unidades_promedio,
        unidades_p90,
        unidades_std,
        registros,
        fecha_calculo
    )
    SELECT
        date_trunc('month', fecha)::date,
        codigo_articulo,
        sucursal,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY precio),
        percentile_cont(0.25) WITHIN GROUP (ORDER BY precio),
        percentile_cont(0.75) WITHIN GROUP (ORDER BY precio),
        percentile_cont(0.5) WITHIN GROUP (ORDER BY unidades),
        avg(unidades),
        percentile_cont(0.9) WITHIN GROUP (ORDER BY unidades),
        stddev(unidades),
        count(*),
        now()
    FROM src.base_ventas_extendida
    WHERE fecha >= v_desde_efectivo
      AND fecha <  p_hasta
    GROUP BY
        date_trunc('month', fecha)::date,
        codigo_articulo,
        sucursal;

    GET DIAGNOSTICS v_baseline_count = ROW_COUNT;
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=COMPLETED rows=% elapsed_s=%',
        v_log_id, v_etapa, v_baseline_count,
        round(extract(epoch FROM (clock_timestamp() - v_etapa_inicio))::numeric, 3);

    v_etapa := 'BUILD_ENRICHED';
    v_etapa_inicio := clock_timestamp();
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=STARTED', v_log_id, v_etapa;
    INSERT INTO datamart.dm_bve_ventas_enriquecidas
    (
        fecha,
        codigo_articulo,
        sucursal,
        precio,
        unidades,
        importe_vendido,
        precio_mediano,
        unidades_mediana,
        factor_precio,
        factor_unidades,
        factor_elasticidad,
        score_promo,
        promo_fuerte_detectada,
        venta_basal,
        venta_promocional,
        fecha_calculo
    )
    SELECT
        v.fecha,
        v.codigo_articulo,
        v.sucursal,
        v.precio,
        v.unidades,
        v.importe_vendido,

        b.precio_mediano,
        b.unidades_mediana,

        round(v.precio / NULLIF(b.precio_mediano, 0), 4),

        round(v.unidades / NULLIF(b.unidades_mediana, 0), 4),

        round(
            (v.unidades / NULLIF(b.unidades_mediana, 0))
            /
            NULLIF((v.precio / NULLIF(b.precio_mediano, 0)), 0),
            4
        ),

        CASE
            WHEN v.precio < b.precio_mediano * 0.80
             AND v.unidades > b.unidades_mediana * 2.5
                THEN 100
            WHEN v.precio < b.precio_mediano * 0.90
             AND v.unidades > b.unidades_mediana * 1.8
                THEN 70
            WHEN v.precio < b.precio_mediano * 0.95
             AND v.unidades > b.unidades_mediana * 1.3
                THEN 40
            ELSE 0
        END,

        CASE
            WHEN v.precio < b.precio_mediano * 0.80
             AND v.unidades > b.unidades_mediana * 2.5
                THEN true
            ELSE false
        END,

        CASE
            WHEN v.precio < b.precio_mediano * 0.90
             AND v.unidades > b.unidades_mediana * 1.8
                THEN LEAST(v.unidades, b.unidades_mediana)
            ELSE v.unidades
        END,

        CASE
            WHEN v.precio < b.precio_mediano * 0.90
             AND v.unidades > b.unidades_mediana * 1.8
                THEN GREATEST(v.unidades - b.unidades_mediana, 0)
            ELSE 0
        END,

        now()
    FROM src.base_ventas_extendida v
    JOIN datamart.dm_bve_baseline_mensual b
      ON b.codigo_articulo = v.codigo_articulo
     AND b.sucursal = v.sucursal
     AND b.mes = date_trunc('month', v.fecha)::date
    WHERE v.fecha >= v_desde_efectivo
      AND v.fecha <  p_hasta;

    GET DIAGNOSTICS v_enriquecida_count = ROW_COUNT;
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=COMPLETED rows=% elapsed_s=%',
        v_log_id, v_etapa, v_enriquecida_count,
        round(extract(epoch FROM (clock_timestamp() - v_etapa_inicio))::numeric, 3);

    IF p_actualizar_base_original THEN

        v_etapa := 'RESET_PROMO';
        v_etapa_inicio := clock_timestamp();
        RAISE LOG '[PDD_BVE] run_id=% stage=% status=STARTED', v_log_id, v_etapa;
        UPDATE src.base_ventas_extendida
		SET promo_fuerte = false
		WHERE fecha >= v_desde_efectivo
		  AND fecha <  p_hasta
		  AND promo_fuerte IS DISTINCT FROM false;
        GET DIAGNOSTICS v_promo_reset_count = ROW_COUNT;
        RAISE LOG '[PDD_BVE] run_id=% stage=% status=COMPLETED rows=% elapsed_s=%',
            v_log_id, v_etapa, v_promo_reset_count,
            round(extract(epoch FROM (clock_timestamp() - v_etapa_inicio))::numeric, 3);

        v_etapa := 'SET_PROMO';
        v_etapa_inicio := clock_timestamp();
        RAISE LOG '[PDD_BVE] run_id=% stage=% status=STARTED', v_log_id, v_etapa;
        UPDATE src.base_ventas_extendida v
        SET promo_fuerte = true
        FROM datamart.dm_bve_ventas_enriquecidas e
        WHERE e.fecha = v.fecha
          AND e.codigo_articulo = v.codigo_articulo
          AND e.sucursal = v.sucursal
          AND e.precio = v.precio
          AND e.promo_fuerte_detectada = true
          AND v.fecha >= v_desde_efectivo
          AND v.fecha <  p_hasta
          AND v.promo_fuerte IS DISTINCT FROM true;

        GET DIAGNOSTICS v_promo_count = ROW_COUNT;
        RAISE LOG '[PDD_BVE] run_id=% stage=% status=COMPLETED rows=% elapsed_s=%',
            v_log_id, v_etapa, v_promo_count,
            round(extract(epoch FROM (clock_timestamp() - v_etapa_inicio))::numeric, 3);

    END IF;

    v_etapa := 'ANALYZE';
    v_etapa_inicio := clock_timestamp();
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=STARTED', v_log_id, v_etapa;
    ANALYZE datamart.dm_bve_baseline_mensual;
    ANALYZE datamart.dm_bve_ventas_enriquecidas;
    RAISE LOG '[PDD_BVE] run_id=% stage=% status=COMPLETED elapsed_s=%',
        v_log_id, v_etapa,
        round(extract(epoch FROM (clock_timestamp() - v_etapa_inicio))::numeric, 3);

    v_fin := clock_timestamp();

    UPDATE datamart.dm_bve_proceso_log
    SET
        estado = 'OK',
        baseline_registros = v_baseline_count,
        ventas_enriquecidas_registros = v_enriquecida_count,
        promos_fuertes_registros = v_promo_count,
        fin = v_fin,
        duracion_segundos = extract(epoch FROM (v_fin - v_inicio)),
        mensaje = 'Proceso finalizado correctamente'
    WHERE id = v_log_id;

    RAISE LOG '[PDD_BVE] run_id=% stage=PROCESS status=COMPLETED elapsed_s=% baseline_rows=% enriched_rows=% promo_reset_rows=% promo_set_rows=%',
        v_log_id,
        round(extract(epoch FROM (v_fin - v_inicio))::numeric, 3),
        v_baseline_count, v_enriquecida_count, v_promo_reset_count, v_promo_count;

EXCEPTION WHEN OTHERS THEN
    v_error := SQLERRM;
    v_fin := clock_timestamp();

    UPDATE datamart.dm_bve_proceso_log
    SET
        estado = 'ERROR',
        fin = v_fin,
        duracion_segundos = extract(epoch FROM (v_fin - v_inicio)),
        mensaje = 'Error durante el procesamiento',
        error_detalle = v_error
    WHERE id = v_log_id;

    RAISE LOG '[PDD_BVE] run_id=% stage=% status=ERROR elapsed_s=% error=%',
        v_log_id, v_etapa,
        round(extract(epoch FROM (v_fin - v_inicio))::numeric, 3), v_error;

    RAISE;
END;
$BODY$;
ALTER PROCEDURE datamart.sp_procesar_promos_mes(date, date, boolean)
    OWNER TO postgres;

