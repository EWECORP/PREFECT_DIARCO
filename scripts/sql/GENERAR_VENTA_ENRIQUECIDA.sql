-- ============================================================
-- PROCESAR PROMOCIONES Y ELASTICIDAD — BASE VENTAS EXTENDIDA
-- ============================================================
-- Objetivo:
--   1. Recalcular baseline mensual
--   2. Recalcular ventas enriquecidas
--   3. Calcular venta basal/promocional
--   4. Actualizar promo_fuerte en tabla original
--
-- Reprocesable por mes.
--
-- Recomendado:
--   Ejecutar dentro de ventana batch/nocturna.
--
-- ============================================================

BEGIN;

-- ============================================================
-- PARAMETROS
-- ============================================================

-- Ajustar manualmente
-- Ejemplo: Mayo 2026

DO $$
DECLARE
    v_desde date := DATE '2026-05-01';
    v_hasta date := DATE '2026-06-01';
BEGIN

RAISE NOTICE 'Procesando desde % hasta %', v_desde, v_hasta;

-- ============================================================
-- 1. LIMPIEZA PREVIA
-- ============================================================

RAISE NOTICE 'Eliminando baseline previo...';

DELETE
FROM datamart.dm_bve_baseline_mensual
WHERE mes = date_trunc('month', v_desde)::date;

RAISE NOTICE 'Eliminando ventas enriquecidas previas...';

DELETE
FROM datamart.dm_bve_ventas_enriquecidas
WHERE fecha >= v_desde
  AND fecha <  v_hasta;

-- ============================================================
-- 2. REGENERAR BASELINE
-- ============================================================

RAISE NOTICE 'Generando baseline mensual...';

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
    date_trunc('month', fecha)::date AS mes,
    codigo_articulo,
    sucursal,

    percentile_cont(0.5)
        WITHIN GROUP (ORDER BY precio) AS precio_mediano,

    percentile_cont(0.25)
        WITHIN GROUP (ORDER BY precio) AS precio_p25,

    percentile_cont(0.75)
        WITHIN GROUP (ORDER BY precio) AS precio_p75,

    percentile_cont(0.5)
        WITHIN GROUP (ORDER BY unidades) AS unidades_mediana,

    avg(unidades) AS unidades_promedio,

    percentile_cont(0.9)
        WITHIN GROUP (ORDER BY unidades) AS unidades_p90,

    stddev(unidades) AS unidades_std,

    count(*) AS registros,

    now()

FROM src.base_ventas_extendida
WHERE fecha >= v_desde
  AND fecha <  v_hasta
GROUP BY
    date_trunc('month', fecha)::date,
    codigo_articulo,
    sucursal;

RAISE NOTICE 'Baseline generado OK';

-- ============================================================
-- 3. GENERAR VENTAS ENRIQUECIDAS
-- ============================================================

RAISE NOTICE 'Generando ventas enriquecidas...';

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

    ROUND(
        v.precio / NULLIF(b.precio_mediano, 0),
        4
    ) AS factor_precio,

    ROUND(
        v.unidades / NULLIF(b.unidades_mediana, 0),
        4
    ) AS factor_unidades,

    ROUND(
        (
            v.unidades / NULLIF(b.unidades_mediana, 0)
        )
        /
        NULLIF(
            (
                v.precio / NULLIF(b.precio_mediano, 0)
            ),
            0
        ),
        4
    ) AS factor_elasticidad,

    CASE
        WHEN
            v.precio < b.precio_mediano * 0.80
            AND
            v.unidades > b.unidades_mediana * 2.5
        THEN 100

        WHEN
            v.precio < b.precio_mediano * 0.90
            AND
            v.unidades > b.unidades_mediana * 1.8
        THEN 70

        WHEN
            v.precio < b.precio_mediano * 0.95
            AND
            v.unidades > b.unidades_mediana * 1.3
        THEN 40

        ELSE 0
    END AS score_promo,

    CASE
        WHEN
            v.precio < b.precio_mediano * 0.80
            AND
            v.unidades > b.unidades_mediana * 2.5
        THEN true
        ELSE false
    END AS promo_fuerte_detectada,

    now()

FROM src.base_ventas_extendida v
JOIN datamart.dm_bve_baseline_mensual b
  ON b.codigo_articulo = v.codigo_articulo
 AND b.sucursal = v.sucursal
 AND b.mes = date_trunc('month', v.fecha)::date

WHERE v.fecha >= v_desde
  AND v.fecha <  v_hasta;

RAISE NOTICE 'Ventas enriquecidas generadas OK';

-- ============================================================
-- 4. CALCULAR VENTA BASAL / PROMOCIONAL
-- ============================================================

RAISE NOTICE 'Calculando venta basal/promocional...';

UPDATE datamart.dm_bve_ventas_enriquecidas
SET
    venta_basal =
        CASE
            WHEN score_promo >= 70
            THEN LEAST(unidades, unidades_mediana)
            ELSE unidades
        END,

    venta_promocional =
        CASE
            WHEN score_promo >= 70
            THEN GREATEST(unidades - unidades_mediana, 0)
            ELSE 0
        END

WHERE fecha >= v_desde
  AND fecha <  v_hasta;

RAISE NOTICE 'Venta basal/promocional calculada OK';

-- ============================================================
-- 5. RESET PROMO_FUERTE
-- ============================================================

RAISE NOTICE 'Reset promo_fuerte original...';

UPDATE src.base_ventas_extendida
SET promo_fuerte = false
WHERE fecha >= v_desde
  AND fecha <  v_hasta;

-- ============================================================
-- 6. ACTUALIZAR PROMO_FUERTE
-- ============================================================

RAISE NOTICE 'Actualizando promo_fuerte...';

UPDATE src.base_ventas_extendida v
SET promo_fuerte = true
FROM datamart.dm_bve_ventas_enriquecidas e
WHERE e.fecha = v.fecha
  AND e.codigo_articulo = v.codigo_articulo
  AND e.sucursal = v.sucursal
  AND e.precio = v.precio

  AND e.promo_fuerte_detectada = true

  AND v.fecha >= v_desde
  AND v.fecha <  v_hasta;

RAISE NOTICE 'promo_fuerte actualizado OK';

-- ============================================================
-- 7. ANALYZE
-- ============================================================

RAISE NOTICE 'Ejecutando ANALYZE...';

ANALYZE datamart.dm_bve_baseline_mensual;
ANALYZE datamart.dm_bve_ventas_enriquecidas;

-- ============================================================
-- 8. KPIS FINALES
-- ============================================================

RAISE NOTICE 'KPIs finales';

PERFORM 1;

END $$;

COMMIT;

-- ============================================================
-- VALIDACIONES POSTERIORES
-- ============================================================

-- Registros enriquecidos
SELECT
    date_trunc('month', fecha)::date AS mes,
    count(*) AS registros
FROM datamart.dm_bve_ventas_enriquecidas
GROUP BY 1
ORDER BY 1;

-- Promos fuertes
SELECT
    date_trunc('month', fecha)::date AS mes,
    count(*) FILTER (WHERE promo_fuerte_detectada) AS promos_fuertes
FROM datamart.dm_bve_ventas_enriquecidas
GROUP BY 1
ORDER BY 1;

-- Distribución score
SELECT
    CASE
        WHEN score_promo >= 80 THEN '80-100 Promo fuerte'
        WHEN score_promo >= 60 THEN '60-79 Promo'
        WHEN score_promo >= 30 THEN '30-59 Promo leve'
        ELSE '0-29 Normal'
    END AS rango_score,
    count(*) AS registros
FROM datamart.dm_bve_ventas_enriquecidas
GROUP BY 1
ORDER BY 1;

-- Top elasticidades
SELECT
    fecha,
    codigo_articulo,
    sucursal,
    precio,
    unidades,
    factor_precio,
    factor_unidades,
    factor_elasticidad,
    score_promo
FROM datamart.dm_bve_ventas_enriquecidas
WHERE score_promo >= 70
ORDER BY factor_elasticidad DESC
LIMIT 100;

