BEGIN;

-- 0) Preparación: truncar staging
TRUNCATE TABLE src.base_ventas_extendida__stg;

-- 1) Determinar fecha desde (ventana)
WITH max_dest AS (
    SELECT COALESCE(MAX(fecha), DATE '1900-01-01') AS max_fecha
    FROM src.base_ventas_extendida
),
params AS (
    SELECT (max_fecha - INTERVAL '14 days')::date AS fecha_desde   -- ventana de 14 días
    FROM max_dest
)

-- 2) Poblar STAGING desde la venta diaria
INSERT INTO src.base_ventas_extendida__stg(
    fecha, codigo_articulo, sucursal, precio, unidades, importe_vendido,
    con_stock, venta_especial, promo_normal, promo_fuerte, precio_prefijado, factor_precio,
    costo, familia, rubro, subrubro, c_proveedor_primario, nombre_articulo, clasificacion,
    fecha_procesado, marca_procesado
)
SELECT
    V.f_venta::date                                      AS fecha,
    V.c_articulo::bigint                                  AS codigo_articulo,
    V.c_sucu_empr::int                                    AS sucursal,
    V.i_precio_venta::numeric(18,6)                       AS precio,
    V.q_unidades_vendidas::numeric(18,6)                  AS unidades,
    V.i_vendido::numeric(18,6)                            AS importe_vendido,
    TRUE  AS con_stock,
    FALSE AS venta_especial,
    FALSE AS promo_normal,
    FALSE AS promo_fuerte,
    NULL  AS precio_prefijado,
    NULL  AS factor_precio,
    V.i_precio_costo::numeric(18,6)                       AS costo,
    V.c_familia::int                                      AS familia, 
    A.c_rubro::int                                        AS rubro,
    A.c_subrubro_1::int                                   AS subrubro,
    A.c_proveedor_primario::int                           AS c_proveedor_primario,
    TRIM(BOTH FROM REPLACE(REPLACE(REPLACE(A.n_articulo, CHR(9), ''), CHR(13), ''), CHR(10), '')) AS nombre_articulo,
    A.c_clasificacion_compra::text                        AS clasificacion,
    CURRENT_TIMESTAMP                                     AS fecha_procesado,
    0                                                     AS marca_procesado
FROM src.t702_est_vtas_por_articulo V
LEFT JOIN src.t050_articulos A
        ON V.c_articulo = A.c_articulo
CROSS JOIN params p
WHERE V.f_venta::date >= p.fecha_desde
    AND A.m_baja = 'N';

-- 3) Enriquecimiento: OFERTAS (promo_normal/promo_fuerte según flags)
WITH oferta AS (
    SELECT
        ofe.c_articulo,
        ofe.c_sucu_empr,
        make_date(ofe.c_anio::int, ofe.c_mes::int, u.d::int) AS fecha,
        NULLIF(BTRIM(UPPER(u.val::text)), '') AS flag
    FROM src.t710_estadis_oferta_folder AS ofe
    CROSS JOIN LATERAL unnest(ARRAY[
        ofe.m_oferta_dia1,  ofe.m_oferta_dia2,  ofe.m_oferta_dia3,  ofe.m_oferta_dia4,  ofe.m_oferta_dia5,
        ofe.m_oferta_dia6,  ofe.m_oferta_dia7,  ofe.m_oferta_dia8,  ofe.m_oferta_dia9,  ofe.m_oferta_dia10,
        ofe.m_oferta_dia11, ofe.m_oferta_dia12, ofe.m_oferta_dia13, ofe.m_oferta_dia14, ofe.m_oferta_dia15,
        ofe.m_oferta_dia16, ofe.m_oferta_dia17, ofe.m_oferta_dia18, ofe.m_oferta_dia19, ofe.m_oferta_dia20,
        ofe.m_oferta_dia21, ofe.m_oferta_dia22, ofe.m_oferta_dia23, ofe.m_oferta_dia24, ofe.m_oferta_dia25,
        ofe.m_oferta_dia26, ofe.m_oferta_dia27, ofe.m_oferta_dia28, ofe.m_oferta_dia29, ofe.m_oferta_dia30,
        ofe.m_oferta_dia31
    ]) WITH ORDINALITY AS u(val, d)
    WHERE u.d <= EXTRACT(DAY FROM (date_trunc('month', make_date(ofe.c_anio::int, ofe.c_mes::int, 1))
                                    + INTERVAL '1 month - 1 day'))
)
UPDATE src.base_ventas_extendida__stg b
SET promo_normal = (of.flag = 'S'),
    promo_fuerte = (of.flag = 'F')
FROM oferta of
WHERE b.codigo_articulo = of.c_articulo
    AND b.sucursal        = of.c_sucu_empr
    AND b.fecha           = of.fecha;

-- 4) Enriquecimiento: PRECIO_PREFIJADO
WITH precios AS (
    SELECT
        ep.c_articulo,
        ep.c_sucu_empr,
        make_date(ep.c_anio::int, ep.c_mes::int, u.d::int) AS fecha,
        u.val::numeric AS precio_prefijado
    FROM src.t710_estadis_precios ep
    CROSS JOIN LATERAL unnest(ARRAY[
        ep.i_precio_vta_1,  ep.i_precio_vta_2,  ep.i_precio_vta_3,  ep.i_precio_vta_4,  ep.i_precio_vta_5,
        ep.i_precio_vta_6,  ep.i_precio_vta_7,  ep.i_precio_vta_8,  ep.i_precio_vta_9,  ep.i_precio_vta_10,
        ep.i_precio_vta_11, ep.i_precio_vta_12, ep.i_precio_vta_13, ep.i_precio_vta_14, ep.i_precio_vta_15,
        ep.i_precio_vta_16, ep.i_precio_vta_17, ep.i_precio_vta_18, ep.i_precio_vta_19, ep.i_precio_vta_20,
        ep.i_precio_vta_21, ep.i_precio_vta_22, ep.i_precio_vta_23, ep.i_precio_vta_24, ep.i_precio_vta_25,
        ep.i_precio_vta_26, ep.i_precio_vta_27, ep.i_precio_vta_28, ep.i_precio_vta_29, ep.i_precio_vta_30,
        ep.i_precio_vta_31
    ]) WITH ORDINALITY AS u(val, d)
    WHERE u.d <= EXTRACT(DAY FROM (date_trunc('month', make_date(ep.c_anio::int, ep.c_mes::int, 1))
                                    + INTERVAL '1 month - 1 day'))
)
UPDATE src.base_ventas_extendida__stg b
SET precio_prefijado = p.precio_prefijado
FROM precios p
WHERE b.codigo_articulo = p.c_articulo
    AND b.sucursal        = p.c_sucu_empr
    AND b.fecha           = p.fecha;

-- 5) Calcular FACTOR_PRECIO (corrige typos)
UPDATE src.base_ventas_extendida__stg
SET factor_precio =
    CASE
        WHEN precio_prefijado IS NULL OR precio_prefijado = 0 THEN NULL
        ELSE ROUND(precio / precio_prefijado, 6)
    END;

-- 6) DQ básico: eliminar duplicados exactos dentro de STG (si los hubiera)
--    Conserva la primera aparición por la PK natural (fecha,art,suc,precio)
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (
        PARTITION BY fecha, codigo_articulo, sucursal, precio
        ORDER BY fecha_procesado
    ) AS rn
    FROM src.base_ventas_extendida__stg
)
DELETE FROM src.base_ventas_extendida__stg s
USING ranked r
WHERE s.fecha = r.fecha
    AND s.codigo_articulo = r.codigo_articulo
    AND s.sucursal = r.sucursal
    AND s.precio = r.precio
    AND r.rn > 1;

-- 7) Upsert al DESTINO (particionado)
INSERT INTO src.base_ventas_extendida AS d
SELECT *
FROM src.base_ventas_extendida__stg s
ON CONFLICT (fecha, codigo_articulo, sucursal, precio)
DO UPDATE
SET unidades         = EXCLUDED.unidades,
    importe_vendido  = EXCLUDED.importe_vendido,
    con_stock        = EXCLUDED.con_stock,
    venta_especial   = EXCLUDED.venta_especial,
    promo_normal     = EXCLUDED.promo_normal,
    promo_fuerte     = EXCLUDED.promo_fuerte,
    precio_prefijado = EXCLUDED.precio_prefijado,
    factor_precio    = EXCLUDED.factor_precio,
    costo            = EXCLUDED.costo,
    familia          = EXCLUDED.familia,
    rubro            = EXCLUDED.rubro,
    subrubro         = EXCLUDED.subrubro,
    c_proveedor_primario = EXCLUDED.c_proveedor_primario,
    nombre_articulo  = EXCLUDED.nombre_articulo,
    clasificacion    = EXCLUDED.clasificacion,
    fecha_procesado  = EXCLUDED.fecha_procesado,
    marca_procesado  = EXCLUDED.marca_procesado;

COMMIT;

-- 8) Mantenimiento post-carga (opcional programado)
ANALYZE src.base_ventas_extendida__stg;
-- Si usan partición por mes, analizar sólo las particiones afectadas.