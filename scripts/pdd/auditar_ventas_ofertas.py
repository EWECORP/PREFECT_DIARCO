"""Audita ventas y ofertas de diarco_data por fecha, articulo y sucursal.

Ejemplo:
  python scripts/pdd/auditar_ventas_ofertas.py --desde 2026-10-01 --hasta 2026-10-10 --articulo 42758 --sucursal 18

Las fechas del CLI son inclusivas. La conexion se abre en modo de solo lectura.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from uuid import UUID

import psycopg2
from dotenv import dotenv_values
from psycopg2.extras import RealDictCursor


ROOT = Path(__file__).resolve().parents[2]
FIELDS = (
    "fecha", "codigo_articulo", "sucursal", "nombre_articulo",
    "origen_filas", "enriquecida_filas", "origen_unidades", "enriquecida_unidades",
    "origen_promo_fuerte", "score_40", "score_70", "score_100",
    "basal_unidades", "promocional_unidades", "baseline_desactualizado",
    "score_incorrecto", "promo_fuerte_incorrecta", "reparto_incorrecto",
    "diferencia_filas", "diferencia_unidades", "pdd_filas",
    "pdd_basal_unidades", "pdd_promocional_unidades", "diferencia_pdd_basal",
    "diferencia_pdd_promocional", "pdd_desactualizado",
    "enriquecida_calculada", "pdd_enriquecida_calculada", "baseline_calculado",
)


def args_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--desde", required=True, type=date.fromisoformat)
    parser.add_argument("--hasta", required=True, type=date.fromisoformat,
                        help="Ultimo dia incluido")
    parser.add_argument("--articulo", type=int)
    parser.add_argument("--sucursal", type=int)
    parser.add_argument("--scope-version-uuid", type=UUID,
                        help="Compara con datamart.dm_pdd_venta_diaria")
    parser.add_argument("--solo-anomalias", action="store_true",
                        help="Lista pares fecha/articulo/sucursal con diferencias")
    parser.add_argument("--csv", type=Path, help="Exporta todas las filas consultadas")
    parser.add_argument("--timeout-segundos", type=int, default=120)
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.hasta < args.desde:
        raise ValueError("--hasta debe ser igual o posterior a --desde")
    days = (args.hasta - args.desde).days + 1
    max_days = 93 if args.articulo is not None or args.sucursal is not None else 31
    if days > max_days:
        raise ValueError(f"Periodo maximo: {max_days} dias para estos filtros")
    if args.timeout_segundos < 1 or args.timeout_segundos > 600:
        raise ValueError("--timeout-segundos debe estar entre 1 y 600")
    if args.articulo is not None and args.articulo < 0:
        raise ValueError("--articulo debe ser no negativo")
    if args.sucursal is not None and args.sucursal < 0:
        raise ValueError("--sucursal debe ser no negativa")


def db_settings() -> dict[str, str]:
    env_path = Path(os.environ.get("ETL_ENV_PATH", ROOT / ".env"))
    values = dotenv_values(env_path) if env_path.exists() else {}
    settings = {key: os.environ.get(key) or values.get(key)
                for key in ("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD")}
    settings["PG_PORT"] = settings["PG_PORT"] or "5432"
    missing = [key for key, value in settings.items() if not value]
    if missing:
        raise ValueError(f"Faltan variables de conexion: {', '.join(missing)}; ETL_ENV_PATH={env_path}")
    return settings


def query(args: argparse.Namespace) -> tuple[str, dict[str, object]]:
    filters = []
    params: dict[str, object] = {
        "desde": args.desde,
        "hasta_exclusiva": args.hasta + timedelta(days=1),
        "scope": str(args.scope_version_uuid) if args.scope_version_uuid else None,
    }
    if args.articulo is not None:
        filters.append("AND codigo_articulo = %(articulo)s")
        params["articulo"] = args.articulo
    if args.sucursal is not None:
        filters.append("AND sucursal = %(sucursal)s")
        params["sucursal"] = args.sucursal
    where = "\n".join(filters)
    pdd_sql = """
    pdd AS (
      SELECT sales_date AS fecha, codigo_articulo, sucursal,
             count(*) AS pdd_filas,
             sum(basal_units) AS pdd_basal_unidades,
             sum(promotional_units) AS pdd_promocional_unidades,
             max(enriched_max_calculated_at) AS pdd_enriquecida_calculada
      FROM datamart.dm_pdd_venta_diaria
      WHERE sales_date >= %(desde)s AND sales_date < %(hasta_exclusiva)s
        AND scope_version_uuid = %(scope)s
      """ + where + """
      GROUP BY 1, 2, 3
    ),
    """ if args.scope_version_uuid else """
    pdd AS (
      SELECT NULL::date fecha, NULL::integer codigo_articulo, NULL::integer sucursal,
             NULL::bigint pdd_filas, NULL::numeric pdd_basal_unidades,
             NULL::numeric pdd_promocional_unidades,
             NULL::timestamp pdd_enriquecida_calculada
      WHERE false
    ),
    """
    sql = """
    WITH origen AS (
      SELECT fecha::date AS fecha, codigo_articulo, sucursal,
             max(nombre_articulo) AS nombre_articulo,
             count(*) AS origen_filas,
             sum(unidades) AS origen_unidades,
             count(*) FILTER (WHERE promo_fuerte) AS origen_promo_fuerte
      FROM src.base_ventas_extendida
      WHERE fecha >= %(desde)s AND fecha < %(hasta_exclusiva)s
      """ + where + """
      GROUP BY 1, 2, 3
    ),
    enriquecida AS (
      SELECT e.fecha::date AS fecha, e.codigo_articulo, e.sucursal,
             count(*) AS enriquecida_filas,
             sum(e.unidades) AS enriquecida_unidades,
             count(*) FILTER (WHERE e.score_promo = 40) AS score_40,
             count(*) FILTER (WHERE e.score_promo = 70) AS score_70,
             count(*) FILTER (WHERE e.score_promo = 100) AS score_100,
             sum(greatest(e.venta_basal, 0)) AS basal_unidades,
             sum(greatest(e.venta_promocional, 0)) AS promocional_unidades,
             count(*) FILTER (WHERE b.mes IS NULL OR e.precio_mediano IS DISTINCT FROM b.precio_mediano
                 OR e.unidades_mediana IS DISTINCT FROM b.unidades_mediana) AS baseline_desactualizado,
             count(*) FILTER (WHERE e.score_promo IS DISTINCT FROM CASE
                 WHEN b.unidades_mediana > 0 AND e.precio < b.precio_mediano * 0.80
                      AND e.unidades > b.unidades_mediana * 2.5 THEN 100
                 WHEN b.unidades_mediana > 0 AND e.precio < b.precio_mediano * 0.90
                      AND e.unidades > b.unidades_mediana * 1.8 THEN 70
                 WHEN b.unidades_mediana > 0 AND e.precio < b.precio_mediano * 0.95
                      AND e.unidades > b.unidades_mediana * 1.3 THEN 40
                 ELSE 0 END) AS score_incorrecto,
             count(*) FILTER (WHERE e.promo_fuerte_detectada IS DISTINCT FROM
                 (b.unidades_mediana > 0 AND e.precio < b.precio_mediano * 0.80
                  AND e.unidades > b.unidades_mediana * 2.5)) AS promo_fuerte_incorrecta,
             count(*) FILTER (WHERE e.venta_basal IS DISTINCT FROM CASE
                 WHEN b.unidades_mediana > 0 AND e.precio < b.precio_mediano * 0.90
                      AND e.unidades > b.unidades_mediana * 1.8
                 THEN least(e.unidades, b.unidades_mediana) ELSE e.unidades END
                 OR e.venta_promocional IS DISTINCT FROM CASE
                 WHEN b.unidades_mediana > 0 AND e.precio < b.precio_mediano * 0.90
                      AND e.unidades > b.unidades_mediana * 1.8
                 THEN greatest(e.unidades - b.unidades_mediana, 0) ELSE 0 END)
                 AS reparto_incorrecto,
             max(e.fecha_calculo) AS enriquecida_calculada,
             max(b.fecha_calculo) AS baseline_calculado
      FROM datamart.dm_bve_ventas_enriquecidas e
      LEFT JOIN datamart.dm_bve_baseline_mensual b
        ON b.mes = date_trunc('month', e.fecha)::date
       AND b.codigo_articulo = e.codigo_articulo AND b.sucursal = e.sucursal
      WHERE e.fecha >= %(desde)s AND e.fecha < %(hasta_exclusiva)s
      """ + where.replace("AND codigo_articulo", "AND e.codigo_articulo").replace("AND sucursal", "AND e.sucursal") + """
      GROUP BY 1, 2, 3
    ),
    """ + pdd_sql + """
    pares AS (
      SELECT coalesce(o.fecha, e.fecha) fecha,
             coalesce(o.codigo_articulo, e.codigo_articulo) codigo_articulo,
             coalesce(o.sucursal, e.sucursal) sucursal,
             o.nombre_articulo, o.origen_filas, e.enriquecida_filas,
             o.origen_unidades, e.enriquecida_unidades, o.origen_promo_fuerte,
             e.score_40, e.score_70, e.score_100, e.basal_unidades,
             e.promocional_unidades, e.baseline_desactualizado,
             e.score_incorrecto, e.promo_fuerte_incorrecta, e.reparto_incorrecto,
             e.enriquecida_calculada, e.baseline_calculado
      FROM origen o FULL JOIN enriquecida e USING (fecha, codigo_articulo, sucursal)
    )
    SELECT x.fecha, x.codigo_articulo, x.sucursal, x.nombre_articulo,
           x.origen_filas, x.enriquecida_filas, x.origen_unidades,
           x.enriquecida_unidades, x.origen_promo_fuerte,
           x.score_40, x.score_70, x.score_100,
           x.basal_unidades, x.promocional_unidades,
           x.baseline_desactualizado, x.score_incorrecto,
           x.promo_fuerte_incorrecta, x.reparto_incorrecto,
           coalesce(x.origen_filas, 0) - coalesce(x.enriquecida_filas, 0) diferencia_filas,
           coalesce(x.origen_unidades, 0) - coalesce(x.enriquecida_unidades, 0) diferencia_unidades,
           p.pdd_filas, p.pdd_basal_unidades, p.pdd_promocional_unidades,
           CASE WHEN p.pdd_filas IS NOT NULL THEN p.pdd_basal_unidades - coalesce(x.basal_unidades, 0) END diferencia_pdd_basal,
           CASE WHEN p.pdd_filas IS NOT NULL THEN p.pdd_promocional_unidades - coalesce(x.promocional_unidades, 0) END diferencia_pdd_promocional,
           CASE WHEN p.pdd_filas IS NOT NULL THEN p.pdd_enriquecida_calculada < x.enriquecida_calculada END pdd_desactualizado,
           x.enriquecida_calculada, p.pdd_enriquecida_calculada, x.baseline_calculado
    FROM pares x LEFT JOIN pdd p USING (fecha, codigo_articulo, sucursal)
    ORDER BY x.fecha, x.codigo_articulo, x.sucursal
    """
    if args.solo_anomalias:
        sql = """
        SELECT * FROM (
        """ + sql + """
        ) detalle
        WHERE diferencia_filas <> 0 OR diferencia_unidades <> 0
           OR baseline_desactualizado > 0 OR score_incorrecto > 0
           OR promo_fuerte_incorrecta > 0 OR reparto_incorrecto > 0
           OR diferencia_pdd_basal <> 0 OR diferencia_pdd_promocional <> 0
           OR pdd_desactualizado
        ORDER BY fecha, codigo_articulo, sucursal
        """
    elif args.articulo is None and args.sucursal is None:
        # Un periodo completo puede tener millones de pares. Agregar en SQL
        # mantiene acotada la memoria del cliente y entrega un renglon por dia.
        sql = """
        SELECT fecha, NULL::integer codigo_articulo, NULL::integer sucursal,
               NULL::text nombre_articulo,
               sum(origen_filas) origen_filas, sum(enriquecida_filas) enriquecida_filas,
               sum(origen_unidades) origen_unidades,
               sum(enriquecida_unidades) enriquecida_unidades,
               sum(origen_promo_fuerte) origen_promo_fuerte,
               sum(score_40) score_40, sum(score_70) score_70,
               sum(score_100) score_100, sum(basal_unidades) basal_unidades,
               sum(promocional_unidades) promocional_unidades,
               sum(baseline_desactualizado) baseline_desactualizado,
               sum(score_incorrecto) score_incorrecto,
               sum(promo_fuerte_incorrecta) promo_fuerte_incorrecta,
               sum(reparto_incorrecto) reparto_incorrecto,
               sum(diferencia_filas) diferencia_filas,
               sum(diferencia_unidades) diferencia_unidades,
               sum(pdd_filas) pdd_filas,
               sum(pdd_basal_unidades) pdd_basal_unidades,
               sum(pdd_promocional_unidades) pdd_promocional_unidades,
               sum(diferencia_pdd_basal) diferencia_pdd_basal,
               sum(diferencia_pdd_promocional) diferencia_pdd_promocional,
               bool_or(pdd_desactualizado) pdd_desactualizado,
               max(enriquecida_calculada) enriquecida_calculada,
               max(pdd_enriquecida_calculada) pdd_enriquecida_calculada,
               max(baseline_calculado) baseline_calculado
        FROM (
        """ + sql + """
        ) detalle
        GROUP BY fecha ORDER BY fecha
        """
    return sql, params


def anomalies(row: dict) -> list[str]:
    flags = []
    for key in ("diferencia_filas", "diferencia_unidades", "baseline_desactualizado",
                "score_incorrecto", "promo_fuerte_incorrecta", "reparto_incorrecto",
                "diferencia_pdd_basal", "diferencia_pdd_promocional"):
        if row[key]:
            flags.append(key)
    if row["pdd_desactualizado"]:
        flags.append("pdd_desactualizado")
    return flags


def main(argv: list[str] | None = None) -> int:
    parser = args_parser()
    args = parser.parse_args(argv)
    try:
        validate_args(args)
        settings = db_settings()
        sql, params = query(args)
        with psycopg2.connect(host=settings["PG_HOST"], port=settings["PG_PORT"],
                              dbname=settings["PG_DB"], user=settings["PG_USER"],
                              password=settings["PG_PASSWORD"], connect_timeout=10,
                              application_name="etl_diarco:auditar_ventas_ofertas") as conn:
            conn.set_session(readonly=True)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET LOCAL statement_timeout = %s", (args.timeout_segundos * 1000,))
                cur.execute(sql, params)
                rows = [dict(row) for row in cur.fetchall()]
        if args.csv:
            with args.csv.open("w", newline="", encoding="utf-8-sig") as output:
                writer = csv.DictWriter(output, fieldnames=FIELDS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
        bad = [(row, anomalies(row)) for row in rows]
        bad = [(row, flags) for row, flags in bad if flags]
        label = "Dias" if args.articulo is None and args.sucursal is None and not args.solo_anomalias else "Pares fecha/articulo/sucursal"
        print(f"{label}: {len(rows)}; con diferencias: {len(bad)}")
        print("Totales:")
        for key in ("origen_filas", "enriquecida_filas", "score_40", "score_70",
                    "score_100", "baseline_desactualizado", "score_incorrecto",
                    "promo_fuerte_incorrecta", "reparto_incorrecto"):
            print(f"  {key}: {sum(row[key] or 0 for row in rows)}")
        for row, flags in bad[:50]:
            identifier = f"{row['fecha']}"
            if row["codigo_articulo"] is not None:
                identifier += f" articulo={row['codigo_articulo']} sucursal={row['sucursal']}"
            print(f"  {identifier}: {', '.join(flags)}")
        if len(bad) > 50:
            print(f"  ... {len(bad) - 50} mas; use --csv para el detalle completo")
        if args.csv:
            print(f"CSV: {args.csv}")
        return 0
    except (ValueError, OSError, psycopg2.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
