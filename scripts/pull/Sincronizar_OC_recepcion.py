# -*- coding: utf-8 -*-
"""
sincronizar_oc_recepcion_pg.py

Sincroniza Órdenes de Compra (cabecera y líneas) desde PostgreSQL diarco_data (DMZ)
hacia PostgreSQL Connexa (TEST / DESA / PROD), usando:
- Extracción por cursor (fetchmany)
- Archivos TSV temporales
- COPY a tablas temporales
- UPSERT (ON CONFLICT) idempotente

Estrategia operativa:
- Un único script.
- Tres despliegues en Prefect (Opción 1): uno por ambiente.
- El ambiente se define por parámetro "dest_env" (TEST | DESA | PROD),
  que resuelve el prefijo de variables de entorno: PGT_ / PGD_ / PGP_.

Variables de entorno esperadas (destino):
  - TEST: PGT_HOST, PGT_PORT, PGT_DB, PGT_USER, PGT_PASSWORD, PGT_SCHEMA, (opcional PGT_SSLMODE)
  - DESA: PGD_HOST, PGD_PORT, PGD_DB, PGD_USER, PGD_PASSWORD, PGD_SCHEMA, (opcional PGD_SSLMODE)
  - PROD: PGP_HOST, PGP_PORT, PGP_DB, PGP_USER, PGP_PASSWORD, PGP_SCHEMA, (opcional PGP_SSLMODE)

Origen diarco_data (siempre):
  - PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, (opcional PG_SSLMODE)

Otras variables opcionales:
  - ETL_ENV_PATH: path a .env
  - OC_SYNC_SOURCE_SCHEMA: default "repl"
  - OC_SYNC_BATCH_FETCH: default 5000
  - OC_SYNC_STRICT_MODE: default false
  - OC_SYNC_STRICT_COMPARE: default true
  - OC_SYNC_SINCE_DAYS: default "90" (solo últimos 90 días; vacío para full)
  - OC_SYNC_UUID_NAMESPACE: default "12345678-1234-5678-1234-567812345678"
  - FOLDER_TMP: default "data/tmp"
"""

from __future__ import annotations

import csv
import os
import uuid
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, Set

from dotenv import load_dotenv
import psycopg  # psycopg v3

from prefect import flow, task, get_run_logger


# ------------------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("oc_sync_pg")


# ------------------------------------------------------------------------------
# HELPERS ENV
# ------------------------------------------------------------------------------
def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "t", "yes", "y", "si", "sí")


def _pick(*keys: str, default: Optional[str] = None) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return v
    return default


def _require_env(keys: list[str], prefix_label: str) -> None:
    missing = [k for k in keys if not _pick(k)]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno para {prefix_label}: {', '.join(missing)}")


# ------------------------------------------------------------------------------
# CONFIG DATACLASSES
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class PgConnInfo:
    host: str
    port: int
    db: str
    user: str
    password: str
    sslmode: str = "prefer"


@dataclass(frozen=True)
class SyncConfig:
    source_schema: str
    target_schema: str
    batch_fetch: int
    strict_mode: bool
    strict_compare: bool
    since_days: Optional[int]
    tmp_dir: Path


def pg_dsn(ci: PgConnInfo) -> str:
    # No loguear este DSN (incluye password)
    return (
        f"host={ci.host} port={ci.port} dbname={ci.db} user={ci.user} "
        f"password={ci.password} sslmode={ci.sslmode}"
    )


# ------------------------------------------------------------------------------
# IDENTITY / HELPERS
# ------------------------------------------------------------------------------
def _load_uuid_namespace() -> uuid.UUID:
    ns = _pick("OC_SYNC_UUID_NAMESPACE", default="12345678-1234-5678-1234-567812345678")
    return uuid.UUID(ns)


UUID_NAMESPACE = _load_uuid_namespace()


def uuid_v5(key: str) -> uuid.UUID:
    return uuid.uuid5(UUID_NAMESPACE, key)


def make_ext_code(c_oc: int, pref: int, suf: int) -> str:
    return f"{c_oc}-{pref}-{suf}"


def make_purchase_number(c_oc: int, pref: int, suf: int) -> int:
    # BIGINT estable y único
    return (c_oc * 100000000000) + (pref * 100000000) + suf


def is_valid_dt(dt: Optional[datetime]) -> bool:
    return dt is not None and getattr(dt, "year", 0) > 1900


def dt_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------------------
# CARGA .ENV + RESOLUCIÓN DE AMBIENTE DESTINO
# ------------------------------------------------------------------------------
def load_dotenv_safe() -> None:
    """
    Carga el .env definido por ETL_ENV_PATH, o por defecto busca .env en el cwd.
    """
    env_path = _pick("ETL_ENV_PATH", default=None)
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()


def load_source_pg() -> PgConnInfo:
    """
    Origen diarco_data: siempre por variables PG_*
    """
    _require_env(["PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD"], "ORIGEN (PG_*)")
    return PgConnInfo(
        host=_pick("PG_HOST") or "",
        port=int(_pick("PG_PORT", default="5432") or "5432"),
        db=_pick("PG_DB") or "",
        user=_pick("PG_USER") or "",
        password=_pick("PG_PASSWORD") or "",
        sslmode=_pick("PG_SSLMODE", default="prefer") or "prefer",
    )


def load_dest_pg_by_env(dest_env: str) -> Tuple[PgConnInfo, str, str]:
    """
    Devuelve (conn_info, target_schema, prefix) según dest_env:
      TEST -> PGT_*
      DESA -> PGD_*
      PROD -> PGP_*
    """
    env = (dest_env or "").strip().upper()
    if env not in ("TEST", "DESA", "PROD"):
        raise ValueError("dest_env inválido. Valores permitidos: TEST | DESA | PROD")

    prefix = {"TEST": "PGT", "DESA": "PGD", "PROD": "PGP"}[env]

    required = [
        f"{prefix}_HOST",
        f"{prefix}_PORT",
        f"{prefix}_DB",
        f"{prefix}_USER",
        f"{prefix}_PASSWORD",
        f"{prefix}_SCHEMA",
    ]
    _require_env(required, f"DESTINO {env} ({prefix}_*)")

    host = _pick(f"{prefix}_HOST") or ""
    port = int(_pick(f"{prefix}_PORT", default="5432") or "5432")
    db = _pick(f"{prefix}_DB") or ""
    user = _pick(f"{prefix}_USER") or ""
    password = _pick(f"{prefix}_PASSWORD") or ""
    schema = _pick(f"{prefix}_SCHEMA", default="procurement_and_sourcing") or "procurement_and_sourcing"
    sslmode = _pick(f"{prefix}_SSLMODE", default="prefer") or "prefer"

    return (
        PgConnInfo(host=host, port=port, db=db, user=user, password=password, sslmode=sslmode),
        schema,
        prefix,
    )


def load_sync_config(target_schema: str) -> SyncConfig:
    tmp_dir = Path(_pick("FOLDER_TMP", default="data/tmp") or "data/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Por defecto 90 días (si se desea FULL, setear OC_SYNC_SINCE_DAYS vacío)
    since_days_raw = _pick("OC_SYNC_SINCE_DAYS", default="90")
    since_days = int(since_days_raw) if (since_days_raw and since_days_raw.strip().isdigit()) else None

    return SyncConfig(
        # IMPORTANTE: en su caso el origen real es "repl"
        source_schema=_pick("OC_SYNC_SOURCE_SCHEMA", default="repl") or "repl",
        target_schema=target_schema,
        batch_fetch=int(_pick("OC_SYNC_BATCH_FETCH", default="5000") or "5000"),
        strict_mode=_env_bool("OC_SYNC_STRICT_MODE", default=False),
        strict_compare=_env_bool("OC_SYNC_STRICT_COMPARE", default=True),
        since_days=since_days,
        tmp_dir=tmp_dir,
    )


# ------------------------------------------------------------------------------
# LOOKUPS en DESTINO (Connexa)
# ------------------------------------------------------------------------------
def load_supplier_map(dst_conn, target_schema: str) -> Dict[str, str]:
    q = f"SELECT id::text, ext_code::text FROM {target_schema}.pas_supplier"
    m: Dict[str, str] = {}
    with dst_conn.cursor() as cur:
        cur.execute(q)
        for _id, ext in cur.fetchall():
            if ext is not None:
                m[str(ext).strip()] = str(_id)
    return m


def load_site_map(dst_conn, target_schema: str) -> Dict[str, str]:
    q = f"SELECT id::text, code::text FROM {target_schema}.pas_site"
    m: Dict[str, str] = {}
    with dst_conn.cursor() as cur:
        cur.execute(q)
        for _id, code in cur.fetchall():
            if code is not None:
                m[str(code).strip()] = str(_id)
    return m


# ------------------------------------------------------------------------------
# EXTRACT ORIGEN (diarco_data) -> TSV
# ------------------------------------------------------------------------------

def export_purchase_orders_to_tsv(
    src_conn,
    cfg: SyncConfig,
    supplier_map: Dict[str, str],
    site_map: Dict[str, str],
    tmp_po_tsv: Path,
) -> Tuple[int, Set[str], Set[str], Set[str]]:
    """
    Exporta cabeceras de OC a TSV.

    Devuelve:
      - written: cantidad de OCs exportadas (válidas)
      - missing_suppliers: set de códigos de proveedor no mapeados
      - missing_sites: set de códigos de site no mapeados
      - valid_po_ids: set de UUIDs de OCs efectivamente exportadas (para filtrar líneas)
    """
    missing_suppliers: Set[str] = set()
    missing_sites: Set[str] = set()
    valid_po_ids: Set[str] = set()

    written = 0
    skipped_supplier = 0
    skipped_site = 0

    where = ""
    params = {}
    if cfg.since_days is not None:
        since = datetime.now() - timedelta(days=cfg.since_days)
        # + c_situac=1 (si su negocio lo requiere; si no, quitarlo)
        where = "WHERE COALESCE(f_modifico, f_alta_sist) >= %(since)s AND c_situac = 1"
        params["since"] = since
    else:
        # FULL sync, igual se respeta c_situac=1 si corresponde
        where = "WHERE c_situac = 1"

    # Casteo a numeric para evitar strings formateados si en origen son money
    sql = f"""
        SELECT
            c_oc,
            u_prefijo_oc,
            u_sufijo_oc,
            c_proveedor,
            c_sucu_destino,
            f_alta_sist,
            f_emision,
            f_entrega,
            f_modifico,
            i_neto_oc::numeric        as i_neto_oc,
            i_iva_oc::numeric         as i_iva_oc,
            i_imp_interno_oc::numeric as i_imp_interno_oc,
            i_total_oc::numeric       as i_total_oc
        FROM {cfg.source_schema}.t080_oc_cabe
        {where}
    """

    with src_conn.cursor() as cur, open(tmp_po_tsv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        cur.execute(sql, params)

        while True:
            rows = cur.fetchmany(cfg.batch_fetch)
            if not rows:
                break

            for r in rows:
                c_oc = int(r[0])
                pref = int(r[1])
                suf = int(r[2])

                c_prov = str(int(r[3])) if r[3] is not None else ""
                c_dest = str(int(r[4])) if r[4] is not None else ""

                ext_code = make_ext_code(c_oc, pref, suf)
                purchase_number = make_purchase_number(c_oc, pref, suf)
                po_id = str(uuid_v5(f"T080|{ext_code}"))

                supplier_id = supplier_map.get(c_prov)
                if not supplier_id:
                    missing_suppliers.add(c_prov)
                    if cfg.strict_mode:
                        raise RuntimeError(f"Proveedor faltante en pas_supplier.ext_code: {c_prov} (OC={ext_code})")
                    skipped_supplier += 1
                    continue

                destination_site_id = site_map.get(c_dest)
                if not destination_site_id:
                    missing_sites.add(c_dest)
                    if cfg.strict_mode:
                        raise RuntimeError(f"Site faltante en pas_site.code: {c_dest} (OC={ext_code})")
                    skipped_site += 1
                    continue

                f_alta = r[5]
                f_emision = r[6]
                f_entrega = r[7]
                f_modif = r[8]
                ts = f_modif if is_valid_dt(f_modif) else f_alta

                neto = float(r[9] or 0)
                iva = float(r[10] or 0)
                imp_int = float(r[11] or 0)
                total = float(r[12] or 0)
                total_tax = iva + imp_int

                w.writerow([
                    po_id,
                    dt_to_str(ts) if ts else r"\N",
                    purchase_number,
                    destination_site_id,
                    supplier_id,
                    ext_code,
                    dt_to_str(f_alta) if f_alta else r"\N",
                    dt_to_str(f_emision) if f_emision else r"\N",
                    dt_to_str(f_entrega) if f_entrega else r"\N",
                    neto,
                    total,
                    total_tax,
                ])

                valid_po_ids.add(po_id)
                written += 1

    log.info(
        "Export PO: written=%s | skipped_supplier=%s | skipped_site=%s | unique_valid_po_ids=%s",
        written, skipped_supplier, skipped_site, len(valid_po_ids)
    )
    if missing_suppliers:
        log.warning("Export PO: proveedores faltantes (muestra): %s", sorted(list(missing_suppliers))[:30])
    if missing_sites:
        log.warning("Export PO: sites faltantes (muestra): %s", sorted(list(missing_sites))[:30])

    return written, missing_suppliers, missing_sites, valid_po_ids


def export_purchase_order_lines_to_tsv(
    src_conn,
    cfg: SyncConfig,
    tmp_line_tsv: Path,
    valid_po_ids: Set[str],
) -> int:
    """
    Exporta líneas de OC a TSV.

    Solo exporta líneas cuyo purchase_order_id esté presente en valid_po_ids,
    evitando violaciones de FK en destino cuando una cabecera fue omitida.
    """
    written = 0
    skipped_no_header = 0

    where = ""
    params = {}
    if cfg.since_days is not None:
        since = datetime.now() - timedelta(days=cfg.since_days)
        where = "WHERE COALESCE(c.f_modifico, c.f_alta_sist) >= %(since)s"
        params["since"] = since

    sql = f"""
        SELECT
            d.c_oc,
            d.u_prefijo_oc,
            d.u_sufijo_oc,
            d.c_articulo,
            d.q_bultos_empr_ped,
            d.i_precio_compra::numeric as i_precio_compra,
            d.i_total_item::numeric    as i_total_item,
            c.f_alta_sist,
            c.f_modifico
        FROM {cfg.source_schema}.t081_oc_deta d
        JOIN {cfg.source_schema}.t080_oc_cabe c
          ON c.c_oc = d.c_oc
         AND c.u_prefijo_oc = d.u_prefijo_oc
         AND c.u_sufijo_oc = d.u_sufijo_oc
        {where}
    """

    with src_conn.cursor() as cur, open(tmp_line_tsv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        cur.execute(sql, params)

        while True:
            rows = cur.fetchmany(cfg.batch_fetch)
            if not rows:
                break

            for r in rows:
                c_oc = int(r[0])
                pref = int(r[1])
                suf = int(r[2])
                art = int(r[3])

                ext_code = make_ext_code(c_oc, pref, suf)
                po_id = str(uuid_v5(f"T080|{ext_code}"))

                if po_id not in valid_po_ids:
                    skipped_no_header += 1
                    continue

                line_id = str(uuid_v5(f"T081|{ext_code}|{art}"))

                f_alta = r[7]
                f_modif = r[8]
                ts = f_modif if is_valid_dt(f_modif) else f_alta

                sku = str(art)
                qty = float(r[4] or 0)
                unit_price = float(r[5] or 0)
                total_price = float(r[6] or 0)

                w.writerow([
                    line_id,
                    dt_to_str(ts) if ts else r"\N",
                    po_id,
                    sku,
                    qty,
                    unit_price,
                    total_price,
                    10,
                ])

                written += 1

    log.info("Export LINES: written=%s | skipped_no_header=%s", written, skipped_no_header)
    return written


# ------------------------------------------------------------------------------
# DESTINO: COPY a temporales + UPSERT
# ------------------------------------------------------------------------------
def copy_file_to_table(cur, copy_sql: str, filepath) -> None:
    """
    COPY robusto: acepta Path o str.
    """
    fp = str(filepath)
    with open(fp, "r", encoding="utf-8") as f:
        with cur.copy(copy_sql) as copy:
            for chunk in iter(lambda: f.read(1024 * 1024), ""):
                copy.write(chunk)


def load_and_upsert_with_counts(
    pg_conn,
    cfg: SyncConfig,
    tmp_po_tsv: Path,
    tmp_line_tsv: Path,
) -> Tuple[int, int, int, int]:
    """
    Devuelve:
      - src_po_count (tmp_po)
      - dst_po_count (subset afectado)
      - src_line_count (tmp_pol)
      - dst_line_count (subset afectado)
    """
    target_schema = cfg.target_schema

    with pg_conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE tmp_po (
                id uuid,
                "timestamp" timestamp without time zone,
                purchase_number bigint,
                destination_site_id uuid,
                supplier_id uuid,
                ext_code varchar,
                creation_date timestamp without time zone,
                purchase_order_date timestamp without time zone,
                expected_receipt_date timestamp without time zone,
                total_amount_whith_tax_excluded double precision,
                total_amount_whith_tax_included double precision,
                total_tax_amount double precision
            ) ON COMMIT DROP;
        """)

        cur.execute("""
            CREATE TEMP TABLE tmp_pol (
                id uuid,
                "timestamp" timestamp without time zone,
                purchase_order_id uuid,
                sku varchar,
                quantity double precision,
                unit_price double precision,
                total_price double precision,
                line_type bigint
            ) ON COMMIT DROP;
        """)

        copy_po_sql = r"""
            COPY tmp_po (
                id, "timestamp", purchase_number, destination_site_id, supplier_id, ext_code,
                creation_date, purchase_order_date, expected_receipt_date,
                total_amount_whith_tax_excluded, total_amount_whith_tax_included, total_tax_amount
            )
            FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N');
        """
        copy_file_to_table(cur, copy_po_sql, tmp_po_tsv)

        copy_pol_sql = r"""
            COPY tmp_pol (
                id, "timestamp", purchase_order_id, sku, quantity, unit_price, total_price, line_type
            )
            FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N');
        """
        copy_file_to_table(cur, copy_pol_sql, tmp_line_tsv)

        cur.execute("SELECT COUNT(*) FROM tmp_po;")
        src_po_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM tmp_pol;")
        src_line_count = cur.fetchone()[0]

        # UPSERT cabeceras por purchase_number (debe ser UNIQUE en destino)
        cur.execute(f"""
            INSERT INTO {target_schema}.pas_purchase_order (
                id, "timestamp", purchase_number, destination_site_id, supplier_id, ext_code,
                creation_date, purchase_order_date, expected_receipt_date,
                total_amount_whith_tax_excluded, total_amount_whith_tax_included, total_tax_amount
            )
            SELECT
                id, "timestamp", purchase_number, destination_site_id, supplier_id, ext_code,
                creation_date, purchase_order_date, expected_receipt_date,
                total_amount_whith_tax_excluded, total_amount_whith_tax_included, total_tax_amount
            FROM tmp_po
            ON CONFLICT (purchase_number) DO UPDATE SET
                id = EXCLUDED.id,
                "timestamp" = EXCLUDED."timestamp",
                destination_site_id = EXCLUDED.destination_site_id,
                supplier_id = EXCLUDED.supplier_id,
                ext_code = EXCLUDED.ext_code,
                creation_date = EXCLUDED.creation_date,
                purchase_order_date = EXCLUDED.purchase_order_date,
                expected_receipt_date = EXCLUDED.expected_receipt_date,
                total_amount_whith_tax_excluded = EXCLUDED.total_amount_whith_tax_excluded,
                total_amount_whith_tax_included = EXCLUDED.total_amount_whith_tax_included,
                total_tax_amount = EXCLUDED.total_tax_amount;
        """)

        # UPSERT líneas por id (UUID determinístico)
        cur.execute(f"""
            INSERT INTO {target_schema}.pas_purchase_order_line (
                id, "timestamp", purchase_order_id, sku, quantity, unit_price, total_price, line_type
            )
            SELECT
                id, "timestamp", purchase_order_id, sku, quantity, unit_price, total_price, line_type
            FROM tmp_pol
            ON CONFLICT (id) DO UPDATE SET
                "timestamp" = EXCLUDED."timestamp",
                purchase_order_id = EXCLUDED.purchase_order_id,
                sku = EXCLUDED.sku,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_price = EXCLUDED.total_price,
                line_type = EXCLUDED.line_type;
        """)

        cur.execute(f"""
            SELECT COUNT(*)
            FROM {target_schema}.pas_purchase_order p
            JOIN tmp_po t ON t.purchase_number = p.purchase_number;
        """)
        dst_po_count = cur.fetchone()[0]

        cur.execute(f"""
            SELECT COUNT(*)
            FROM {target_schema}.pas_purchase_order_line l
            JOIN tmp_pol t ON t.id = l.id;
        """)
        dst_line_count = cur.fetchone()[0]

    return src_po_count, dst_po_count, src_line_count, dst_line_count


# ------------------------------------------------------------------------------
# CORE RUNNER (PG -> PG)
# ------------------------------------------------------------------------------
def run_sync(dest_env: str) -> None:
    """
    Ejecuta la sincronización:
      - Origen: PostgreSQL diarco_data (tablas t080/t081 en cfg.source_schema)
      - Destino: PostgreSQL Connexa (según dest_env: TEST/DESA/PROD)
      - Exporta a TSV temporales, carga con COPY a tablas temp, UPSERT a destino.
      - Garantiza integridad: solo exporta líneas de OCs cuyas cabeceras fueron exportadas (valid_po_ids).
    """
    log.info("Iniciando sincronización OC (PG->PG) para dest_env=%s ...", dest_env)

    load_dotenv_safe()

    src_ci = load_source_pg()
    dst_ci, target_schema, prefix = load_dest_pg_by_env(dest_env)
    cfg = load_sync_config(target_schema)

    src_conn = None
    dst_conn = None

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp_po_tsv = cfg.tmp_dir / f"tmp_po_{dest_env.lower()}_{stamp}.tsv"
    tmp_line_tsv = cfg.tmp_dir / f"tmp_pol_{dest_env.lower()}_{stamp}.tsv"

    log.info("Conectando ORIGEN: host=%s db=%s schema=%s", src_ci.host, src_ci.db, cfg.source_schema)
    log.info("Conectando DESTINO(%s): host=%s db=%s schema=%s", prefix, dst_ci.host, dst_ci.db, cfg.target_schema)

    try:
        src_conn = psycopg.connect(pg_dsn(src_ci), connect_timeout=15)
        src_conn.autocommit = False

        dst_conn = psycopg.connect(pg_dsn(dst_ci), connect_timeout=15)
        dst_conn.autocommit = False

        log.info(
            "Sincronización OC: ORIGEN=%s:%s/%s | DESTINO(%s)=%s:%s/%s | TARGET_SCHEMA=%s",
            src_ci.host, src_ci.port, src_ci.db,
            prefix, dst_ci.host, dst_ci.port, dst_ci.db,
            cfg.target_schema,
        )

        # Lookups destino
        supplier_map = load_supplier_map(dst_conn, cfg.target_schema)
        site_map = load_site_map(dst_conn, cfg.target_schema)
        log.info("Mapas destino: suppliers=%s | sites=%s", len(supplier_map), len(site_map))

        # Export cabeceras (devuelve valid_po_ids)
        po_count, miss_sup, miss_site, valid_po_ids = export_purchase_orders_to_tsv(
            src_conn=src_conn,
            cfg=cfg,
            supplier_map=supplier_map,
            site_map=site_map,
            tmp_po_tsv=tmp_po_tsv,
        )
        log.info("PO exportadas (válidas): %s | valid_po_ids=%s", po_count, len(valid_po_ids))

        if miss_sup:
            log.warning("Proveedores faltantes (muestra): %s", sorted(list(miss_sup))[:30])
        if miss_site:
            log.warning("Sites faltantes (muestra): %s", sorted(list(miss_site))[:30])

        if po_count == 0 or not valid_po_ids:
            log.warning("No hay OCs válidas para sincronizar. Finalizando sin carga.")
            dst_conn.rollback()
            return

        # Export líneas SOLO de cabeceras válidas
        pol_count = export_purchase_order_lines_to_tsv(
            src_conn=src_conn,
            cfg=cfg,
            tmp_line_tsv=tmp_line_tsv,
            valid_po_ids=valid_po_ids,
        )
        log.info("Líneas exportadas (válidas): %s", pol_count)

        # Load + upsert (una transacción destino)
        src_po, dst_po, src_ln, dst_ln = load_and_upsert_with_counts(
            pg_conn=dst_conn,
            cfg=cfg,
            tmp_po_tsv=tmp_po_tsv,
            tmp_line_tsv=tmp_line_tsv,
        )

        log.info("Comparativa PO | origen(tmp_po)=%s vs destino(subset)=%s", src_po, dst_po)
        log.info("Comparativa LN | origen(tmp_pol)=%s vs destino(subset)=%s", src_ln, dst_ln)

        if cfg.strict_compare:
            if src_po != dst_po:
                raise RuntimeError(f"Mismatch PO: origen={src_po} destino={dst_po}. Rollback.")
            if src_ln != dst_ln:
                raise RuntimeError(f"Mismatch LINES: origen={src_ln} destino={dst_ln}. Rollback.")

        dst_conn.commit()
        log.info("Sincronización finalizada OK (COMMIT).")

    except Exception as e:
        try:
            if dst_conn is not None:
                dst_conn.rollback()
        except Exception:
            pass
        log.error("ERROR en sincronización OC: %s", e, exc_info=True)
        raise

    finally:
        try:
            if src_conn is not None:
                src_conn.close()
        except Exception:
            pass
        try:
            if dst_conn is not None:
                dst_conn.close()
        except Exception:
            pass

        # Limpieza temporales
        for p in (tmp_po_tsv, tmp_line_tsv):
            try:
                if isinstance(p, Path) and p.exists():
                    p.unlink()
            except Exception:
                pass


# ------------------------------------------------------------------------------
# PREFECT (Opción 1: un despliegue por ambiente)
# ------------------------------------------------------------------------------
@task(name="sync_oc_recepcion_pg_core", retries=1, retry_delay_seconds=30)
def sync_task(dest_env: str) -> None:
    logger = get_run_logger()
    logger.info("Iniciando sincronización OC (PG->PG) para dest_env=%s ...", dest_env)
    run_sync(dest_env)
    logger.info("Sincronización OC finalizada OK para dest_env=%s.", dest_env)


# IMPORTANTE: Prefect no admite ciertos caracteres en el name. Evitar "<", ">", "&", "%", "/".
@flow(name="sync_oc_recepcion_diarco_data_to_connexa_pg")
def sync_flow(dest_env: str = "PROD") -> None:
    """
    Para Opción 1, se crean 3 despliegues (TEST/DESA/PROD) fijando el parámetro dest_env.
    """
    sync_task(dest_env)


# ------------------------------------------------------------------------------
# CLI ENTRYPOINT (ejecución manual)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    dest_env = os.getenv("DEST_ENV", "PROD")
    run_sync(dest_env)
