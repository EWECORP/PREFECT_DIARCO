import csv
import os
import uuid
import logging
from datetime import datetime

import pyodbc
import psycopg

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("oc_sync")

# =========================
# CONFIG
# =========================

#ORIGEN ==================== NECESARIA VPN DE DIARCO CONECTADA.
SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=10.54.200.92,1433;"
    "DATABASE=data-sync;"
    "UID=eettlin;"
    "PWD=lOc4l_eXt$24;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Trusted_Connection=no;"
    "Authentication=SqlPassword;"
)

#DESTINO
POSTGRES_DSN = "host=186.158.182.102 port=5432 dbname=connexa_platform user=connexa_platform_user password=Aladelta10$ sslmode=require"


SCHEMA = "procurement_and_sourcing"
BATCH_FETCH = 5000

# UUID determinísticos (idempotencia)
UUID_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")

# Si True: aborta cuando encuentra supplier/site faltante
STRICT_MODE = False

# Si True: si los conteos no coinciden, levanta error y rollback (recomendado)
STRICT_COMPARE = True

# Archivos temporales
TMP_PO_TSV = "tmp_po.tsv"
TMP_LINE_TSV = "tmp_po_line.tsv"

# =========================
# HELPERS
# =========================

def make_ext_code(c_oc: int, pref: int, suf: int) -> str:
    return f"{c_oc}-{pref}-{suf}"

def make_purchase_number(c_oc: int, pref: int, suf: int) -> int:
    # BIGINT estable y único
    return (c_oc * 100000000000) + (pref * 100000000) + suf

def uuid_v5(key: str) -> uuid.UUID:
    return uuid.uuid5(UUID_NAMESPACE, key)

def is_valid_dt(dt: datetime) -> bool:
    return dt is not None and dt.year > 1900

def dt_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# =========================
# LOOKUPS (PG)
# =========================

def load_supplier_map(pg_conn) -> dict[str, str]:
    q = f"SELECT id::text, ext_code::text FROM {SCHEMA}.pas_supplier"
    m = {}
    with pg_conn.cursor() as cur:
        cur.execute(q)
        for _id, ext in cur.fetchall():
            if ext is not None:
                m[ext.strip()] = _id
    return m

def load_site_map(pg_conn) -> dict[str, str]:
    q = f"SELECT id::text, code::text FROM {SCHEMA}.pas_site"
    m = {}
    with pg_conn.cursor() as cur:
        cur.execute(q)
        for _id, code in cur.fetchall():
            if code is not None:
                m[code.strip()] = _id
    return m

# =========================
# EXTRACT -> TSV (PO)
# =========================

def export_purchase_orders_to_tsv(sql_conn, supplier_map, site_map) -> tuple[int, set[str], set[str]]:
    missing_suppliers = set()
    missing_sites = set()
    written = 0

    sql = """
    SELECT
        C_OC,
        U_PREFIJO_OC,
        U_SUFIJO_OC,
        C_PROVEEDOR,
        C_SUCU_DESTINO,
        F_ALTA_SIST,
        F_EMISION,
        F_ENTREGA,
        F_MODIFICO,
        I_NETO_OC,
        I_IVA_OC,
        I_IMP_INTERNO_OC,
        I_TOTAL_OC
    FROM repl.T080_OC_CABE
    """

    with sql_conn.cursor() as cur, open(TMP_PO_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

        cur.execute(sql)

        while True:
            rows = cur.fetchmany(BATCH_FETCH)
            if not rows:
                break

            for r in rows:
                c_oc = int(r.C_OC)
                pref = int(r.U_PREFIJO_OC)
                suf = int(r.U_SUFIJO_OC)
                c_prov = str(int(r.C_PROVEEDOR))
                c_dest = str(int(r.C_SUCU_DESTINO))

                ext_code = make_ext_code(c_oc, pref, suf)
                purchase_number = make_purchase_number(c_oc, pref, suf)
                po_id = str(uuid_v5(f"T080|{ext_code}"))

                supplier_id = supplier_map.get(c_prov)
                if not supplier_id:
                    missing_suppliers.add(c_prov)
                    if STRICT_MODE:
                        raise RuntimeError(f"Proveedor faltante en pas_supplier.ext_code: {c_prov}")

                destination_site_id = site_map.get(c_dest)
                if not destination_site_id:
                    missing_sites.add(c_dest)
                    if STRICT_MODE:
                        raise RuntimeError(f"Site faltante en pas_site.code: {c_dest}")

                f_alta = r.F_ALTA_SIST
                f_emision = r.F_EMISION
                f_entrega = r.F_ENTREGA
                f_modif = r.F_MODIFICO

                ts = f_modif if is_valid_dt(f_modif) else f_alta

                neto = float(r.I_NETO_OC)
                iva = float(r.I_IVA_OC)
                imp_int = float(r.I_IMP_INTERNO_OC)
                total = float(r.I_TOTAL_OC)
                total_tax = iva + imp_int

                w.writerow([
                    po_id,                              # id
                    dt_to_str(ts),                      # "timestamp"
                    purchase_number,                    # purchase_number
                    destination_site_id,                # destination_site_id
                    supplier_id,                        # supplier_id
                    ext_code,                           # ext_code
                    dt_to_str(f_alta) if f_alta else r"\N",        # creation_date
                    dt_to_str(f_emision) if f_emision else r"\N",  # purchase_order_date
                    dt_to_str(f_entrega) if f_entrega else r"\N",  # expected_receipt_date
                    neto,                               # total_amount_whith_tax_excluded
                    total,                              # total_amount_whith_tax_included
                    total_tax,                          # total_tax_amount
                ])
                written += 1

    return written, missing_suppliers, missing_sites

# =========================
# EXTRACT -> TSV (LINES)
# =========================

def export_purchase_order_lines_to_tsv(sql_conn) -> int:
    written = 0

    sql = """
    SELECT
        d.C_OC,
        d.U_PREFIJO_OC,
        d.U_SUFIJO_OC,
        d.C_ARTICULO,
        d.Q_BULTOS_EMPR_PED,
        d.I_PRECIO_COMPRA,
        d.I_TOTAL_ITEM,
        c.F_ALTA_SIST,
        c.F_MODIFICO
    FROM repl.T081_OC_DETA d
    JOIN repl.T080_OC_CABE c
      ON c.C_OC = d.C_OC
     AND c.U_PREFIJO_OC = d.U_PREFIJO_OC
     AND c.U_SUFIJO_OC = d.U_SUFIJO_OC
    """

    with sql_conn.cursor() as cur, open(TMP_LINE_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

        cur.execute(sql)

        while True:
            rows = cur.fetchmany(BATCH_FETCH)
            if not rows:
                break

            for r in rows:
                c_oc = int(r.C_OC)
                pref = int(r.U_PREFIJO_OC)
                suf = int(r.U_SUFIJO_OC)
                art = int(r.C_ARTICULO)

                ext_code = make_ext_code(c_oc, pref, suf)

                po_id = str(uuid_v5(f"T080|{ext_code}"))
                line_id = str(uuid_v5(f"T081|{ext_code}|{art}"))

                ts = r.F_MODIFICO if is_valid_dt(r.F_MODIFICO) else r.F_ALTA_SIST

                sku = str(art)
                qty = float(r.Q_BULTOS_EMPR_PED)
                unit_price = float(r.I_PRECIO_COMPRA)
                total_price = float(r.I_TOTAL_ITEM)

                w.writerow([
                    line_id,                 # id
                    dt_to_str(ts),           # "timestamp"
                    po_id,                   # purchase_order_id
                    sku,                     # sku
                    qty,                     # quantity
                    unit_price,              # unit_price
                    total_price,             # total_price
                    10,                      # line_type (Item)
                ])
                written += 1

    return written

# =========================
# PG COPY helpers (psycopg v3 robust)
# =========================

def copy_file_to_table(cur, copy_sql: str, filepath: str) -> None:
    with open(filepath, "r", encoding="utf-8") as f:
        with cur.copy(copy_sql) as copy:
            for chunk in iter(lambda: f.read(1024 * 1024), ""):
                copy.write(chunk)

# =========================
# LOAD TO POSTGRES (COPY + UPSERT + COUNTS)
# =========================

def load_and_upsert_with_counts(pg_conn) -> tuple[int, int, int, int]:
    """
    Devuelve:
      - src_po_count (desde tmp_po)
      - dst_po_count (presentes en pas_purchase_order para ese subset)
      - src_line_count (desde tmp_pol)
      - dst_line_count (presentes en pas_purchase_order_line para ese subset)
    """
    with pg_conn.cursor() as cur:
        # Temp tables sobreviven dentro de la transacción
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

        # COPY tmp_po
        copy_po_sql = r"""
    COPY tmp_po (
        id, "timestamp", purchase_number, destination_site_id, supplier_id, ext_code,
        creation_date, purchase_order_date, expected_receipt_date,
        total_amount_whith_tax_excluded, total_amount_whith_tax_included, total_tax_amount
    )
    FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N');
"""
        copy_file_to_table(cur, copy_po_sql, TMP_PO_TSV)

        # COPY tmp_pol
        copy_pol_sql = r"""
    COPY tmp_pol (
        id, "timestamp", purchase_order_id, sku, quantity, unit_price, total_price, line_type
    )
    FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N');
"""
        copy_file_to_table(cur, copy_pol_sql, TMP_LINE_TSV)

        # Conteos fuente (lo que estamos procesando)
        cur.execute("SELECT COUNT(*) FROM tmp_po;")
        src_po_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM tmp_pol;")
        src_line_count = cur.fetchone()[0]

        # UPSERT purchase orders por purchase_number (UNIQUE)
        cur.execute(f"""
            INSERT INTO {SCHEMA}.pas_purchase_order (
                id, "timestamp", purchase_number, destination_site_id, supplier_id, ext_code,
                creation_date, purchase_order_date, expected_receipt_date,
                total_amount_whith_tax_excluded, total_amount_whith_tax_included, total_tax_amount
            )
            SELECT
                id, "timestamp", purchase_number, destination_site_id, supplier_id, ext_code,
                creation_date, purchase_order_date, expected_receipt_date,
                total_amount_whith_tax_excluded, total_amount_whith_tax_included, total_tax_amount
            FROM tmp_po
            ON CONFLICT (purchase_number)
            DO UPDATE SET
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

        # UPSERT lines por PK id
        cur.execute(f"""
            INSERT INTO {SCHEMA}.pas_purchase_order_line (
                id, "timestamp", purchase_order_id, sku, quantity, unit_price, total_price, line_type
            )
            SELECT
                id, "timestamp", purchase_order_id, sku, quantity, unit_price, total_price, line_type
            FROM tmp_pol
            ON CONFLICT (id)
            DO UPDATE SET
                "timestamp" = EXCLUDED."timestamp",
                purchase_order_id = EXCLUDED.purchase_order_id,
                sku = EXCLUDED.sku,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_price = EXCLUDED.total_price,
                line_type = EXCLUDED.line_type;
        """)

        # Conteos destino para el mismo subset
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {SCHEMA}.pas_purchase_order p
            JOIN tmp_po t ON t.purchase_number = p.purchase_number;
        """)
        dst_po_count = cur.fetchone()[0]

        cur.execute(f"""
            SELECT COUNT(*)
            FROM {SCHEMA}.pas_purchase_order_line l
            JOIN tmp_pol t ON t.id = l.id;
        """)
        dst_line_count = cur.fetchone()[0]

        return src_po_count, dst_po_count, src_line_count, dst_line_count

# =========================
# MAIN
# =========================

def main():
    # Conectar SQL Server (solo lectura)
    log.info("Conectando a SQL Server (origen)...")
    print("SQLSERVER_CONN_STR =", SQLSERVER_CONN_STR)

    sql_conn = pyodbc.connect(SQLSERVER_CONN_STR, timeout=15)
    sql_conn.autocommit = False

    # Conectar PostgreSQL (destino)
    log.info("Conectando a PostgreSQL (destino)...")
    pg_conn = psycopg.connect(POSTGRES_DSN, connect_timeout=15)
    pg_conn.autocommit = False

    try:
        # Lookups
        log.info("Cargando mapas de supplier y site desde PostgreSQL...")
        supplier_map = load_supplier_map(pg_conn)
        site_map = load_site_map(pg_conn)
        log.info("Suppliers en mapa: %s | Sites en mapa: %s", len(supplier_map), len(site_map))

        # Export TSV
        log.info("Extrayendo purchase orders desde repl.T080_OC_CABE...")
        po_count, miss_sup, miss_site = export_purchase_orders_to_tsv(sql_conn, supplier_map, site_map)
        log.info("PO exportadas a TSV: %s", po_count)

        if miss_sup:
            log.warning("Proveedores faltantes (ext_code) - muestra: %s", sorted(list(miss_sup))[:30])
        if miss_site:
            log.warning("Sites faltantes (code) - muestra: %s", sorted(list(miss_site))[:30])

        log.info("Extrayendo purchase order lines desde repl.T081_OC_DETA...")
        pol_count = export_purchase_order_lines_to_tsv(sql_conn)
        log.info("Líneas exportadas a TSV: %s", pol_count)

        # Carga + upsert + comparativas (TODO dentro de la misma transacción PG)
        log.info("Cargando a PostgreSQL (COPY + UPSERT) dentro de una transacción...")
        src_po, dst_po, src_ln, dst_ln = load_and_upsert_with_counts(pg_conn)

        log.info("Comparativa PO | origen(tmp_po)=%s vs destino(pas_purchase_order subset)=%s", src_po, dst_po)
        log.info("Comparativa LN | origen(tmp_pol)=%s vs destino(pas_purchase_order_line subset)=%s", src_ln, dst_ln)

        # Validación fuerte
        if STRICT_COMPARE:
            if src_po != dst_po:
                raise RuntimeError(f"Mismatch PO: origen={src_po} destino={dst_po}. Rollback.")
            if src_ln != dst_ln:
                raise RuntimeError(f"Mismatch LINES: origen={src_ln} destino={dst_ln}. Rollback.")

        # Commit
        pg_conn.commit()
        log.info("Carga finalizada OK (COMMIT).")

    except Exception as e:
        pg_conn.rollback()
        log.error("ERROR: %s", e, exc_info=True)
        raise
    finally:
        try:
            sql_conn.close()
        except Exception:
            pass
        try:
            pg_conn.close()
        except Exception:
            pass

        # Limpieza temporales
        for p in (TMP_PO_TSV, TMP_LINE_TSV):
            if os.path.exists(p):
                os.remove(p)

if __name__ == "__main__":
    main()
