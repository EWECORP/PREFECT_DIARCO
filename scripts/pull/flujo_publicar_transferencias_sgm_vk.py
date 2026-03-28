# scripts/pull/publicar_transferencias_sgm.py
#
# Publica transferencias desde CONNEXA (PostgreSQL Microservicios) hacia staging en SQL Server DMZ:
#   [data-sync].[repl].[TRANSF_CONNEXA_IN]
#
# Regla implementada:
# - Solo se publican detalles con stock neto disponible suficiente, bajo lógica
#   "todo o nada", descontando saldo en memoria por (sucursal origen, artículo).
# - Si no alcanza el saldo completo para una línea, esa línea NO se publica.
# - Las líneas no publicadas permanecen en Connexa bajo cabecera PRECARGA_CONNEXA.
# - La cabecera se actualiza a 80 (SINCRONIZANDO) solo cuando TODOS sus detalles
#   ya fueron publicados (previamente o en esta corrida).
#
# Fuentes / destinos:
# - Connexa MS (PGP_*): lectura de transferencias + update de estado cabeceras
# - diarco_data (PG_*): lectura de src.base_stock_sucursal + auditoría en audit.*
# - SQL Server DMZ: inserción en repl.TRANSF_CONNEXA_IN
# - Valkimia vía linked server SQL Server: lectura de necesidades ACO para calcular SND
#
# Stock Neto Disponible (SND):
#   SND = q_bultos_disponible_base - bultos_aco_valkimia
#
# donde:
# - q_bultos_disponible_base sale de src.base_stock_sucursal
# - bultos_aco_valkimia sale de [DIARCO-VKMSQL\SQL2008R2].[VALKIMIA].[dbo].[IntNecIN]
#
# Supuesto:
# - El query de Valkimia no discrimina origen, por lo que se imputa la reserva ACO
#   únicamente al CD 41 (origin_cd_num = 41).
#
# Capacidades operativas:
# - Logging estructurado a consola + archivo
# - Export de rechazadas a CSV
# - Auditoría persistente en PostgreSQL diarco_data / schema audit
#
# Notas:
# - Este script NO ejecuta el SP publicador SGM; solo carga staging + marca cabeceras en 80.
# - Para evitar duplicados por reintento, se filtran connexa_detail_uuid ya presentes en staging.
# - Las búsquedas masivas en SQL Server se resuelven por bloques para evitar errores pyodbc/ODBC.

import os
import sys
import uuid
import logging
import traceback
import urllib.parse
from pathlib import Path
from datetime import datetime
from typing import List, Set

import pandas as pd
from dotenv import dotenv_values, load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# =========================
# 1) CONFIGURACIÓN GENERAL
# =========================
PROCESS_NAME = "publicar_transferencias_sgm"

ENV_PATH = os.environ.get("ETL_ENV_PATH", r"E:\ETL\ETL_DIARCO\.env")
DEFAULT_LOG_DIR = os.environ.get("TRANSFER_LOG_DIR", r"E:\ETL\ETL_DIARCO\logs")
DEFAULT_REJECT_DIR = os.environ.get("TRANSFER_REJECT_DIR", r"E:\ETL\ETL_DIARCO\salidas\rechazadas")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

LOG_DIR = Path(DEFAULT_LOG_DIR)
REJECT_DIR = Path(DEFAULT_REJECT_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)
REJECT_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / f"{PROCESS_NAME}_{RUN_ID}.log"
REJECT_FILE = REJECT_DIR / f"transferencias_rechazadas_{RUN_ID}.csv"


# =========================
# 2) LOGGING
# =========================
def setup_logger() -> logging.Logger:
    logger = logging.getLogger(PROCESS_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


logger = setup_logger()


def log_kv(evento: str, **kwargs) -> None:
    partes = [f"evento={evento}"]
    for k, v in kwargs.items():
        partes.append(f"{k}={repr(v)}")
    logger.info(" | ".join(partes))


# =========================
# 3) CARGA DE ENTORNO
# =========================
if not os.path.exists(ENV_PATH):
    logger.error(f"No existe el archivo .env en: {ENV_PATH}")
    logger.error(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)

secrets = dotenv_values(ENV_PATH)
load_dotenv(ENV_PATH)

log_kv(
    "inicio_configuracion",
    env_path=ENV_PATH,
    pgp_db=secrets.get("PGP_DB"),
    pgp_host=secrets.get("PGP_HOST"),
    pgp_user=secrets.get("PGP_USER"),
    pg_db=secrets.get("PG_DB"),
    pg_host=secrets.get("PG_HOST"),
    pg_user=secrets.get("PG_USER"),
    sql_server=secrets.get("SQL_SERVER"),
    sql_database=secrets.get("SQL_DATABASE"),
    log_file=str(LOG_FILE),
    reject_file=str(REJECT_FILE),
)


# =========================
# 4) CONEXIONES A BASES
# =========================
def get_pg_connexa_engine() -> Engine:
    """
    PostgreSQL Connexa Microservicios.
    Usa variables PGP_*.
    """
    host = os.getenv("PGP_HOST")
    port = os.getenv("PGP_PORT", "5432")
    db = os.getenv("PGP_DB")
    user = os.getenv("PGP_USER")
    pwd = os.getenv("PGP_PASSWORD")

    log_kv("conexion_pg_connexa_intento", host=host, port=port, db=db, user=user)

    if not all([host, db, user, pwd, port]):
        raise RuntimeError("Faltan variables de entorno PGP_* para conectarse a PostgreSQL Connexa")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def get_pg_diarco_data_engine() -> Engine:
    """
    PostgreSQL diarco_data.
    Usa variables PG_*.
    """
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    pwd = os.getenv("PG_PASSWORD")

    log_kv("conexion_pg_diarco_data_intento", host=host, port=port, db=db, user=user)

    if not all([host, port, db, user, pwd]):
        raise RuntimeError("Faltan variables de entorno PG_* para conectarse a PostgreSQL diarco_data")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def get_sqlserver_engine() -> Engine:
    """
    SQL Server DMZ / data-sync.
    """
    host = os.getenv("SQL_SERVER")
    port = os.getenv("SQL_PORT", "1433")
    db = os.getenv("SQL_DATABASE", "data-sync")
    user = os.getenv("SQL_USER")
    pwd = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    if not all([host, db, user, pwd]):
        raise RuntimeError("Faltan variables de entorno SQL_SERVER, SQL_DATABASE, SQL_USER o SQL_PASSWORD")

    timeout_seconds = 30

    log_kv("conexion_sqlserver_intento", host=host, port=port, db=db, user=user, driver=driver)

    params = urllib.parse.quote_plus(
        f"DRIVER={driver};"
        f"SERVER={host},{port};"
        f"DATABASE={db};"
        f"UID={user};PWD={pwd};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        f"Connect Timeout={timeout_seconds};"
    )
    url = f"mssql+pyodbc:///?odbc_connect={params}"
    return create_engine(url, pool_pre_ping=True, fast_executemany=True)


# =========================
# 5) AUDITORÍA EN DIARCO_DATA
# =========================
def audit_insert_run_start(pg_diarco_data_engine: Engine, run_id: str, started_at: datetime) -> None:
    sql = """
    INSERT INTO audit.transfer_publication_run (
        run_id,
        process_name,
        started_at,
        status,
        log_file,
        reject_file
    )
    VALUES (
        :run_id,
        :process_name,
        :started_at,
        'RUNNING',
        :log_file,
        :reject_file
    )
    ON CONFLICT (run_id) DO UPDATE
       SET process_name = EXCLUDED.process_name,
           started_at   = EXCLUDED.started_at,
           status       = EXCLUDED.status,
           log_file     = EXCLUDED.log_file,
           reject_file  = EXCLUDED.reject_file
    """

    with pg_diarco_data_engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "run_id": run_id,
                "process_name": PROCESS_NAME,
                "started_at": started_at,
                "log_file": str(LOG_FILE),
                "reject_file": str(REJECT_FILE),
            },
        )


def audit_update_run_end(
    pg_diarco_data_engine: Engine,
    run_id: str,
    finished_at: datetime,
    status: str,
    total_source_rows: int = 0,
    total_already_published: int = 0,
    total_publishable_now: int = 0,
    total_not_publishable: int = 0,
    total_inserted: int = 0,
    total_headers_updated: int = 0,
    total_rejected_exported: int = 0,
    error_message: str = "",
) -> None:
    sql = """
    UPDATE audit.transfer_publication_run
       SET finished_at = :finished_at,
           status = :status,
           total_source_rows = :total_source_rows,
           total_already_published = :total_already_published,
           total_publishable_now = :total_publishable_now,
           total_not_publishable = :total_not_publishable,
           total_inserted = :total_inserted,
           total_headers_updated = :total_headers_updated,
           total_rejected_exported = :total_rejected_exported,
           error_message = :error_message
     WHERE run_id = :run_id
    """

    with pg_diarco_data_engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "run_id": run_id,
                "finished_at": finished_at,
                "status": status,
                "total_source_rows": total_source_rows,
                "total_already_published": total_already_published,
                "total_publishable_now": total_publishable_now,
                "total_not_publishable": total_not_publishable,
                "total_inserted": total_inserted,
                "total_headers_updated": total_headers_updated,
                "total_rejected_exported": total_rejected_exported,
                "error_message": error_message[:4000] if error_message else "",
            },
        )


def audit_insert_detail_rows(pg_diarco_data_engine: Engine, df_asignado: pd.DataFrame, run_id: str) -> int:
    if df_asignado.empty:
        return 0

    df = df_asignado.copy()

    def to_uuid_or_none(v):
        try:
            return str(uuid.UUID(str(v))) if pd.notna(v) and str(v).strip() else None
        except Exception:
            return None

    rows = []
    for _, r in df.iterrows():
        rows.append(
            {
                "run_id": run_id,
                "process_name": PROCESS_NAME,
                "connexa_header_uuid": to_uuid_or_none(r.get("connexa_header_uuid")),
                "connexa_detail_uuid": to_uuid_or_none(r.get("connexa_detail_uuid")),
                "connexa_purchase_code": r.get("connexa_purchase_code"),
                "created_by": r.get("created_by"),
                "origin_cd_raw": str(r.get("origin_cd")) if pd.notna(r.get("origin_cd")) else None,
                "origin_cd_num": int(r["origin_cd_num"]) if pd.notna(r.get("origin_cd_num")) else None,
                "destination_store_code_raw": str(r.get("destination_store_code")) if pd.notna(r.get("destination_store_code")) else None,
                "dest_store_num": int(r["dest_store_num"]) if pd.notna(r.get("dest_store_num")) else None,
                "item_code_raw": str(r.get("item_code")) if pd.notna(r.get("item_code")) else None,
                "item_code_num": int(r["item_code_num"]) if pd.notna(r.get("item_code_num")) else None,
                "item_description": r.get("item_description"),
                "qty_requested_raw": float(r["qty_requested"]) if pd.notna(r.get("qty_requested")) else None,
                "qty_requested_num": float(r["qty_requested_num"]) if pd.notna(r.get("qty_requested_num")) else None,
                "units_per_package": float(r["units_per_package"]) if pd.notna(r.get("units_per_package")) else None,
                "q_bultos_disponible_base": float(r["q_bultos_disponible_base"]) if pd.notna(r.get("q_bultos_disponible_base")) else None,
                "bultos_aco_valkimia": float(r["bultos_aco_valkimia"]) if pd.notna(r.get("bultos_aco_valkimia")) else None,
                "q_bultos_disponible": float(r["q_bultos_disponible"]) if pd.notna(r.get("q_bultos_disponible")) else None,
                "saldo_inicial_grupo": float(r["saldo_inicial_grupo"]) if pd.notna(r.get("saldo_inicial_grupo")) else None,
                "saldo_antes": float(r["saldo_antes"]) if pd.notna(r.get("saldo_antes")) else None,
                "saldo_despues": float(r["saldo_despues"]) if pd.notna(r.get("saldo_despues")) else None,
                "q_bultos_asignado": float(r["q_bultos_asignado"]) if pd.notna(r.get("q_bultos_asignado")) else None,
                "ya_publicado": bool(r.get("ya_publicado", False)),
                "publicable": bool(r.get("publicable", False)),
                "publicable_ahora": bool(r.get("publicable_ahora", False)),
                "motivo_no_publicado": r.get("motivo_no_publicado"),
                "requested_at": r.get("requested_at").to_pydatetime() if pd.notna(r.get("requested_at")) else None, # type: ignore
                "created_at": r.get("created_at").to_pydatetime() if pd.notna(r.get("created_at")) else None, # type: ignore
            }
        )

    sql = text("""
    INSERT INTO audit.transfer_publication_detail (
        run_id,
        process_name,
        connexa_header_uuid,
        connexa_detail_uuid,
        connexa_purchase_code,
        created_by,
        origin_cd_raw,
        origin_cd_num,
        destination_store_code_raw,
        dest_store_num,
        item_code_raw,
        item_code_num,
        item_description,
        qty_requested_raw,
        qty_requested_num,
        units_per_package,
        q_bultos_disponible_base,
        bultos_aco_valkimia,
        q_bultos_disponible,
        saldo_inicial_grupo,
        saldo_antes,
        saldo_despues,
        q_bultos_asignado,
        ya_publicado,
        publicable,
        publicable_ahora,
        motivo_no_publicado,
        requested_at,
        created_at
    )
    VALUES (
        :run_id,
        :process_name,
        CAST(:connexa_header_uuid AS uuid),
        CAST(:connexa_detail_uuid AS uuid),
        :connexa_purchase_code,
        :created_by,
        :origin_cd_raw,
        :origin_cd_num,
        :destination_store_code_raw,
        :dest_store_num,
        :item_code_raw,
        :item_code_num,
        :item_description,
        :qty_requested_raw,
        :qty_requested_num,
        :units_per_package,
        :q_bultos_disponible_base,
        :bultos_aco_valkimia,
        :q_bultos_disponible,
        :saldo_inicial_grupo,
        :saldo_antes,
        :saldo_despues,
        :q_bultos_asignado,
        :ya_publicado,
        :publicable,
        :publicable_ahora,
        :motivo_no_publicado,
        :requested_at,
        :created_at
    )
    """)

    with pg_diarco_data_engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)


# =========================
# 6) LECTURA DESDE CONNEXA
# =========================
def obtener_transferencias_precarga(pg_connexa_engine: Engine) -> pd.DataFrame:
    sql = """
    SELECT
        d.id                          AS connexa_detail_uuid,
        h.id                          AS connexa_header_uuid,
        h.origin_cd,
        h.destination_store_code,
        h.connexa_purchase_code,
        h.requested_at,
        h.created_by,
        h.created_at,
        h.status_id,
        s.code                        AS status_code,
        d.item_code,
        d.item_description,
        d.qty_requested,
        d.qty_planned,
        d.qty_shipped,
        d.qty_received,
        d.uom_id,
        d.units_per_package,
        d.packages_per_layer,
        d.layers_per_pallet
    FROM supply_planning.spl_distribution_transfer_detail d
    JOIN supply_planning.spl_distribution_transfer h
      ON d.distribution_transfer_id = h.id
    JOIN supply_planning.spl_distribution_transfer_status s
      ON h.status_id = s.id
    WHERE s.code = 'PRECARGA_CONNEXA';
    """

    df = pd.read_sql(sql, pg_connexa_engine, parse_dates=["requested_at", "created_at"])
    log_kv("transferencias_precarga_leidas", cantidad=len(df))
    return df


# =========================
# 7) NORMALIZACIÓN
# =========================
def normalizar_transferencias(df_src: pd.DataFrame) -> pd.DataFrame:
    if df_src.empty:
        return df_src.copy()

    df = df_src.copy()

    df["item_code_num"] = pd.to_numeric(df["item_code"], errors="coerce").fillna(0).astype(int)
    df["dest_store_num"] = pd.to_numeric(df["destination_store_code"], errors="coerce").fillna(0).astype(int)
    df["qty_requested_num"] = pd.to_numeric(df["qty_requested"], errors="coerce").fillna(0.0).round(3)

    df["connexa_header_uuid"] = df["connexa_header_uuid"].astype(str).str.strip().str.lower()
    df["connexa_detail_uuid"] = df["connexa_detail_uuid"].astype(str).str.strip().str.lower()

    origin_str = df["origin_cd"].astype(str)
    df["origin_cd_num"] = (
        origin_str.str.extract(r"^(\d+)", expand=False)
        .fillna("0")
        .astype(int)
    )

    log_kv("transferencias_normalizadas", cantidad=len(df))
    return df


# =========================
# 8) STOCK BASE DESDE DIARCO_DATA
# =========================
def obtener_stock_disponible(pg_diarco_data_engine: Engine, df_norm: pd.DataFrame) -> pd.DataFrame:
    """
    Obtiene q_bultos_disponible_base por (codigo_articulo, codigo_sucursal origen)
    desde src.base_stock_sucursal.
    """
    cols = ["item_code_num", "origin_cd_num"]
    if df_norm.empty or not set(cols).issubset(df_norm.columns):
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible_base"])

    articulos = sorted([int(x) for x in df_norm["item_code_num"].dropna().unique().tolist() if int(x) > 0])
    sucursales = sorted([int(x) for x in df_norm["origin_cd_num"].dropna().unique().tolist() if int(x) > 0])

    if not articulos or not sucursales:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible_base"])

    sql = """
        SELECT
            codigo_articulo::bigint AS item_code_num,
            codigo_sucursal::bigint AS origin_cd_num,
            MAX(
                FLOOR(
                    (COALESCE(stock, 0) + COALESCE(transfer_pendiente, 0))
                    / NULLIF(COALESCE(factor_venta, 0), 0)
                )
            )::bigint AS q_bultos_disponible_base
        FROM src.base_stock_sucursal
        WHERE codigo_sucursal = ANY(CAST(:lista_sucursales AS bigint[]))
          AND codigo_articulo = ANY(CAST(:lista_articulos AS bigint[]))
          AND COALESCE(factor_venta, 0) > 0
        GROUP BY codigo_articulo, codigo_sucursal
        HAVING MAX(
            FLOOR(
                (COALESCE(stock, 0) + COALESCE(transfer_pendiente, 0))
                / NULLIF(COALESCE(factor_venta, 0), 0)
            )
        ) > 0
    """

    with pg_diarco_data_engine.connect() as conn:
        df_stock = pd.read_sql(
            text(sql),
            conn,
            params={
                "lista_sucursales": sucursales,
                "lista_articulos": articulos,
            }, # type: ignore
        )

    if df_stock.empty:
        log_kv("stock_base_cd_leido", cantidad=0)
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible_base"])

    df_stock["item_code_num"] = pd.to_numeric(df_stock["item_code_num"], errors="coerce").fillna(0).astype(int)
    df_stock["origin_cd_num"] = pd.to_numeric(df_stock["origin_cd_num"], errors="coerce").fillna(0).astype(int)
    df_stock["q_bultos_disponible_base"] = pd.to_numeric(
        df_stock["q_bultos_disponible_base"], errors="coerce"
    ).fillna(0.0)

    log_kv("stock_base_cd_leido", cantidad=len(df_stock))
    return df_stock


# =========================
# 9) RESERVAS ACO VALKIMIA
# =========================
def obtener_bultos_aco_valkimia(sql_engine: Engine, df_norm: pd.DataFrame, chunk_size: int = 300) -> pd.DataFrame:
    """
    Obtiene bultos ya comprometidos en Valkimia (estado ACO) por SKU.

    Se interpreta como reserva del CD 41, por lo que luego se imputa
    a origin_cd_num = 41.
    """
    if df_norm.empty or "item_code_num" not in df_norm.columns:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "bultos_aco_valkimia"])

    skus = sorted([int(x) for x in df_norm["item_code_num"].dropna().unique().tolist() if int(x) > 0])
    if not skus:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "bultos_aco_valkimia"])

    encontrados = []

    log_kv(
        "consulta_bultos_aco_valkimia_inicio",
        total_skus=len(skus),
        chunk_size=chunk_size,
    )

    for i in range(0, len(skus), chunk_size):
        bloque = skus[i:i + chunk_size]

        placeholders = ", ".join([f":p{j}" for j in range(len(bloque))])
        sql = text(f"""
            SELECT
                CAST([INIArtId] AS bigint) AS item_code_num,
                SUM(CAST([INICnt1] AS decimal(18,3))) AS bultos_aco_valkimia
            FROM [DIARCO-VKMSQL\\SQL2008R2].[VALKIMIA].[dbo].[IntNecIN]
            WHERE INIEst = 'ACO'
              AND [INIEntId] < 300
              AND [INICnt1] > 0
              AND [INIArtId] IN ({placeholders})
            GROUP BY [INIArtId]
        """)

        params = {f"p{j}": bloque[j] for j in range(len(bloque))}

        with sql_engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)

        if not df.empty:
            encontrados.append(df)

    if not encontrados:
        log_kv("bultos_aco_valkimia_leidos", cantidad=0, total_bultos_aco=0)
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "bultos_aco_valkimia"])

    df_aco = pd.concat(encontrados, ignore_index=True)
    df_aco["item_code_num"] = pd.to_numeric(df_aco["item_code_num"], errors="coerce").fillna(0).astype(int)
    df_aco["bultos_aco_valkimia"] = pd.to_numeric(df_aco["bultos_aco_valkimia"], errors="coerce").fillna(0.0)

    # Se imputa solamente al CD 41
    df_aco["origin_cd_num"] = 41

    df_aco = (
        df_aco.groupby(["item_code_num", "origin_cd_num"], as_index=False)["bultos_aco_valkimia"]
        .sum()
    )

    log_kv(
        "bultos_aco_valkimia_leidos",
        cantidad=len(df_aco),
        total_bultos_aco=float(df_aco["bultos_aco_valkimia"].sum()) if not df_aco.empty else 0.0,
    )

    return df_aco


# =========================
# 10) DETALLES YA PUBLICADOS EN DMZ
# =========================
def obtener_detalles_ya_publicados(sql_engine: Engine, detail_uuids: List[str], chunk_size: int = 300) -> Set[str]:
    """
    Devuelve el conjunto de connexa_detail_uuid ya existentes en repl.TRANSF_CONNEXA_IN.

    Se consulta por bloques para evitar errores de SQL Server / pyodbc con listas IN muy grandes.
    """
    ids = sorted({str(x).strip().lower() for x in detail_uuids if str(x).strip()})
    if not ids:
        log_kv("detalles_ya_publicados_consultados", cantidad=0, chunks=0)
        return set()

    encontrados: Set[str] = set()

    log_kv(
        "consulta_detalles_ya_publicados_inicio",
        total_detail_uuids=len(ids),
        chunk_size=chunk_size,
    )

    for i in range(0, len(ids), chunk_size):
        bloque = ids[i:i + chunk_size]

        placeholders = ", ".join([f":p{j}" for j in range(len(bloque))])
        sql = text(f"""
            SELECT LOWER(LTRIM(RTRIM(connexa_detail_uuid))) AS connexa_detail_uuid
            FROM repl.TRANSF_CONNEXA_IN
            WHERE connexa_detail_uuid IN ({placeholders})
        """)

        params = {f"p{j}": bloque[j] for j in range(len(bloque))}

        with sql_engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)

        if not df.empty:
            encontrados.update(
                df["connexa_detail_uuid"].astype(str).str.strip().str.lower().tolist()
            )

    log_kv(
        "detalles_ya_publicados_consultados",
        cantidad=len(encontrados),
        total_ids=len(ids),
        chunks=((len(ids) - 1) // chunk_size) + 1,
        chunk_size=chunk_size,
    )
    return encontrados


def marcar_detalles_ya_publicados(df: pd.DataFrame, ya_publicados: Set[str]) -> pd.DataFrame:
    out = df.copy()
    out["ya_publicado"] = out["connexa_detail_uuid"].isin(ya_publicados)
    return out


# =========================
# 11) STOCK NETO DISPONIBLE (SND)
# =========================
def enriquecer_con_stock_y_snd(
    df_norm: pd.DataFrame,
    df_stock_base: pd.DataFrame,
    df_aco_valkimia: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enriquece detalles con:
      - q_bultos_disponible_base
      - bultos_aco_valkimia
      - q_bultos_disponible (SND final)

    Fórmula:
      SND = max(0, q_bultos_disponible_base - bultos_aco_valkimia)
    """
    if df_norm.empty:
        df = df_norm.copy()
        df["q_bultos_disponible_base"] = pd.Series(dtype="float")
        df["bultos_aco_valkimia"] = pd.Series(dtype="float")
        df["q_bultos_disponible"] = pd.Series(dtype="float")
        return df

    df = df_norm.merge(
        df_stock_base,
        how="left",
        on=["item_code_num", "origin_cd_num"],
    )

    df = df.merge(
        df_aco_valkimia,
        how="left",
        on=["item_code_num", "origin_cd_num"],
    )

    df["q_bultos_disponible_base"] = pd.to_numeric(
        df.get("q_bultos_disponible_base"), errors="coerce" # type: ignore
    ).fillna(0.0) # type: ignore

    df["bultos_aco_valkimia"] = pd.to_numeric(
        df.get("bultos_aco_valkimia"), errors="coerce" # type: ignore
    ).fillna(0.0) # type: ignore

    df["q_bultos_disponible"] = (
        df["q_bultos_disponible_base"] - df["bultos_aco_valkimia"]
    ).clip(lower=0.0)

    log_kv(
        "stock_neto_disponible_calculado",
        cantidad=len(df),
        stock_base_total=float(df["q_bultos_disponible_base"].sum()) if not df.empty else 0.0,
        aco_total=float(df["bultos_aco_valkimia"].sum()) if not df.empty else 0.0,
        snd_total=float(df["q_bultos_disponible"].sum()) if not df.empty else 0.0,
    )

    return df


# =========================
# 12) ASIGNACIÓN EN MEMORIA
# =========================
def asignar_stock_en_memoria_todo_o_nada(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asigna stock neto disponible en memoria por (origin_cd_num, item_code_num), bajo criterio FIFO y todo o nada.
    """
    if df.empty:
        out = df.copy()
        out["saldo_inicial_grupo"] = pd.Series(dtype="float")
        out["saldo_antes"] = pd.Series(dtype="float")
        out["saldo_despues"] = pd.Series(dtype="float")
        out["q_bultos_asignado"] = pd.Series(dtype="float")
        out["publicable"] = pd.Series(dtype="bool")
        out["motivo_no_publicado"] = pd.Series(dtype="object")
        out["publicable_ahora"] = pd.Series(dtype="bool")
        return out

    work = df.copy()

    work["requested_at_ord"] = pd.to_datetime(work["requested_at"], errors="coerce")
    work["created_at_ord"] = pd.to_datetime(work["created_at"], errors="coerce")
    work["qty_requested_num"] = pd.to_numeric(work["qty_requested_num"], errors="coerce").fillna(0.0).round(3)
    work["q_bultos_disponible"] = pd.to_numeric(work["q_bultos_disponible"], errors="coerce").fillna(0.0)

    work = work.sort_values(
        by=["origin_cd_num", "item_code_num", "requested_at_ord", "created_at_ord", "connexa_detail_uuid"],
        ascending=[True, True, True, True, True],
        kind="mergesort",
    ).copy()

    resultados = []

    for (origin_cd_num, item_code_num), grp in work.groupby(["origin_cd_num", "item_code_num"], sort=False):
        grp = grp.copy()
        saldo = float(grp["q_bultos_disponible"].iloc[0]) if len(grp) else 0.0
        saldo_inicial = saldo

        log_kv(
            "inicio_grupo_asignacion",
            origin_cd_num=origin_cd_num,
            item_code_num=item_code_num,
            saldo_inicial=saldo_inicial,
            cantidad_detalles=len(grp),
        )

        for _, row in grp.iterrows():
            row = row.copy()
            qty = float(row["qty_requested_num"]) if pd.notna(row["qty_requested_num"]) else 0.0

            row["saldo_inicial_grupo"] = round(saldo_inicial, 3)
            row["saldo_antes"] = round(saldo, 3)

            if bool(row.get("ya_publicado", False)):
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = True
                row["motivo_no_publicado"] = ""
                row["saldo_despues"] = round(saldo, 3)
                resultados.append(row)
                continue

            if qty <= 0:
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = False
                row["motivo_no_publicado"] = "QTY_REQUESTED_INVALIDA"
                row["saldo_despues"] = round(saldo, 3)
                resultados.append(row)
                continue

            if saldo >= qty:
                saldo -= qty
                row["q_bultos_asignado"] = round(qty, 3)
                row["publicable"] = True
                row["motivo_no_publicado"] = ""
                row["saldo_despues"] = round(saldo, 3)
            else:
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = False
                row["motivo_no_publicado"] = "SIN_STOCK_SUFICIENTE"
                row["saldo_despues"] = round(saldo, 3)

            resultados.append(row)

        log_kv(
            "fin_grupo_asignacion",
            origin_cd_num=origin_cd_num,
            item_code_num=item_code_num,
            saldo_final=round(saldo, 3),
        )

    df_res = pd.DataFrame(resultados)
    df_res["publicable_ahora"] = df_res["publicable"] & (~df_res["ya_publicado"])

    log_kv(
        "fin_asignacion_memoria",
        total=len(df_res),
        publicables_ahora=int(df_res["publicable_ahora"].sum()) if not df_res.empty else 0,
        ya_publicados=int(df_res["ya_publicado"].sum()) if not df_res.empty else 0,
        no_publicables=int((~df_res["publicable"] & ~df_res["ya_publicado"]).sum()) if not df_res.empty else 0,
    )
    return df_res


# =========================
# 13) TRANSFORMACIÓN A STAGING
# =========================
def transformar_a_staging(df_src: pd.DataFrame) -> pd.DataFrame:
    base_cols = [
        "c_articulo",
        "c_sucu_dest",
        "c_sucu_orig",
        "q_requerida",
        "q_bultos",
        "q_factor",
        "f_alta",
        "m_alta_prioridad",
        "vchUsuario",
        "vchTerminal",
        "forzarTransf",
        "estado",
        "mensaje_error",
        "connexa_header_uuid",
        "connexa_detail_uuid",
    ]

    if df_src.empty:
        return pd.DataFrame(columns=base_cols)

    df = df_src.copy()

    df["units_per_package"] = pd.to_numeric(df["units_per_package"], errors="coerce").fillna(1.0)
    df.loc[df["units_per_package"] <= 0, "units_per_package"] = 1.0

    df["q_factor"] = df["units_per_package"].round(0).astype(int)
    df["q_bultos"] = pd.to_numeric(df["q_bultos_asignado"], errors="coerce").fillna(0.0).round(3)
    df["q_requerida"] = (df["q_bultos"] * df["q_factor"]).round(3)

    now = datetime.now()
    df["f_alta"] = df["requested_at"].fillna(df["created_at"]).fillna(now)

    df["m_alta_prioridad"] = "N"
    df["vchUsuario"] = "CONNEXA"
    df["vchTerminal"] = "API"
    df["forzarTransf"] = "N"
    df["estado"] = "PENDIENTE"
    df["mensaje_error"] = ""

    df_stg = pd.DataFrame(
        {
            "c_articulo": df["item_code_num"],
            "c_sucu_dest": df["dest_store_num"],
            "c_sucu_orig": df["origin_cd_num"],
            "q_requerida": df["q_requerida"],
            "q_bultos": df["q_bultos"],
            "q_factor": df["q_factor"],
            "f_alta": pd.to_datetime(df["f_alta"]),
            "m_alta_prioridad": df["m_alta_prioridad"],
            "vchUsuario": df["vchUsuario"],
            "vchTerminal": df["vchTerminal"],
            "forzarTransf": df["forzarTransf"],
            "estado": df["estado"],
            "mensaje_error": df["mensaje_error"],
            "connexa_header_uuid": df["connexa_header_uuid"],
            "connexa_detail_uuid": df["connexa_detail_uuid"],
        }
    )

    df_stg = df_stg[
        (df_stg["c_articulo"] > 0)
        & (df_stg["c_sucu_dest"] > 0)
        & (df_stg["c_sucu_orig"] > 0)
        & (df_stg["q_bultos"] > 0)
        & (df_stg["q_factor"] > 0)
    ].copy()

    log_kv("transformacion_staging", cantidad=len(df_stg))
    return df_stg


# =========================
# 14) INSERCIÓN EN SQL SERVER
# =========================
def insertar_en_staging_sqlserver(df_stg: pd.DataFrame, sql_engine: Engine) -> int:
    if df_stg.empty:
        return 0

    try:
        df_stg.to_sql(
            name="TRANSF_CONNEXA_IN",
            con=sql_engine,
            schema="repl",
            if_exists="append",
            index=False,
        )
        log_kv("insercion_staging_ok", cantidad=len(df_stg))
        return len(df_stg)
    except Exception:
        logger.exception("Error al insertar en SQL Server staging")
        raise


# =========================
# 15) UPDATE DE CABECERAS EN CONNEXA
# =========================
def _to_uuid_list(values: List[str]) -> List[uuid.UUID]:
    uuids: List[uuid.UUID] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip().lower()
        if not s:
            continue
        try:
            uuids.append(uuid.UUID(s))
        except Exception:
            pass
    return uuids


def actualizar_estado_cabeceras(pg_connexa_engine: Engine, header_uuids: List[str], nuevo_estado_id: int = 80) -> int:
    if not header_uuids:
        return 0

    header_uuid_objs = _to_uuid_list(header_uuids)
    if not header_uuid_objs:
        logger.warning("No quedaron UUIDs válidos para actualizar cabeceras en Connexa.")
        return 0

    sql = """
        UPDATE supply_planning.spl_distribution_transfer
           SET status_id = :nuevo_estado,
               updated_at = NOW()
         WHERE id = ANY(CAST(:lista_ids AS uuid[]))
    """

    with pg_connexa_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {"nuevo_estado": nuevo_estado_id, "lista_ids": header_uuid_objs},
        )

    log_kv("actualizacion_cabeceras_ok", cantidad=result.rowcount, nuevo_estado_id=nuevo_estado_id)
    return result.rowcount


# =========================
# 16) CABECERAS COMPLETAMENTE PUBLICABLES
# =========================
def obtener_headers_completamente_publicables(df_all: pd.DataFrame) -> List[str]:
    if df_all.empty:
        return []

    df = df_all.copy()
    df["quedara_publicado"] = df["ya_publicado"] | df["publicable"]

    resumen = (
        df.groupby("connexa_header_uuid", as_index=False)
        .agg(
            total_detalles=("connexa_detail_uuid", "count"),
            total_publicables=("quedara_publicado", "sum"),
        )
    )

    headers = resumen.loc[
        resumen["total_detalles"] == resumen["total_publicables"],
        "connexa_header_uuid"
    ].astype(str).str.lower().tolist()

    headers = sorted(set(headers))
    log_kv("cabeceras_completamente_publicables", cantidad=len(headers))
    return headers


# =========================
# 17) EXPORT DE RECHAZADAS
# =========================
def exportar_rechazadas(df_asignado: pd.DataFrame, output_file: Path) -> int:
    if df_asignado.empty:
        return 0

    df_rech = df_asignado[
        (~df_asignado["ya_publicado"]) &
        (~df_asignado["publicable"])
    ].copy()

    if df_rech.empty:
        return 0

    columnas = [
        "connexa_header_uuid",
        "connexa_detail_uuid",
        "connexa_purchase_code",
        "origin_cd",
        "origin_cd_num",
        "destination_store_code",
        "dest_store_num",
        "item_code",
        "item_code_num",
        "item_description",
        "qty_requested",
        "qty_requested_num",
        "units_per_package",
        "q_bultos_disponible_base",
        "bultos_aco_valkimia",
        "q_bultos_disponible",
        "saldo_inicial_grupo",
        "saldo_antes",
        "saldo_despues",
        "motivo_no_publicado",
        "requested_at",
        "created_at",
        "created_by",
    ]

    columnas_existentes = [c for c in columnas if c in df_rech.columns]
    df_rech = df_rech[columnas_existentes].copy()

    df_rech.to_csv(output_file, sep=";", index=False, encoding="utf-8-sig")
    log_kv("rechazadas_exportadas", cantidad=len(df_rech), archivo=str(output_file))
    return len(df_rech)


def log_resumen_rechazadas(df_asignado: pd.DataFrame) -> None:
    if df_asignado.empty:
        return

    df_rech = df_asignado[
        (~df_asignado["ya_publicado"]) &
        (~df_asignado["publicable"])
    ].copy()

    if df_rech.empty:
        log_kv("resumen_rechazadas", cantidad=0)
        return

    resumen = (
        df_rech.groupby("motivo_no_publicado", dropna=False)
        .size()
        .reset_index(name="cantidad")
        .sort_values("cantidad", ascending=False)
    )

    for _, row in resumen.iterrows():
        log_kv(
            "rechazadas_por_motivo",
            motivo=row["motivo_no_publicado"],
            cantidad=int(row["cantidad"]),
        )


# =========================
# 18) MAIN
# =========================
def main() -> int:
    inicio = datetime.now()
    pg_connexa_engine = None
    pg_diarco_data_engine = None

    try:
        pg_connexa_engine = get_pg_connexa_engine()
        pg_diarco_data_engine = get_pg_diarco_data_engine()
        sql_engine = get_sqlserver_engine()

        audit_insert_run_start(pg_diarco_data_engine, RUN_ID, inicio)
        log_kv("inicio_ejecucion", run_id=RUN_ID)

        # 1. Leer transferencias desde Connexa
        df_src = obtener_transferencias_precarga(pg_connexa_engine)

        if df_src.empty:
            audit_update_run_end(
                pg_diarco_data_engine=pg_diarco_data_engine,
                run_id=RUN_ID,
                finished_at=datetime.now(),
                status="SUCCESS",
                total_source_rows=0,
                total_already_published=0,
                total_publishable_now=0,
                total_not_publishable=0,
                total_inserted=0,
                total_headers_updated=0,
                total_rejected_exported=0,
                error_message="",
            )
            log_kv("sin_datos_precarga", mensaje="No hay transferencias en PRECARGA_CONNEXA para publicar.")
            return 0

        # 2. Normalizar
        df_norm = normalizar_transferencias(df_src)

        # 3. Detectar detalles ya publicados en staging DMZ
        ya_publicados = obtener_detalles_ya_publicados(
            sql_engine,
            df_norm["connexa_detail_uuid"].astype(str).tolist(),
            chunk_size=300,
        )

        # 4. Leer stock base desde diarco_data
        df_stock_base = obtener_stock_disponible(pg_diarco_data_engine, df_norm)

        # 5. Leer reservas ACO Valkimia
        df_aco_valkimia = obtener_bultos_aco_valkimia(sql_engine, df_norm, chunk_size=300)

        # 6. Enriquecer con SND
        df_work = enriquecer_con_stock_y_snd(
            df_norm=df_norm,
            df_stock_base=df_stock_base,
            df_aco_valkimia=df_aco_valkimia,
        )

        # 7. Marcar publicados previos
        df_work = marcar_detalles_ya_publicados(df_work, ya_publicados)

        # 8. Asignar stock neto en memoria (todo o nada)
        df_asignado = asignar_stock_en_memoria_todo_o_nada(df_work)

        # 9. Filtrar líneas que se publican ahora
        df_a_insertar = df_asignado[df_asignado["publicable_ahora"]].copy()

        # 10. Transformar staging
        df_stg = transformar_a_staging(df_a_insertar)

        # 11. Insertar en SQL Server
        if not df_stg.empty:
            n_insertadas = insertar_en_staging_sqlserver(df_stg, sql_engine)
        else:
            n_insertadas = 0
            log_kv("sin_filas_para_insertar", mensaje="No hay filas nuevas para insertar en staging.")

        # 12. Determinar cabeceras completas y actualizarlas en Connexa
        header_uuids_actualizables = obtener_headers_completamente_publicables(df_asignado)
        if header_uuids_actualizables:
            n_headers_actualizadas = actualizar_estado_cabeceras(
                pg_connexa_engine,
                header_uuids_actualizables,
                nuevo_estado_id=80
            )
        else:
            n_headers_actualizadas = 0
            log_kv("sin_cabeceras_para_actualizar", mensaje="No hay cabeceras completas para actualizar a 80.")

        # 13. Exportar rechazadas
        n_rechazadas = exportar_rechazadas(df_asignado, REJECT_FILE)
        log_resumen_rechazadas(df_asignado)

        # 14. Persistir auditoría detalle en diarco_data
        n_audit_detail = audit_insert_detail_rows(pg_diarco_data_engine, df_asignado, RUN_ID)
        log_kv("auditoria_detalle_insertada", cantidad=n_audit_detail)

        # 15. Métricas finales
        total_origen = len(df_asignado)
        total_ya_publicado = int(df_asignado["ya_publicado"].sum()) if not df_asignado.empty else 0
        total_publicable_ahora = int(df_asignado["publicable_ahora"].sum()) if not df_asignado.empty else 0
        total_no_publicable = int((~df_asignado["publicable"] & ~df_asignado["ya_publicado"]).sum()) if not df_asignado.empty else 0

        fin = datetime.now()

        audit_update_run_end(
            pg_diarco_data_engine=pg_diarco_data_engine,
            run_id=RUN_ID,
            finished_at=fin,
            status="SUCCESS",
            total_source_rows=total_origen,
            total_already_published=total_ya_publicado,
            total_publishable_now=total_publicable_ahora,
            total_not_publishable=total_no_publicable,
            total_inserted=n_insertadas,
            total_headers_updated=n_headers_actualizadas,
            total_rejected_exported=n_rechazadas,
            error_message="",
        )

        log_kv(
            "fin_ejecucion",
            run_id=RUN_ID,
            total_origen=total_origen,
            total_ya_publicado=total_ya_publicado,
            total_publicable_ahora=total_publicable_ahora,
            total_no_publicable=total_no_publicable,
            total_insertadas=n_insertadas,
            total_headers_actualizadas=n_headers_actualizadas,
            total_rechazadas_exportadas=n_rechazadas,
            log_file=str(LOG_FILE),
            reject_file=str(REJECT_FILE) if n_rechazadas > 0 else "",
        )

        return 0

    except RuntimeError as e:
        err = str(e)
        logger.error(f"Error de configuración de entorno: {err}")
        logger.error(traceback.format_exc())

        if pg_diarco_data_engine is not None:
            try:
                audit_update_run_end(
                    pg_diarco_data_engine=pg_diarco_data_engine,
                    run_id=RUN_ID,
                    finished_at=datetime.now(),
                    status="ERROR",
                    error_message=err,
                )
            except Exception:
                logger.exception("No se pudo actualizar auditoría RUN en manejo de RuntimeError")

        return 1

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        logger.exception(f"Ocurrió un error inesperado: {err}")

        if pg_diarco_data_engine is not None:
            try:
                audit_update_run_end(
                    pg_diarco_data_engine=pg_diarco_data_engine,
                    run_id=RUN_ID,
                    finished_at=datetime.now(),
                    status="ERROR",
                    error_message=err,
                )
            except Exception:
                logger.exception("No se pudo actualizar auditoría RUN en manejo de Exception")

        return 2


if __name__ == "__main__":
    sys.exit(main())