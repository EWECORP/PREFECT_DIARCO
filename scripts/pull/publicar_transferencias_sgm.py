# scripts/pull/publicar_transferencias_sgm.py
#
# Publica transferencias desde CONNEXA (PostgreSQL) hacia staging en SQL Server DMZ:
#   [data-sync].[repl].[TRANSF_CONNEXA_IN]
#
# Regla implementada:
# - Solo se publican detalles con stock disponible suficiente, bajo lógica
#   "todo o nada", descontando saldo en memoria por (sucursal origen, artículo).
# - Si no alcanza el saldo completo para una línea, esa línea NO se publica.
# - Las líneas no publicadas permanecen en Connexa bajo cabecera PRECARGA_CONNEXA.
# - La cabecera se actualiza a 80 (SINCRONIZANDO) solo cuando TODOS sus detalles
#   ya fueron publicados (previamente o en esta corrida).
#
# Reglas confirmadas:
# - qty_requested en Connexa YA está en BULTOS.
# - units_per_package = unidades por bulto (q_factor).
# - q_requerida = q_bultos * q_factor.
# - origin_cd puede venir como "41CD" / "82CD" y debe grabarse como 41 / 82.
#
# Capacidades operativas agregadas:
# - Logging estructurado a consola + archivo
# - Export de rechazadas a CSV
# - Resumen de ejecución y métricas
#
# Notas:
# - Este script NO ejecuta el SP publicador SGM; solo carga staging + marca cabeceras en 80.
# - Para evitar duplicados por reintento, se filtran connexa_detail_uuid ya presentes en staging.

# scripts/pull/publicar_transferencias_sgm.py
#
# Publica transferencias desde CONNEXA (PostgreSQL) hacia staging en SQL Server DMZ:
#   [data-sync].[repl].[TRANSF_CONNEXA_IN]
#
# Regla implementada:
# - Solo se publican detalles con stock disponible suficiente, bajo lógica
#   "todo o nada", descontando saldo en memoria por (sucursal origen, artículo).
# - Si no alcanza el saldo completo para una línea, esa línea NO se publica.
# - Las líneas no publicadas permanecen en Connexa bajo cabecera PRECARGA_CONNEXA.
# - La cabecera se actualiza a 80 (SINCRONIZANDO) solo cuando TODOS sus detalles
#   ya fueron publicados (previamente o en esta corrida).
#
# Capacidades operativas agregadas:
# - Logging estructurado a consola + archivo
# - Export de rechazadas a CSV
# - Resumen de ejecución y métricas
#
# Notas:
# - Este script NO ejecuta el SP publicador SGM; solo carga staging + marca cabeceras en 80.
# - Para evitar duplicados por reintento, se filtran connexa_detail_uuid ya presentes en staging.

import os
import sys
import urllib.parse
from datetime import datetime
from typing import List
import uuid
import logging
from pathlib import Path
import traceback

import pandas as pd
from dotenv import dotenv_values, load_dotenv
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine


# =========================
# 1) CONFIGURACIÓN Y ENTORNO
# =========================
ENV_PATH = os.environ.get("ETL_ENV_PATH", r"E:\ETL\ETL_DIARCO\.env")
DEFAULT_LOG_DIR = os.environ.get("TRANSFER_LOG_DIR", r"E:\ETL\ETL_DIARCO\logs")
DEFAULT_REJECT_DIR = os.environ.get("TRANSFER_REJECT_DIR", r"E:\ETL\ETL_DIARCO\salidas\rechazadas")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR = Path(DEFAULT_LOG_DIR)
REJECT_DIR = Path(DEFAULT_REJECT_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)
REJECT_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / f"publicar_transferencias_sgm_{RUN_ID}.log"
REJECT_FILE = REJECT_DIR / f"transferencias_rechazadas_{RUN_ID}.csv"


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("publicar_transferencias_sgm")
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


def log_kv(evento: str, **kwargs):
    """
    Logging simple estilo key=value para facilitar lectura operativa.
    """
    partes = [f"evento={evento}"]
    for k, v in kwargs.items():
        partes.append(f"{k}={repr(v)}")
    logger.info(" | ".join(partes))


if not os.path.exists(ENV_PATH):
    logger.error(f"No existe el archivo .env en: {ENV_PATH}")
    logger.error(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)

secrets = dotenv_values(ENV_PATH)
load_dotenv(ENV_PATH)

log_kv(
    "inicio_configuracion",
    env_path=ENV_PATH,
    pg_db=secrets.get("PGP_DB"),
    pg_host=secrets.get("PGP_HOST"),
    pg_user=secrets.get("PGP_USER"),
    log_file=str(LOG_FILE),
    reject_file=str(REJECT_FILE),
)


# =========================
# 2) CONEXIONES A BASES DE DATOS
# =========================
def get_pg_engine() -> Engine:
    host = os.getenv("PGP_HOST")
    port = os.getenv("PGP_PORT", "5432")
    db = os.getenv("PGP_DB")
    user = os.getenv("PGP_USER")
    pwd = os.getenv("PGP_PASSWORD")

    log_kv("conexion_pg_intento", host=host, port=port, db=db, user=user)

    if not all([host, db, user, pwd, port]):
        raise RuntimeError("Faltan variables de entorno PGP_* para conectarse a PostgreSQL")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def get_sqlserver_engine() -> Engine:
    host = os.getenv("SQL_SERVER")
    port = os.getenv("SQL_PORT", "1433")
    db = os.getenv("SQL_DATABASE", "data-sync")
    user = os.getenv("SQL_USER")
    pwd = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    if not all([host, db, user, pwd]):
        raise RuntimeError("Faltan variables de entorno SQL_SERVER, SQL_DATABASE, SQL_USER, o SQL_PASSWORD")

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
# 3) LECTURA Y NORMALIZACIÓN
# =========================
def obtener_transferencias_precarga(pg_engine: Engine) -> pd.DataFrame:
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
    df = pd.read_sql(sql, pg_engine, parse_dates=["requested_at", "created_at"])
    log_kv("transferencias_precarga_leidas", cantidad=len(df))
    return df


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
# 4) STOCK DISPONIBLE
# =========================
def obtener_stock_disponible(pg_engine: Engine, df_norm: pd.DataFrame) -> pd.DataFrame:
    cols = ["item_code_num", "origin_cd_num"]
    if df_norm.empty or not set(cols).issubset(df_norm.columns):
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible"])

    articulos = sorted([int(x) for x in df_norm["item_code_num"].dropna().unique().tolist() if int(x) > 0])
    sucursales = sorted([int(x) for x in df_norm["origin_cd_num"].dropna().unique().tolist() if int(x) > 0])

    if not articulos or not sucursales:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible"])

    sql = """
        SELECT
            codigo_articulo::bigint AS item_code_num,
            codigo_sucursal::bigint AS origin_cd_num,
            MAX(
                FLOOR(
                    (COALESCE(stock, 0) + COALESCE(transfer_pendiente, 0))
                    / NULLIF(COALESCE(factor_venta, 0), 0)
                )
            )::bigint AS q_bultos_disponible
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

    with pg_engine.connect() as conn:
        df_stock = pd.read_sql(
            text(sql),
            conn,
            params={
                "lista_sucursales": sucursales,
                "lista_articulos": articulos,
            }, # type: ignore
        )

    if df_stock.empty:
        log_kv("stock_disponible_leido", cantidad=0)
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible"])

    df_stock["item_code_num"] = pd.to_numeric(df_stock["item_code_num"], errors="coerce").fillna(0).astype(int)
    df_stock["origin_cd_num"] = pd.to_numeric(df_stock["origin_cd_num"], errors="coerce").fillna(0).astype(int)
    df_stock["q_bultos_disponible"] = pd.to_numeric(df_stock["q_bultos_disponible"], errors="coerce").fillna(0.0)

    log_kv("stock_disponible_leido", cantidad=len(df_stock))
    return df_stock


def enriquecer_con_stock(df_norm: pd.DataFrame, df_stock: pd.DataFrame) -> pd.DataFrame:
    if df_norm.empty:
        df = df_norm.copy()
        df["q_bultos_disponible"] = pd.Series(dtype="float")
        return df

    df = df_norm.merge(
        df_stock,
        how="left",
        on=["item_code_num", "origin_cd_num"],
    )

    df["q_bultos_disponible"] = pd.to_numeric(df["q_bultos_disponible"], errors="coerce").fillna(0.0)
    log_kv("transferencias_enriquecidas_con_stock", cantidad=len(df))
    return df


# =========================
# 5) DETALLES YA PUBLICADOS
# =========================
def obtener_detalles_ya_publicados(sql_engine: Engine, detail_uuids: List[str]) -> set[str]:
    ids = sorted({str(x).strip().lower() for x in detail_uuids if str(x).strip()})
    if not ids:
        return set()

    sql = text("""
        SELECT LOWER(LTRIM(RTRIM(connexa_detail_uuid))) AS connexa_detail_uuid
        FROM repl.TRANSF_CONNEXA_IN
        WHERE connexa_detail_uuid IN :ids
    """).bindparams(bindparam("ids", expanding=True))

    with sql_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"ids": ids}) # type: ignore

    if df.empty:
        log_kv("detalles_ya_publicados_consultados", cantidad=0)
        return set()

    publicados = set(df["connexa_detail_uuid"].astype(str).str.strip().str.lower().tolist())
    log_kv("detalles_ya_publicados_consultados", cantidad=len(publicados))
    return publicados


def marcar_detalles_ya_publicados(df: pd.DataFrame, ya_publicados: set[str]) -> pd.DataFrame:
    out = df.copy()
    out["ya_publicado"] = out["connexa_detail_uuid"].isin(ya_publicados)
    return out


# =========================
# 6) ASIGNACIÓN EN MEMORIA (TODO O NADA)
# =========================
def asignar_stock_en_memoria_todo_o_nada(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        out["saldo_inicial_grupo"] = pd.Series(dtype="float")
        out["saldo_antes"] = pd.Series(dtype="float")
        out["saldo_despues"] = pd.Series(dtype="float")
        out["q_bultos_asignado"] = pd.Series(dtype="float")
        out["publicable"] = pd.Series(dtype="bool")
        out["motivo_no_publicado"] = pd.Series(dtype="object")
        return out

    work = df.copy()

    work["requested_at_ord"] = pd.to_datetime(work["requested_at"], errors="coerce")
    work["created_at_ord"] = pd.to_datetime(work["created_at"], errors="coerce")
    work["qty_requested_num"] = pd.to_numeric(work["qty_requested_num"], errors="coerce").fillna(0.0).round(3)
    work["q_bultos_disponible"] = pd.to_numeric(work["q_bultos_disponible"], errors="coerce").fillna(0.0)

    work = work.sort_values(
        by=["origin_cd_num", "item_code_num", "requested_at_ord", "created_at_ord", "connexa_detail_uuid"],
        ascending=[True, True, True, True, True],
        kind="mergesort"
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
# 7) TRANSFORMACIÓN A STAGING
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
# 8) INSERCIÓN Y UPDATE
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
    except Exception as e:
        logger.exception("Error al insertar en SQL Server staging")
        raise


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


def actualizar_estado_cabeceras(pg_engine: Engine, header_uuids: List[str], nuevo_estado_id: int = 80) -> int:
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

    with pg_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {"nuevo_estado": nuevo_estado_id, "lista_ids": header_uuid_objs},
        )

    log_kv("actualizacion_cabeceras_ok", cantidad=result.rowcount, nuevo_estado_id=nuevo_estado_id)
    return result.rowcount


# =========================
# 9) CABECERAS COMPLETAS
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
# 10) EXPORT DE RECHAZADAS
# =========================
def exportar_rechazadas(df_asignado: pd.DataFrame, output_file: Path) -> int:
    """
    Exporta solo líneas no publicadas y no previamente publicadas.
    """
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


def log_resumen_rechazadas(df_asignado: pd.DataFrame):
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
# 11) MAIN
# =========================
def main() -> int:
    inicio = datetime.now()
    log_kv("inicio_ejecucion", run_id=RUN_ID)

    try:
        pg_engine = get_pg_engine()
        sql_engine = get_sqlserver_engine()

        df_src = obtener_transferencias_precarga(pg_engine)
        if df_src.empty:
            log_kv("sin_datos_precarga", mensaje="No hay transferencias en PRECARGA_CONNEXA para publicar.")
            return 0

        df_norm = normalizar_transferencias(df_src)

        ya_publicados = obtener_detalles_ya_publicados(
            sql_engine,
            df_norm["connexa_detail_uuid"].astype(str).tolist()
        )

        df_stock = obtener_stock_disponible(pg_engine, df_norm)
        df_work = enriquecer_con_stock(df_norm, df_stock)
        df_work = marcar_detalles_ya_publicados(df_work, ya_publicados)

        df_asignado = asignar_stock_en_memoria_todo_o_nada(df_work)
        df_a_insertar = df_asignado[df_asignado["publicable_ahora"]].copy()

        df_stg = transformar_a_staging(df_a_insertar)

        if not df_stg.empty:
            n_insertadas = insertar_en_staging_sqlserver(df_stg, sql_engine)
        else:
            n_insertadas = 0
            log_kv("sin_filas_para_insertar", mensaje="No hay filas nuevas para insertar en staging.")

        header_uuids_actualizables = obtener_headers_completamente_publicables(df_asignado)
        if header_uuids_actualizables:
            n_headers_actualizadas = actualizar_estado_cabeceras(
                pg_engine,
                header_uuids_actualizables,
                nuevo_estado_id=80
            )
        else:
            n_headers_actualizadas = 0
            log_kv("sin_cabeceras_para_actualizar", mensaje="No hay cabeceras completas para actualizar a 80.")

        n_rechazadas = exportar_rechazadas(df_asignado, REJECT_FILE)
        log_resumen_rechazadas(df_asignado)

        total_origen = len(df_asignado)
        total_ya_publicado = int(df_asignado["ya_publicado"].sum()) if not df_asignado.empty else 0
        total_publicable_ahora = int(df_asignado["publicable_ahora"].sum()) if not df_asignado.empty else 0
        total_no_publicable = int((~df_asignado["publicable"] & ~df_asignado["ya_publicado"]).sum()) if not df_asignado.empty else 0

        fin = datetime.now()
        duracion_seg = round((fin - inicio).total_seconds(), 2)

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
            duracion_segundos=duracion_seg,
            log_file=str(LOG_FILE),
            reject_file=str(REJECT_FILE) if n_rechazadas > 0 else "",
        )

        return 0

    except RuntimeError as e:
        logger.error(f"Error de configuración de entorno: {e}")
        logger.error(traceback.format_exc())
        return 1
    except Exception as e:
        logger.exception(f"Ocurrió un error inesperado: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())