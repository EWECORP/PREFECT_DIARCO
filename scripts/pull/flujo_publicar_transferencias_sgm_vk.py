# scripts/pull/flujo_publicar_transferencias_sgm_vk.py
from __future__ import annotations

import os
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from prefect import flow, task, get_run_logger


# =============================================================================
# CONFIG
# =============================================================================

ENV_PATH = os.environ.get("ETL_ENV_PATH", r"E:\ETL\ETL_DIARCO\.env")


@dataclass(frozen=True)
class FlowConfig:
    # Batches (SQL Server SP)
    batch_size: int = 500
    max_seconds_sp: int = 120

    # Loop control
    max_loops: int = 200              # corte de seguridad
    sleep_seconds_between_loops: float = 0.25

    # Estados Connexa
    status_id_ok: int = 10            # SOLICITADA (o el que definan como "publicada OK")
    status_id_error: int = 85         # ERROR_SINCRONIZACION (recomendado crear)
    status_id_syncing: int = 80       # SINCRONIZANDO (ya lo usan)

    # SPs
    sp_publicar_sgm: str = "repl.SP_PUBLICAR_TRANSF_CONNEXA_SGM"
    sp_publicar_vk: str = "repl.SP_PUBLICAR_TRANSF_CONNEXA_VK"
    sp_retorno_cabeceras: str = "repl.SP_TRANSF_CONNEXA_RETORNO_CABECERAS"  # versión extendida a VK


# =============================================================================
# ENGINES
# =============================================================================

def _require_env_file() -> None:
    if not os.path.exists(ENV_PATH):
        print(f"[ERROR] No existe el archivo .env en: {ENV_PATH}")
        print(f"[DEBUG] Directorio actual: {os.getcwd()}")
        sys.exit(1)
    load_dotenv(ENV_PATH)


def get_pg_engine() -> Engine:
    host = os.getenv("PGP_HOST")
    port = os.getenv("PGP_PORT", "5432")
    db = os.getenv("PGP_DB")
    user = os.getenv("PGP_USER")
    pwd = os.getenv("PGP_PASSWORD")

    if not all([host, port, db, user, pwd]):
        raise RuntimeError("Faltan variables PGP_* para conectarse a PostgreSQL (Connexa).")

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
        raise RuntimeError("Faltan variables SQL_SERVER/SQL_DATABASE/SQL_USER/SQL_PASSWORD para SQL Server.")

    timeout_seconds = 30
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


# =============================================================================
# TASKS
# =============================================================================

@task(name="Ejecutar SP por lote (SQL Server)", retries=0)
def ejecutar_sp_batch(sql_engine: Engine, sp_fullname: str, batch_size: int, max_seconds_sp: int) -> Dict[str, int]:
    """
    Ejecuta un SP que devuelve un SELECT con columnas: claimed, processed, elapsed_s.
    """
    logger = get_run_logger()
    sql = text(f"EXEC {sp_fullname} @BatchSize=:bs, @MaxSeconds=:ms;")

    with sql_engine.begin() as conn:
        df = pd.read_sql(sql, conn, params={"bs": batch_size, "ms": max_seconds_sp})

    if df.empty:
        # fallback defensivo
        logger.warning("El SP no devolvió dataset; se asume claimed=0.")
        return {"claimed": 0, "processed": 0, "elapsed_s": 0}

    row = df.iloc[0].to_dict()
    claimed = int(row.get("claimed", 0))
    processed = int(row.get("processed", 0))
    elapsed_s = int(row.get("elapsed_s", 0))

    logger.info(f"{sp_fullname}: claimed={claimed}, processed={processed}, elapsed_s={elapsed_s}")
    return {"claimed": claimed, "processed": processed, "elapsed_s": elapsed_s}


@task(name="Loop SP hasta agotar pendientes", retries=0)
def loop_sp_hasta_agotar(
    sql_engine: Engine,
    sp_fullname: str,
    batch_size: int,
    max_seconds_sp: int,
    max_loops: int,
    sleep_s: float,
) -> Dict[str, int]:
    """
    Ejecuta el SP en loop hasta que claimed=0 o se alcance max_loops.
    Devuelve totales.
    """
    logger = get_run_logger()
    total_claimed = 0
    total_processed = 0
    loops = 0

    while loops < max_loops:
        loops += 1
        res = ejecutar_sp_batch(sql_engine, sp_fullname, batch_size, max_seconds_sp)
        total_claimed += res["claimed"]
        total_processed += res["processed"]

        if res["claimed"] == 0:
            logger.info(f"Loop finalizado para {sp_fullname}. loops={loops}, total_claimed={total_claimed}, total_processed={total_processed}")
            break

        if sleep_s > 0:
            time.sleep(sleep_s)

    if loops >= max_loops:
        logger.warning(f"Se alcanzó max_loops={max_loops} en {sp_fullname}. total_claimed={total_claimed}, total_processed={total_processed}")

    return {"loops": loops, "total_claimed": total_claimed, "total_processed": total_processed}


@task(name="Leer retorno por cabeceras (SQL Server)", retries=0)
def leer_retorno_cabeceras(sql_engine: Engine) -> pd.DataFrame:
    """
    Lee el retorno consolidado por cabecera desde SQL Server.
    Se asume que el SP devuelve:
      connexa_header_uuid, cnt_total, cnt_ok, cnt_error, cnt_abiertas, last_processed_at, resultado, mensaje_error
    y que resultado ya contempla estado_vk.
    """
    sql = text("EXEC repl.SP_TRANSF_CONNEXA_RETORNO_CABECERAS @SoloCerrables=1;")
    with sql_engine.begin() as conn:
        df = pd.read_sql(sql, conn)
    return df


@task(name="Actualizar estados en Connexa (PostgreSQL)", retries=0)
def actualizar_estados_connexa(
    pg_engine: Engine,
    df_retorno: pd.DataFrame,
    status_ok: int,
    status_error: int,
) -> Tuple[int, int]:
    """
    Actualiza status_id en supply_planning.spl_distribution_transfer para cabeceras cerrables.
    - resultado='OK' -> status_ok
    - resultado='ERROR' -> status_error
    Devuelve (ok_count, error_count)
    """
    logger = get_run_logger()

    if df_retorno is None or df_retorno.empty:
        logger.info("No hay cabeceras cerrables para actualizar en Connexa.")
        return (0, 0)

    # Normalizar
    df = df_retorno.copy()
    df["connexa_header_uuid"] = df["connexa_header_uuid"].astype(str).str.lower()
    df["resultado"] = df["resultado"].astype(str).str.upper()

    ok_uuids = df.loc[df["resultado"] == "OK", "connexa_header_uuid"].dropna().unique().tolist()
    err_uuids = df.loc[df["resultado"] == "ERROR", "connexa_header_uuid"].dropna().unique().tolist()

    ok_count = 0
    err_count = 0

    with pg_engine.begin() as conn:
        if ok_uuids:
            q_ok = text("""
                UPDATE supply_planning.spl_distribution_transfer
                   SET status_id = :status_id,
                       updated_at = NOW()
                 WHERE id = ANY(:uuids)
            """)
            res_ok = conn.execute(q_ok, {"status_id": status_ok, "uuids": ok_uuids})
            ok_count = int(res_ok.rowcount or 0)

        if err_uuids:
            q_err = text("""
                UPDATE supply_planning.spl_distribution_transfer
                   SET status_id = :status_id,
                       updated_at = NOW()
                 WHERE id = ANY(:uuids)
            """)
            res_err = conn.execute(q_err, {"status_id": status_error, "uuids": err_uuids})
            err_count = int(res_err.rowcount or 0)

    logger.info(f"Connexa actualizado: OK={ok_count}, ERROR={err_count}")
    return (ok_count, err_count)


# =============================================================================
# FLOW
# =============================================================================

@flow(name="Publicar transferencias Connexa -> SGM + Valkimia + Retorno")
def publicar_transferencias_sgm_vk(cfg: Optional[FlowConfig] = None) -> Dict[str, object]:
    """
    Orquesta:
      1) SP_PUBLICAR_TRANSF_CONNEXA_SGM hasta agotar
      2) SP_PUBLICAR_TRANSF_CONNEXA_VK hasta agotar
      3) SP_RETNO_CABECERAS (cerrables)
      4) Update Connexa status_id (OK/ERROR)
    """
    logger = get_run_logger()
    _require_env_file()
    cfg = cfg or FlowConfig()

    sql_engine = get_sqlserver_engine()
    pg_engine = get_pg_engine()

    logger.info("Iniciando publicación de transferencias: etapa SGM...")
    sgm_tot = loop_sp_hasta_agotar(
        sql_engine,
        cfg.sp_publicar_sgm,
        cfg.batch_size,
        cfg.max_seconds_sp,
        cfg.max_loops,
        cfg.sleep_seconds_between_loops,
    )

    logger.info("Iniciando publicación de transferencias: etapa VALKIMIA...")
    vk_tot = loop_sp_hasta_agotar(
        sql_engine,
        cfg.sp_publicar_vk,
        cfg.batch_size,
        cfg.max_seconds_sp,
        cfg.max_loops,
        cfg.sleep_seconds_between_loops,
    )

    logger.info("Leyendo retorno por cabeceras (considerando SGM+VK)...")
    df_ret = leer_retorno_cabeceras(sql_engine)
    logger.info(f"Cabeceras cerrables devueltas por retorno: {len(df_ret)}")

    logger.info("Actualizando estados de cabecera en Connexa según retorno...")
    ok_count, err_count = actualizar_estados_connexa(
        pg_engine,
        df_ret,
        status_ok=cfg.status_id_ok,
        status_error=cfg.status_id_error,
    )

    salida = {
        "sgm": sgm_tot,
        "vk": vk_tot,
        "retorno_cabeceras": int(len(df_ret)),
        "connexa_ok_updated": ok_count,
        "connexa_error_updated": err_count,
    }
    logger.info(f"Resumen: {salida}")
    return salida


if __name__ == "__main__":
    # Permite ejecución local sin orquestación (útil para pruebas en DMZ)
    res = publicar_transferencias_sgm_vk()
    print(res)
