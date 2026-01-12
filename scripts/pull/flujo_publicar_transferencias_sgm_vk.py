# scripts/pull/flujo_publicar_transferencias_sgm_vk.py
from __future__ import annotations

import importlib.util
import os
import sys
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =============================================================================
# CONFIG
# =============================================================================

ENV_PATH = os.environ.get("ETL_ENV_PATH", r"E:\ETL\ETL_DIARCO\.env")


@dataclass(frozen=True)
class FlowConfig:
    # Lotes / SP
    batch_size: int = 500
    max_seconds_sp: int = 120

    # Looping
    max_loops: int = 200
    sleep_seconds_between_loops: float = 0.25

    # Estados Connexa
    status_id_ok: int = 10
    status_id_error: int = 85
    status_id_syncing: int = 80

    # Stored Procedures (SQL Server - data-sync / repl)
    sp_publicar_sgm: str = "repl.SP_PUBLICAR_TRANSF_CONNEXA_SGM"
    sp_publicar_vk: str = "repl.SP_PUBLICAR_TRANSF_CONNEXA_VK"
    sp_retorno_cabeceras: str = "repl.SP_TRANSF_CONNEXA_RETORNO_CABECERAS"


# =============================================================================
# ENV + ENGINES
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
# HELPERS
# =============================================================================


def _load_module_from_path(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar spec para {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _to_uuid_list(values: List[str]) -> List[uuid.UUID]:
    out: List[uuid.UUID] = []
    for v in values or []:
        if v is None:
            continue
        s = str(v).strip().lower()
        if not s:
            continue
        try:
            out.append(uuid.UUID(s))
        except Exception:
            # si llega basura, simplemente se ignora
            pass
    return out


# =============================================================================
# TASKS
# =============================================================================


@task(name="Pre-publicar transferencias (Connexa -> Staging SQL Server)", retries=0, cache_policy=NO_CACHE)
def ejecutar_pre_publicacion_staging() -> int:
    """
    Ejecuta scripts/pull/publicar_transferencias_sgm.py (versión original),
    de forma sincrónica (bloqueante), y devuelve su exit code.
    """
    logger = get_run_logger()

    base_dir = Path(__file__).resolve().parent  # scripts/pull
    script_path = base_dir / "publicar_transferencias_sgm.py"

    if not script_path.exists():
        raise FileNotFoundError(f"No se encontró el script previo en: {script_path}")

    logger.info(f"Ejecutando pre-publicación: {script_path}")

    mod = _load_module_from_path("publicar_transferencias_sgm_mod", script_path)

    if not hasattr(mod, "main"):
        raise AttributeError("El script publicar_transferencias_sgm.py no expone función main().")

    rc = int(mod.main() or 0)
    if rc != 0:
        raise RuntimeError(f"Pre-publicación a staging falló. Exit code={rc}")

    logger.info("Pre-publicación a staging finalizada OK.")
    return rc


@task(name="Ejecutar SP por lote (SQL Server)", retries=0, cache_policy=NO_CACHE)
def ejecutar_sp_batch(sp_fullname: str, batch_size: int, max_seconds_sp: int) -> Dict[str, int]:
    """
    Ejecuta un SP (SQL Server) que devuelve un dataset con columnas:
    claimed, processed, elapsed_s.
    IMPORTANTE: se ejecuta en AUTOCOMMIT para evitar promoción a transacciones distribuidas
    (MSDTC) cuando el SP toca linked servers.
    """
    logger = get_run_logger()
    sql_engine = get_sqlserver_engine()

    sql = text(f"EXEC {sp_fullname} @BatchSize=:bs, @MaxSeconds=:ms;")

    with sql_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        df = pd.read_sql(sql, conn, params={"bs": batch_size, "ms": max_seconds_sp})

    if df.empty:
        logger.warning("El SP no devolvió dataset; se asume claimed=0.")
        return {"claimed": 0, "processed": 0, "elapsed_s": 0}

    row = df.iloc[0].to_dict()
    claimed = int(row.get("claimed", 0))
    processed = int(row.get("processed", 0))
    elapsed_s = int(row.get("elapsed_s", 0))

    logger.info(f"{sp_fullname}: claimed={claimed}, processed={processed}, elapsed_s={elapsed_s}")
    return {"claimed": claimed, "processed": processed, "elapsed_s": elapsed_s}


@task(name="Loop SP hasta agotar pendientes", retries=0, cache_policy=NO_CACHE)
def loop_sp_hasta_agotar(
    sp_fullname: str,
    batch_size: int,
    max_seconds_sp: int,
    max_loops: int,
    sleep_s: float,
) -> Dict[str, int]:
    logger = get_run_logger()
    total_claimed = 0
    total_processed = 0
    loops = 0
    
    stalled_count = 0
    max_stalled = 5

    while loops < max_loops:
        loops += 1
        res = ejecutar_sp_batch(sp_fullname=sp_fullname, batch_size=batch_size, max_seconds_sp=max_seconds_sp)
        total_claimed += res["claimed"]
        total_processed += res["processed"]

        if res["claimed"] == 0:
            logger.info(
                f"Loop finalizado para {sp_fullname}. loops={loops}, "
                f"total_claimed={total_claimed}, total_processed={total_processed}"
            )
            break
        
        if res["claimed"] > 0 and res["processed"] == 0:
            stalled_count += 1
        else:
            stalled_count = 0

        if stalled_count >= max_stalled:
            logger.error(
                f"SP {sp_fullname} estancado: claimed>0 pero processed=0 "
                f"por {stalled_count} iteraciones. Se corta para evitar loop infinito."
            )
            break

        if sleep_s and sleep_s > 0:
            time.sleep(sleep_s)

    if loops >= max_loops:
        logger.warning(
            f"Se alcanzó max_loops={max_loops} en {sp_fullname}. "
            f"total_claimed={total_claimed}, total_processed={total_processed}"
        )

    return {"loops": loops, "total_claimed": total_claimed, "total_processed": total_processed}


@task(name="Leer retorno por cabeceras (SQL Server)", retries=0, cache_policy=NO_CACHE)
def leer_retorno_cabeceras(sp_retorno_cabeceras: str) -> pd.DataFrame:
    """
    Lee cabeceras cerrables desde SQL Server.
    Se usa AUTOCOMMIT para evitar efectos colaterales si el SP toca linked servers o registra eventos.
    """
    sql_engine = get_sqlserver_engine()
    sql = text(f"EXEC {sp_retorno_cabeceras} @SoloCerrables=1;")

    with sql_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        df = pd.read_sql(sql, conn)

    return df


@task(name="Actualizar estados en Connexa (PostgreSQL)", retries=0, cache_policy=NO_CACHE)
def actualizar_estados_connexa(
    df_retorno: pd.DataFrame,
    status_ok: int,
    status_error: int,
) -> Tuple[int, int]:
    """
    Actualiza supply_planning.spl_distribution_transfer según retorno de SQL Server.
    Espera columnas: connexa_header_uuid, resultado (OK/ERROR)
    """
    logger = get_run_logger()
    pg_engine = get_pg_engine()

    if df_retorno is None or df_retorno.empty:
        logger.info("No hay cabeceras cerrables para actualizar en Connexa.")
        return (0, 0)

    df = df_retorno.copy()

    if "connexa_header_uuid" not in df.columns or "resultado" not in df.columns:
        raise KeyError(
            "El retorno de cabeceras no trae columnas esperadas: connexa_header_uuid, resultado. "
            f"Columnas recibidas: {list(df.columns)}"
        )

    df["connexa_header_uuid"] = df["connexa_header_uuid"].astype(str).str.lower()
    df["resultado"] = df["resultado"].astype(str).str.upper()

    ok_ids = df.loc[df["resultado"] == "OK", "connexa_header_uuid"].dropna().unique().tolist()
    err_ids = df.loc[df["resultado"] == "ERROR", "connexa_header_uuid"].dropna().unique().tolist()

    ok_uuids = _to_uuid_list(ok_ids)
    err_uuids = _to_uuid_list(err_ids)

    ok_count = 0
    err_count = 0

    q = text(
        """
        UPDATE supply_planning.spl_distribution_transfer
           SET status_id = :status_id,
               updated_at = NOW()
         WHERE id = ANY(CAST(:uuids AS uuid[]))
        """
    )

    with pg_engine.begin() as conn:
        if ok_uuids:
            res_ok = conn.execute(q, {"status_id": status_ok, "uuids": ok_uuids})
            ok_count = int(res_ok.rowcount or 0)

        if err_uuids:
            res_err = conn.execute(q, {"status_id": status_error, "uuids": err_uuids})
            err_count = int(res_err.rowcount or 0)

    logger.info(f"Connexa actualizado: OK={ok_count}, ERROR={err_count}")
    return (ok_count, err_count)


# =============================================================================
# FLOW
# =============================================================================


@flow(name="Publicar transferencias Connexa SGM Valkimia Retorno")
def publicar_transferencias_sgm_vk(cfg: Optional[FlowConfig] = None) -> Dict[str, object]:
    logger = get_run_logger()
    _require_env_file()
    cfg = cfg or FlowConfig()

    # 0) PRE-ETAPA: cargar staging + pasar cabeceras a 80 (lo hace el script previo)
    ejecutar_pre_publicacion_staging()

    # 1) Publicación en SQL Server (SPs por lote)
    logger.info("Iniciando publicación de transferencias: etapa SGM...")
    sgm_tot = loop_sp_hasta_agotar(
        sp_fullname=cfg.sp_publicar_sgm,
        batch_size=cfg.batch_size,
        max_seconds_sp=cfg.max_seconds_sp,
        max_loops=cfg.max_loops,
        sleep_s=cfg.sleep_seconds_between_loops,
    )

    logger.info("Iniciando publicación de transferencias: etapa VALKIMIA...")
    vk_tot = loop_sp_hasta_agotar(
        sp_fullname=cfg.sp_publicar_vk,
        batch_size=cfg.batch_size,
        max_seconds_sp=cfg.max_seconds_sp,
        max_loops=cfg.max_loops,
        sleep_s=cfg.sleep_seconds_between_loops,
    )

    # 2) Retorno por cabeceras
    logger.info("Leyendo retorno por cabeceras (considerando SGM+VK)...")
    df_ret = leer_retorno_cabeceras(sp_retorno_cabeceras=cfg.sp_retorno_cabeceras)
    logger.info(f"Cabeceras cerrables devueltas por retorno: {len(df_ret)}")

    # 3) Update estados en Connexa
    logger.info("Actualizando estados de cabecera en Connexa según retorno...")
    ok_count, err_count = actualizar_estados_connexa(
        df_retorno=df_ret,
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
    res = publicar_transferencias_sgm_vk()
    print(res)
