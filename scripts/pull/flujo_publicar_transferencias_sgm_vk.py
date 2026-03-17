# scripts/pull/flujo_publicar_transferencias_sgm_vk.py
from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

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

    # Looping general
    max_loops: int = 200
    sleep_seconds_between_loops: float = 0.25
    max_stalled_loops: int = 5

    # Estados Connexa
    status_id_syncing: int = 80
    status_id_ok: int = 60
    status_id_error: int = 85

    # Stored Procedures (SQL Server - data-sync / repl)
    sp_publicar_sgm: str = "repl.SP_PUBLICAR_TRANSF_CONNEXA_SGM"
    sp_publicar_vk: str = "repl.SP_PUBLICAR_TRANSF_CONNEXA_VK"

    # Vista de retorno consolidado desde Valkimia
    vw_retorno_cabeceras: str = "repl.V_CONNEXA_RETORNO_CABECERAS_VK"

    # Política por etapa
    sgm_fail_on_stall: bool = True
    sgm_fail_on_max_loops: bool = True

    vk_fail_on_stall: bool = False
    vk_fail_on_max_loops: bool = False


# =============================================================================
# ENV + ENGINES
# =============================================================================
def build_pg_engine(host: str, port: str, db: str, user: str, pwd: str) -> Engine:
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def _require_env_file() -> None:
    if not os.path.exists(ENV_PATH):
        raise FileNotFoundError(f"No existe el archivo .env en: {ENV_PATH}")
    load_dotenv(ENV_PATH)


def get_pg_connexa_engine() -> Engine:
    host = os.getenv("PGP_HOST")
    port = os.getenv("PGP_PORT", "5432")
    db = os.getenv("PGP_DB")
    user = os.getenv("PGP_USER")
    pwd = os.getenv("PGP_PASSWORD")

    if not all([host, port, db, user, pwd]):
        raise RuntimeError("Faltan variables PGP_* para conectarse a PostgreSQL (Connexa).")

    return build_pg_engine(host, port, db, user, pwd)


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
def _to_uuid_str_list(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    for v in values or []:
        if v is None:
            continue
        s = str(v).strip().lower()
        if not s:
            continue
        try:
            out.append(str(uuid.UUID(s)))
        except Exception:
            pass
    return sorted(set(out))


# =============================================================================
# TASKS
# =============================================================================
@task(name="Pre-publicar transferencias (Connexa -> Staging SQL Server)", retries=0, cache_policy=NO_CACHE)
def ejecutar_pre_publicacion_staging() -> int:
    """
    Ejecuta scripts/pull/publicar_transferencias_sgm.py como subprocess
    y corta el flujo si retorna código distinto de 0.
    """
    logger = get_run_logger()

    base_dir = Path(__file__).resolve().parent  # scripts/pull
    script_path = base_dir / "publicar_transferencias_sgm.py"

    if not script_path.exists():
        raise FileNotFoundError(f"No se encontró el script previo en: {script_path}")

    python_exe = sys.executable
    cmd = [python_exe, str(script_path)]

    logger.info(f"Ejecutando pre-publicación: {' '.join(cmd)}")

    workdir = base_dir.parent.parent  # raíz del proyecto ETL_DIARCO

    result = subprocess.run(
        cmd,
        cwd=str(workdir),
        capture_output=True,
        text=True,
        shell=False,
    )

    if result.stdout:
        logger.info("STDOUT pre-publicación:\n%s", result.stdout)

    if result.stderr:
        logger.error("STDERR pre-publicación:\n%s", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Pre-publicación a staging falló. Exit code={result.returncode}")

    logger.info("Pre-publicación a staging finalizada OK.")
    return result.returncode


@task(name="Ejecutar SP por lote (SQL Server)", retries=0, cache_policy=NO_CACHE)
def ejecutar_sp_batch(sp_fullname: str, batch_size: int, max_seconds_sp: int) -> Dict[str, int]:
    """
    Ejecuta un SP (SQL Server) que devuelve un dataset con columnas:
    claimed, processed, elapsed_s.

    IMPORTANTE:
    Se ejecuta en AUTOCOMMIT para evitar promoción a transacciones distribuidas
    (MSDTC) cuando el SP toca linked servers.
    """
    logger = get_run_logger()
    sql_engine = get_sqlserver_engine()

    sql = text(f"EXEC {sp_fullname} @BatchSize=:bs, @MaxSeconds=:ms;")

    with sql_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        df = pd.read_sql(sql, conn, params={"bs": batch_size, "ms": max_seconds_sp})

    if df.empty:
        logger.warning(f"{sp_fullname}: no devolvió dataset; se asume claimed=0.")
        return {"claimed": 0, "processed": 0, "elapsed_s": 0}

    row = df.iloc[0].to_dict()
    claimed = int(row.get("claimed", 0) or 0)
    processed = int(row.get("processed", 0) or 0)
    elapsed_s = int(row.get("elapsed_s", 0) or 0)

    logger.info(f"{sp_fullname}: claimed={claimed}, processed={processed}, elapsed_s={elapsed_s}")
    return {"claimed": claimed, "processed": processed, "elapsed_s": elapsed_s}


@task(name="Loop SP hasta agotar pendientes", retries=0, cache_policy=NO_CACHE)
def loop_sp_hasta_agotar(
    sp_fullname: str,
    batch_size: int,
    max_seconds_sp: int,
    max_loops: int,
    sleep_s: float,
    max_stalled_loops: int,
    fail_on_stall: bool = True,
    fail_on_max_loops: bool = True,
) -> Dict[str, int]:
    logger = get_run_logger()

    total_claimed = 0
    total_processed = 0
    loops = 0
    stalled_count = 0
    ultimo_claimed = 0
    motivo_salida = "AGOTADO"

    while loops < max_loops:
        loops += 1

        res = ejecutar_sp_batch(
            sp_fullname=sp_fullname,
            batch_size=batch_size,
            max_seconds_sp=max_seconds_sp,
        )

        total_claimed += res["claimed"]
        total_processed += res["processed"]
        ultimo_claimed = res["claimed"]

        if res["claimed"] == 0:
            motivo_salida = "AGOTADO"
            logger.info(
                f"Loop finalizado para {sp_fullname}. "
                f"motivo={motivo_salida}, loops={loops}, "
                f"total_claimed={total_claimed}, total_processed={total_processed}"
            )
            return {
                "loops": loops,
                "total_claimed": total_claimed,
                "total_processed": total_processed,
                "motivo_salida": motivo_salida,
            }

        if res["claimed"] > 0 and res["processed"] == 0:
            stalled_count += 1
        else:
            stalled_count = 0

        if stalled_count >= max_stalled_loops:
            motivo_salida = "STALL"
            msg = (
                f"SP {sp_fullname} estancado: claimed>0 pero processed=0 "
                f"durante {stalled_count} iteraciones consecutivas."
            )
            if fail_on_stall:
                raise RuntimeError(msg)

            logger.warning(msg + " Se continúa con el flujo.")
            break

        if sleep_s and sleep_s > 0:
            time.sleep(sleep_s)

    if loops >= max_loops and ultimo_claimed > 0:
        motivo_salida = "MAX_LOOPS"
        msg = (
            f"Se alcanzó max_loops={max_loops} en {sp_fullname} sin agotar pendientes. "
            f"total_claimed={total_claimed}, total_processed={total_processed}"
        )
        if fail_on_max_loops:
            raise RuntimeError(msg)

        logger.warning(msg + " Se continúa con el flujo.")

    logger.info(
        f"Loop finalizado para {sp_fullname}. "
        f"motivo={motivo_salida}, loops={loops}, "
        f"total_claimed={total_claimed}, total_processed={total_processed}"
    )

    return {
        "loops": loops,
        "total_claimed": total_claimed,
        "total_processed": total_processed,
        "motivo_salida": motivo_salida,
    }


@task(name="Leer retorno por cabeceras desde vista SQL Server", retries=0, cache_policy=NO_CACHE)
def leer_retorno_cabeceras_desde_vista(vw_retorno_cabeceras: str) -> pd.DataFrame:
    """
    Lee cabeceras cerrables desde la vista consolidada SQL Server.
    Se esperan como mínimo las columnas:
      - connexa_header_uuid
      - resultado
    """
    logger = get_run_logger()
    sql_engine = get_sqlserver_engine()

    sql = text(f"""
        SELECT
            connexa_header_uuid,
            resultado,
            cerrable,
            total_lineas,
            cant_aco,
            cant_pre,
            cant_rem,
            cant_etr,
            cant_otro
        FROM {vw_retorno_cabeceras}
        WHERE cerrable = 1
    """)

    with sql_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        df = pd.read_sql(sql, conn)

    logger.info(f"Cabeceras cerrables leídas desde vista: {len(df)}")

    if not df.empty and "resultado" in df.columns:
        resumen = (
            df["resultado"]
            .astype(str)
            .str.upper()
            .value_counts(dropna=False)
            .to_dict()
        )
        logger.info(f"Distribución de resultados en vista: {resumen}")

    return df


@task(name="Actualizar estados en Connexa (PostgreSQL)", retries=0, cache_policy=NO_CACHE)
def actualizar_estados_connexa(
    df_retorno: pd.DataFrame,
    status_ok: int,
    status_error: int,
    status_id_origen: int,
) -> Tuple[int, int]:
    """
    Actualiza supply_planning.spl_distribution_transfer según retorno de SQL Server.
    Espera columnas:
      - connexa_header_uuid
      - resultado  (OK / ERROR)

    Solo actualiza cabeceras cuyo estado actual sea status_id_origen.
    """
    logger = get_run_logger()
    pg_engine = get_pg_connexa_engine()

    if df_retorno is None or df_retorno.empty:
        logger.info("No hay cabeceras cerrables para actualizar en Connexa.")
        return (0, 0)

    df = df_retorno.copy()

    if "connexa_header_uuid" not in df.columns or "resultado" not in df.columns:
        raise KeyError(
            "El retorno no trae columnas esperadas: connexa_header_uuid, resultado. "
            f"Columnas recibidas: {list(df.columns)}"
        )

    df["connexa_header_uuid"] = df["connexa_header_uuid"].astype(str).str.lower().str.strip()
    df["resultado"] = df["resultado"].astype(str).str.upper().str.strip()

    ok_ids = df.loc[df["resultado"] == "OK", "connexa_header_uuid"].dropna().unique().tolist()
    err_ids = df.loc[df["resultado"] == "ERROR", "connexa_header_uuid"].dropna().unique().tolist()

    ok_uuid_list = _to_uuid_str_list(ok_ids)
    err_uuid_list = _to_uuid_str_list(err_ids)

    ok_count = 0
    err_count = 0

    q = text(
        """
        UPDATE supply_planning.spl_distribution_transfer
           SET status_id = :status_id,
               updated_at = NOW()
         WHERE id = ANY(CAST(:uuids AS uuid[]))
           AND status_id = :status_id_origen
        """
    )

    with pg_engine.begin() as conn:
        if ok_uuid_list:
            res_ok = conn.execute(
                q,
                {
                    "status_id": status_ok,
                    "uuids": ok_uuid_list,
                    "status_id_origen": status_id_origen,
                },
            )
            ok_count = int(res_ok.rowcount or 0)

        if err_uuid_list:
            res_err = conn.execute(
                q,
                {
                    "status_id": status_error,
                    "uuids": err_uuid_list,
                    "status_id_origen": status_id_origen,
                },
            )
            err_count = int(res_err.rowcount or 0)

    logger.info(
        "Connexa actualizado desde retorno de vista: "
        f"OK={ok_count}, ERROR={err_count}, status_origen={status_id_origen}"
    )
    return (ok_count, err_count)


# =============================================================================
# FLOW
# =============================================================================
@flow(name="Publicar transferencias Connexa SGM Valkimia Retorno")
def publicar_transferencias_sgm_vk(cfg: Optional[FlowConfig] = None) -> Dict[str, object]:
    logger = get_run_logger()

    _require_env_file()
    cfg = cfg or FlowConfig()

    logger.info("Inicio de flujo publicar_transferencias_sgm_vk")
    logger.info(
        "Configuración: "
        f"batch_size={cfg.batch_size}, "
        f"max_seconds_sp={cfg.max_seconds_sp}, "
        f"max_loops={cfg.max_loops}, "
        f"sleep_seconds_between_loops={cfg.sleep_seconds_between_loops}, "
        f"max_stalled_loops={cfg.max_stalled_loops}, "
        f"status_id_syncing={cfg.status_id_syncing}, "
        f"status_id_ok={cfg.status_id_ok}, "
        f"status_id_error={cfg.status_id_error}, "
        f"sgm_fail_on_stall={cfg.sgm_fail_on_stall}, "
        f"sgm_fail_on_max_loops={cfg.sgm_fail_on_max_loops}, "
        f"vk_fail_on_stall={cfg.vk_fail_on_stall}, "
        f"vk_fail_on_max_loops={cfg.vk_fail_on_max_loops}"
    )

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
        max_stalled_loops=cfg.max_stalled_loops,
        fail_on_stall=cfg.sgm_fail_on_stall,
        fail_on_max_loops=cfg.sgm_fail_on_max_loops,
    )

    logger.info("Iniciando publicación de transferencias: etapa VALKIMIA...")
    vk_tot = loop_sp_hasta_agotar(
        sp_fullname=cfg.sp_publicar_vk,
        batch_size=cfg.batch_size,
        max_seconds_sp=cfg.max_seconds_sp,
        max_loops=cfg.max_loops,
        sleep_s=cfg.sleep_seconds_between_loops,
        max_stalled_loops=cfg.max_stalled_loops,
        fail_on_stall=cfg.vk_fail_on_stall,
        fail_on_max_loops=cfg.vk_fail_on_max_loops,
    )

    # 2) Retorno por cabeceras desde vista SQL Server
    logger.info("Leyendo retorno por cabeceras desde vista consolidada de Valkimia...")
    df_ret = leer_retorno_cabeceras_desde_vista(vw_retorno_cabeceras=cfg.vw_retorno_cabeceras)
    logger.info(f"Cabeceras cerrables devueltas por vista: {len(df_ret)}")

    # 3) Update estados en Connexa
    logger.info("Actualizando estados de cabecera en Connexa según retorno de vista...")
    ok_count, err_count = actualizar_estados_connexa(
        df_retorno=df_ret,
        status_ok=cfg.status_id_ok,
        status_error=cfg.status_id_error,
        status_id_origen=cfg.status_id_syncing,
    )

    salida = {
        "sgm": sgm_tot,
        "vk": vk_tot,
        "retorno_cabeceras": int(len(df_ret)),
        "connexa_ok_updated": ok_count,
        "connexa_error_updated": err_count,
        "vista_retorno": cfg.vw_retorno_cabeceras,
    }

    logger.info(f"Resumen final del flujo: {salida}")
    return salida


if __name__ == "__main__":
    res = publicar_transferencias_sgm_vk()
    print(res)