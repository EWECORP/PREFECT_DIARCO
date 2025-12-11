# scripts/pull/publicar_transferencias_sgm.py

import os
import sys
import urllib.parse
from math import ceil  # ya no lo usamos directamente, pero se deja por si luego se quiere volver a usar
from datetime import datetime

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import dotenv_values, load_dotenv

# =========================
# 1) CONFIGURACIÓN Y ENTORNO
# =========================
ENV_PATH = os.environ.get("ETL_ENV_PATH", r"E:\ETL\ETL_DIARCO\.env")
if not os.path.exists(ENV_PATH):
    print(f"[ERROR] No existe el archivo .env en: {ENV_PATH}")
    print(f"[DEBUG] Directorio actual: {os.getcwd()}")
    sys.exit(1)

secrets = dotenv_values(ENV_PATH)
load_dotenv(ENV_PATH)  # Carga en os.environ para que os.getenv() funcione

# Confirmación de variables de PG (Connexa)
print(f"[INFO] PGP_DB:{secrets.get('PGP_DB')} - PGP_HOST:{secrets.get('PGP_HOST')} - PGP_USER:{secrets.get('PGP_USER')}")


# =========================
# 2) CONEXIONES A BASES DE DATOS
# =========================
def get_pg_engine() -> Engine:
    """ Devuelve un engine de SQLAlchemy para PostgreSQL (Connexa). """
    host = os.getenv("PGP_HOST")
    port = os.getenv("PGP_PORT", "5432")
    db = os.getenv("PGP_DB")
    user = os.getenv("PGP_USER")
    pwd = os.getenv("PGP_PASSWORD")

    print(f"[INFO] Conectando a PostgreSQL: host={host}, port={port}, db={db}, user={user}")

    if not all([host, db, user, pwd, port]):
        raise RuntimeError("Faltan variables de entorno PGP_* para conectarse a PostgreSQL")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def get_sqlserver_engine() -> Engine:
    """
    Devuelve un engine para SQL Server (data-sync) usando pyodbc/ODBC Driver 17.
    Se utiliza un 'Connect Timeout' de 30 segundos.
    """
    host = os.getenv("SQL_SERVER")
    port = os.getenv("SQL_PORT", "1433")
    db = os.getenv("SQL_DATABASE", "data-sync")
    user = os.getenv("SQL_USER")
    pwd = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    if not all([host, db, user, pwd]):
        raise RuntimeError("Faltan variables de entorno SQL_SERVER, SQL_DATABASE, SQL_USER, o SQL_PASSWORD")

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


# =========================
# 3) LECTURA Y TRANSFORMACIÓN
# =========================
def obtener_transferencias_precarga(pg_engine: Engine) -> pd.DataFrame:
    """
    Obtiene todas las transferencias en estado PRECARGA_CONNEXA a nivel detalle.
    """
    sql = """
    SELECT
        d.id                          AS detail_id,
        h.id                          AS header_id,
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
    return pd.read_sql(sql, pg_engine, parse_dates=["requested_at", "created_at"])

def transformar_a_staging(df_src: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de Connexa al formato esperado
    por [data-sync].[repl].[TRANSF_CONNEXA_IN].

    Ajustes correctos:
      - item_code → entero
      - destination_store_code → entero
      - origin_cd '41CD' → 41
      - Cálculo de bultos y factor vectorizado
      - Fechas normalizadas
    """

    if df_src.empty:
        return pd.DataFrame(
            columns=[
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
            ]
        )

    df = df_src.copy()

    # -----------------------------
    # 1. Normalización CÓDIGOS
    # -----------------------------

    # Artículo → entero
    df["item_code_num"] = pd.to_numeric(df["item_code"], errors="coerce").fillna(0).astype(int)

    # Sucursal destino → entero
    df["dest_store_num"] = pd.to_numeric(df["destination_store_code"], errors="coerce").fillna(0).astype(int)

    # Sucursal origen: extraer números al inicio (41CD → 41)
    origin_str = df["origin_cd"].astype(str)
    df["origin_cd_num"] = (
        origin_str.str.extract(r"^(\d+)", expand=False)  # toma solo la parte numérica inicial
                 .fillna("0")
                 .astype(int)
    )

    # -----------------------------
    # 2. Cantidades (vectorizado)
    # -----------------------------
    df["units_per_package"] = df["units_per_package"].fillna(1.0)
    df.loc[df["units_per_package"] <= 0, "units_per_package"] = 1.0

    df["qty_planned"] = df["qty_planned"].fillna(0.0)

    df["q_factor"] = df["units_per_package"].astype(int)
    
    df["q_requerida"] = (df["units_per_package"] * df["qty_requested"]).astype(int)  # Los Paso a Unidades de Venta

    df["q_bultos"] = df["qty_requested"].astype(int)

    # -----------------------------
    # 3. Fecha de alta
    # -----------------------------
    now = datetime.now()
    df["f_alta"] = (
        df["requested_at"]
        .fillna(df["created_at"])
        .fillna(now)
    )

    # -----------------------------
    # 4. Campos fijos
    # -----------------------------
    df["m_alta_prioridad"] = "N"
    df["vchUsuario"] = "CONNEXA"
    df["vchTerminal"] = "API"
    df["forzarTransf"] = "N"
    df["estado"] = "PENDIENTE"
    df["mensaje_error"] = ""

    # -----------------------------
    # 5. Mapeo final
    # -----------------------------
    df_stg = pd.DataFrame({
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
    })

    return df_stg

# =========================
# 4) INSERCIÓN Y ACTUALIZACIÓN
# =========================
def insertar_en_staging_sqlserver(df_stg: pd.DataFrame, sql_engine: Engine) -> int:
    """
    Inserta las filas en [data-sync].[repl].[TRANSF_CONNEXA_IN] usando to_sql.
    """
    if df_stg.empty:
        return 0

    table_name = "TRANSF_CONNEXA_IN"
    schema_name = "repl"

    try:
        df_stg.to_sql(
            name=table_name,
            con=sql_engine,
            schema=schema_name,
            if_exists="append",
            index=False,
        )
        return len(df_stg)
    except Exception as e:
        print(f"[ERROR] Error al insertar en SQL Server: {e}")
        raise


def actualizar_estado_cabeceras(pg_engine: Engine, header_ids: list[int], nuevo_estado_id: int = 80) -> int:
    """
    Actualiza el estado de las cabeceras de transferencia en Connexa
    al estado SINCRONIZANDO (80). Devuelve cuántas filas fueron actualizadas.
    """
    if not header_ids:
        return 0

    sql = """
        UPDATE supply_planning.spl_distribution_transfer
        SET status_id = :nuevo_estado
        WHERE id = ANY(:lista_ids)
    """

    with pg_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {
                "nuevo_estado": nuevo_estado_id,
                "lista_ids": header_ids,
            },
        )

    return result.rowcount


# =========================
# 5) MAIN
# =========================
def main() -> int:
    try:
        pg_engine = get_pg_engine()
        sql_engine = get_sqlserver_engine()

        # 1. Leer transferencias desde Connexa (PostgreSQL)
        print("\n[INFO] Leyendo transferencias en estado PRECARGA_CONNEXA desde Connexa...")
        df_src = obtener_transferencias_precarga(pg_engine)
        print(f"[INFO] Registros origen (detalle): {len(df_src)}")

        if df_src.empty:
            print("[INFO] No hay transferencias en PRECARGA_CONNEXA para publicar.")
            return 0

        # Cabeceras únicas
        header_ids = sorted(df_src["header_id"].unique().tolist())
        print(f"[INFO] Cabeceras a actualizar después de publicar: {len(header_ids)}")

        # 2. Transformar datos
        print("\n[INFO] Transformando datos a formato TRANSF_CONNEXA_IN...")
        df_stg = transformar_a_staging(df_src)
        print(f"[INFO] Registros a insertar en staging: {len(df_stg)}")

        if df_stg.empty:
            print("[WARN] Después de la transformación no hay filas válidas para publicar.")
            return 0

        # 3. Insertar en SQL Server
        print("\n[INFO] Insertando en SQL Server [data-sync].[repl].[TRANSF_CONNEXA_IN]...")
        n = insertar_en_staging_sqlserver(df_stg, sql_engine)
        print(f"[INFO] Filas insertadas en TRANSF_CONNEXA_IN: {n}")

        if n == 0:
            print("[WARN] No se insertaron filas en TRANSF_CONNEXA_IN. No se actualizan estados.")
            return 0

        # 4. Actualizar estado en Connexa
        print("\n[INFO] Actualizando estado de cabeceras a SINCRONIZANDO (80)...")
        filas_actualizadas = actualizar_estado_cabeceras(pg_engine, header_ids, nuevo_estado_id=80)
        print(f"[INFO] Cabeceras actualizadas: {filas_actualizadas}")

        return 0

    except RuntimeError as e:
        print(f"\n[FATAL] Error de configuración de entorno: {e}")
        return 1
    except Exception as e:
        print(f"\n[FATAL] Ocurrió un error inesperado: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
