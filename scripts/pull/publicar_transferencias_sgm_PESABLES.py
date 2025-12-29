# scripts/pull/publicar_transferencias_sgm.py
#
# Publica transferencias desde CONNEXA (PostgreSQL) hacia staging en SQL Server DMZ:
#   [data-sync].[repl].[TRANSF_CONNEXA_IN]
#
# Reglas confirmadas:
# - qty_requested en Connexa YA está en BULTOS (formato esperado por SGM).
# - units_per_package = unidades por bulto (q_factor).
# - q_requerida = q_bultos * q_factor (derivado en unidades; auditoría).
# - origin_cd puede venir como "41CD" / "82CD" y debe grabarse como 41 / 82.
# - Luego de insertar en staging, se actualiza el status de la cabecera en Connexa a 80 (SINCRONIZANDO).
#
# Requisitos:
# - Variables .env para PG (PGP_*) y SQL Server (SQL_*)
# - Tabla destino ya creada:
#   repl.TRANSF_CONNEXA_IN con columnas incluidas UUID (uniqueidentifier)
#
# Notas:
# - Este script NO ejecuta el SP publicador SGM; solo carga staging + marca cabeceras en 80.
# - Para evitar duplicados por reintento, se recomienda crear un índice único filtrado por connexa_detail_uuid.

import os
import sys
import urllib.parse
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
from dotenv import dotenv_values, load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


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
print(
    f"[INFO] PGP_DB:{secrets.get('PGP_DB')} - PGP_HOST:{secrets.get('PGP_HOST')} - PGP_USER:{secrets.get('PGP_USER')}"
)

# =========================
# 2) CONEXIONES A BASES DE DATOS
# =========================
def get_pg_engine() -> Engine:
    """Devuelve un engine de SQLAlchemy para PostgreSQL (Connexa)."""
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
    Devuelve un engine para SQL Server (data-sync) usando pyodbc.
    Se utiliza 'Connect Timeout' de 30 segundos.
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
    Obtiene todas las transferencias en estado PRECARGA_CONNEXA (detalle),
    incluyendo UUIDs de cabecera/detalle.
    """
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
    return pd.read_sql(sql, pg_engine, parse_dates=["requested_at", "created_at"])


def transformar_a_staging(df_src: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de Connexa al formato esperado por:
      [data-sync].[repl].[TRANSF_CONNEXA_IN]

    Reglas:
      - qty_requested ya está en BULTOS => q_bultos = qty_requested (DECIMAL(13,3))
      - q_factor = units_per_package (unidades por bulto; entero DECIMAL(6,0))
      - q_requerida = q_bultos * q_factor (unidades; DECIMAL(13,3))
      - origin_cd '41CD' => c_sucu_orig = 41
      - destination_store_code => c_sucu_dest (decimal(3,0))
      - item_code => c_articulo (decimal(6,0))
      - estado inicial = PENDIENTE
    """
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

    # -----------------------------
    # 1) Normalización de códigos
    # -----------------------------
    # Artículo -> int (decimal(6,0))
    df["item_code_num"] = pd.to_numeric(df["item_code"], errors="coerce").fillna(0).astype(int)

    # Sucursal destino -> int (decimal(3,0))
    df["dest_store_num"] = pd.to_numeric(df["destination_store_code"], errors="coerce").fillna(0).astype(int)

    # UUIDs (guardar como string canónico para pyodbc -> uniqueidentifier)
    df["connexa_header_uuid"] = df["connexa_header_uuid"].astype(str).str.lower()
    df["connexa_detail_uuid"] = df["connexa_detail_uuid"].astype(str).str.lower()

    # Sucursal origen: extraer números al inicio (41CD → 41)
    origin_str = df["origin_cd"].astype(str)
    df["origin_cd_num"] = (
        origin_str.str.extract(r"^(\d+)", expand=False)  # toma solo la parte numérica inicial
        .fillna("0")
        .astype(int)
    )

    # -----------------------------
    # 2) Cantidades (qty_requested = BULTOS)
    # -----------------------------
    df["units_per_package"] = pd.to_numeric(df["units_per_package"], errors="coerce").fillna(1.0)
    df.loc[df["units_per_package"] <= 0, "units_per_package"] = 1.0

    df["qty_planned"] = df["qty_planned"].fillna(0.0)

      # factor PESABLE con Decimales (unidades por bulto)
    df["q_factor"] = df["units_per_package"].round(3)

    # unidades requeridas (derivado)
    df["qty_requested"] = pd.to_numeric(df["qty_requested"], errors="coerce").fillna(0.0)
    
    df["q_requerida"] = (df["units_per_package"] * df["qty_requested"]).astype(int)  # Los Paso a Unidades de Venta
    
    # bultos (puede ser decimal(13,3))
    df["q_bultos"] = df["qty_requested"].round(3)

    # -----------------------------
    # 3) Fecha de alta
    # -----------------------------
    now = datetime.now()
    df["f_alta"] = df["requested_at"].fillna(df["created_at"]).fillna(now)

    # -----------------------------
    # 4) Campos fijos
    # -----------------------------
    df["m_alta_prioridad"] = "N"
    df["vchUsuario"] = "CONNEXA"
    df["vchTerminal"] = "API"
    df["forzarTransf"] = "N"
    df["estado"] = "PENDIENTE"
    df["mensaje_error"] = ""

    # -----------------------------
    # 5) Mapeo final
    # -----------------------------
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
            "connexa_header_uuid": df["f"],
            "connexa_detail_uuid": df["connexa_detail_uuid"],
        }
    )

    # Limpieza defensiva: descartar filas "inválidas" que romperían SGM
    # (si prefieren mantenerlas y que el SP las marque error, comenten este filtro)
    # df_stg = df_stg[
    #     (df_stg["c_articulo"] > 0)
    #     & (df_stg["c_sucu_dest"] > 0)
    #     & (df_stg["c_sucu_orig"] > 0)
    #     & (df_stg["q_bultos"] >= 0)
    #     & (df_stg["q_factor"] > 0)
    # ].copy()

    return df_stg


# =========================
# 4) INSERCIÓN Y ACTUALIZACIÓN
# =========================
def insertar_en_staging_sqlserver(df_stg: pd.DataFrame, sql_engine: Engine) -> int:
    """Inserta filas en [data-sync].[repl].[TRANSF_CONNEXA_IN] usando to_sql."""
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
        return len(df_stg)
    except Exception as e:
        print(f"[ERROR] Error al insertar en SQL Server: {e}")
        raise


def actualizar_estado_cabeceras(pg_engine: Engine, header_uuids: List[str], nuevo_estado_id: int = 80) -> int:
    """
    Actualiza el estado de las cabeceras de transferencia en Connexa a SINCRONIZANDO (80).
    header_uuids: lista de UUID (strings).
    """
    if not header_uuids:
        return 0

    sql = """
        UPDATE supply_planning.spl_distribution_transfer
           SET status_id = :nuevo_estado,
               updated_at = NOW()
         WHERE id = ANY(:lista_ids)
    """

    with pg_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {"nuevo_estado": nuevo_estado_id, "lista_ids": header_uuids},
        )

    return result.rowcount


# =========================
# 5) MAIN
# =========================
def main() -> int:
    try:
        pg_engine = get_pg_engine()
        sql_engine = get_sqlserver_engine()

        # 1) Leer transferencias desde Connexa
        print("\n[INFO] Leyendo transferencias en estado PRECARGA_CONNEXA desde Connexa...")
        df_src = obtener_transferencias_precarga(pg_engine)
        print(f"[INFO] Registros origen (detalle): {len(df_src)}")

        if df_src.empty:
            print("[INFO] No hay transferencias en PRECARGA_CONNEXA para publicar.")
            return 0

        # 2) Cabeceras UUID únicas
        header_uuids = sorted(df_src["connexa_header_uuid"].astype(str).str.lower().unique().tolist())
        print(f"[INFO] Cabeceras a actualizar a SINCRONIZANDO (80): {len(header_uuids)}")

        # 3) Transformar
        print("\n[INFO] Transformando datos a formato TRANSF_CONNEXA_IN...")
        df_stg = transformar_a_staging(df_src)
        print(f"[INFO] Registros a insertar en staging: {len(df_stg)}")

        if df_stg.empty:
            print("[WARN] Después de la transformación no hay filas válidas para insertar.")
            return 0

        # 4) Insertar staging SQL Server
        print("\n[INFO] Insertando en SQL Server [data-sync].[repl].[TRANSF_CONNEXA_IN]...")
        n = insertar_en_staging_sqlserver(df_stg, sql_engine)
        print(f"[INFO] Filas insertadas en TRANSF_CONNEXA_IN: {n}")

        if n == 0:
            print("[WARN] No se insertaron filas. No se actualizan estados en Connexa.")
            return 0

        # 5) Actualizar estado cabeceras en Connexa
        print("\n[INFO] Actualizando estado de cabeceras a SINCRONIZANDO (80)...")
        filas_actualizadas = actualizar_estado_cabeceras(pg_engine, header_uuids, nuevo_estado_id=80)
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
