from prefect import flow, task, get_run_logger
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
from datetime import datetime
import os
import sys
# ====================== LOGGING ======================
logger = logging.getLogger("replicacion_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
os.makedirs("logs", exist_ok=True)
file_handler = logging.FileHandler("logs/replicacion_psycopg2.log", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ====================== VARIABLES ======================
load_dotenv()
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# ====================== CONEXIONES ======================

# === CONFIGURACIÓN ===
SQLSERVER_CONN_STR = f"DRIVER=ODBC+Driver+17+for+SQL+Server;SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD}"
PG_CONN_STR = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/diarco_data"

# === TASKS ===

@task
def obtener_lsn_desde_postgres(tabla: str) -> bytes:
    engine = create_engine(PG_CONN_STR)
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT valor FROM control_replicacion WHERE tabla = :tabla"),
            {"tabla": tabla}
        ).fetchone()
        return result[0] if result else None # type: ignore

@task
def obtener_max_lsn_desde_sqlserver() -> bytes:
    with pyodbc.connect(SQLSERVER_CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT sys.fn_cdc_get_max_lsn()")
        return cursor.fetchone()[0] # type: ignore

@task
def extraer_cambios_cdc(from_lsn: bytes, to_lsn: bytes) -> pd.DataFrame:
    query = """
    SELECT * FROM cdc.fn_cdc_get_all_changes_repl_T050_ARTICULOS(?, ?, 'all')
    """
    with pyodbc.connect(SQLSERVER_CONN_STR) as conn:
        df = pd.read_sql(query, conn, params=[from_lsn, to_lsn]) # type: ignore
    return df

@task
def aplicar_cambios_postgres(df: pd.DataFrame):
    if df.empty:
        return "Sin cambios"

    # Filtramos operaciones insert y update (op 2 y 4)
    df_filtrado = df[df["__$operation"].isin([2, 4])]
    df_filtrado = df_filtrado.drop(columns=["__$start_lsn", "__$end_lsn", "__$operation", "__$seqval"])

    engine = create_engine(PG_CONN_STR)
    df_filtrado.to_sql("T050_ARTICULOS", engine, schema="repl", if_exists="append", index=False, method="multi")
    return f"{len(df_filtrado)} registros insertados/actualizados"

@task
def guardar_lsn_postgres(tabla: str, lsn: bytes):
    engine = create_engine(PG_CONN_STR)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO control_replicacion (tabla, valor, fecha)
            VALUES (:tabla, :valor, now())
            ON CONFLICT (tabla)
            DO UPDATE SET valor = EXCLUDED.valor, fecha = EXCLUDED.fecha
        """), {"tabla": tabla, "valor": lsn})

# === FLOW ===

@flow(name="cdc_replicar_T050_ARTICULOS")
def replicar_tabla_cdc():
    tabla = "T050_ARTICULOS"
    from_lsn = obtener_lsn_desde_postgres(tabla)
    to_lsn = obtener_max_lsn_desde_sqlserver()

    if from_lsn == to_lsn:
        print("⏳ No hay cambios nuevos.")
        return

    df = extraer_cambios_cdc(from_lsn, to_lsn)
    aplicar_cambios_postgres(df)
    guardar_lsn_postgres(tabla, to_lsn)
    print(f"✅ LSN actualizado para {tabla}.")

