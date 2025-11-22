# obtener_base_productos_transito.py

import os
import sys
import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
from datetime import datetime
from prefect import flow, task, get_run_logger
import ast
from typing import Optional

# ====================== CONFIGURACIÓN Y LOGGING ======================
load_dotenv()

# Variables de entorno
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

SP_NAME = "[dbo].[SP_BASE_PRODUCTOS_EN_TRANSITO]"  # Manual EWE
TABLE_DESTINO = "src.base_productos_en_transito"

# Logging
logger = logging.getLogger("replicacion_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
os.makedirs("logs", exist_ok=True)
file_handler = logging.FileHandler("logs/replicacion_transito.log", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ====================== CONEXIONES ======================
sql_engine = create_engine(
    f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}"
    f"?driver=ODBC+Driver+17+for+SQL+Server"
)


def open_pg_conn():
    return pg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )


# ====================== ESQUEMA DEFINIDO MANUALMENTE ======================

ESQUEMA_BASE_PRODUCTOS_TRANSITO = {
    "f_alta_sist": "timestamp without time zone",
    "c_sucu_orig": "integer",
    "c_sucu_dest": "integer",
    "c_articulo": "integer",
    "n_articulo": "varchar(60)",
    "q_unid_peso_transf": "double precision",
    "q_unid_peso_recep": "double precision",
    "q_unid_transito": "double precision",
    "vnsucursalorig": "varchar(50)",
    "vnsucursaldest": "varchar(50)",
    "vpreciovtadestd": "double precision",
    "k_coef_iva": "numeric(4,3)",
    "q_factor_vta_sucu": "integer",
    "i_costo_estadistico": "double precision",
    "fuente_origen": "varchar(100)",
    "fecha_extraccion": "timestamp without time zone",
    "estado_sincronizacion": "integer",
}


def crear_sentencia_create(schema_dict: dict, table_name: str) -> str:
    columnas_sql = ", ".join([f'"{col}" {tipo}' for col, tipo in schema_dict.items()])
    return f"CREATE TABLE {table_name} ({columnas_sql})"


def insert_dataframe_postgres(df: pd.DataFrame, table_fullname: str):
    df = df.copy()
    df.columns = [col.lower() for col in df.columns]

    # Aseguramos que existan TODAS las columnas definidas en el esquema
    for col in ESQUEMA_BASE_PRODUCTOS_TRANSITO.keys():
        if col not in df.columns:
            df[col] = None

    # Nos quedamos solo con las columnas del esquema y en el orden del esquema
    df = df[list(ESQUEMA_BASE_PRODUCTOS_TRANSITO.keys())]

    # Conversión a tipos nativos para psycopg2 (None en lugar de NaN/<NA>)
    df = df.astype(object).where(pd.notnull(df), None)

    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_fullname} CASCADE")
            create_sql = crear_sentencia_create(
                ESQUEMA_BASE_PRODUCTOS_TRANSITO, table_fullname
            )
            cur.execute(create_sql)

            columnas = ", ".join([f'"{col}"' for col in df.columns])
            insert_sql = f"INSERT INTO {table_fullname} ({columnas}) VALUES %s"
            values = [tuple(row) for row in df.itertuples(index=False, name=None)]
            execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()


# ====================== TAREAS PREFECT ======================


@task(name="cargar_base_productos_transito_pg")
def cargar_base_productos_transito_pg():
    logger_prefect = get_run_logger()
    query = f"EXEC {SP_NAME}"
    logger_prefect.info("🟡 Iniciando lectura de SQL Server...")

    try:
        df = pd.read_sql(query, sql_engine)

        # Normalizamos nombres de columnas a minúsculas UNA sola vez aquí
        df.columns = [c.lower() for c in df.columns]

        logger_prefect.info(
            f"Columnas devueltas por el SP (normalizadas): {list(df.columns)}"
        )

        if df.empty:
            logger_prefect.warning("⚠️ No se recuperaron registros desde el SP")
            return df

        # Enriquecemos con metadatos internos
        df["fuente_origen"] = "SP_BASE_PRODUCTOS_EN_TRANSITO"
        df["fecha_extraccion"] = datetime.now()
        df["estado_sincronizacion"] = 0

        # --- Tipificación defensiva ---

        # Fecha
        if "f_alta_sist" in df.columns:
            df["f_alta_sist"] = pd.to_datetime(df["f_alta_sist"], errors="coerce")
        else:
            logger_prefect.warning(
                "Columna 'f_alta_sist' no devuelta por el SP; se completará como NULL en PG."
            )
            df["f_alta_sist"] = pd.NaT

        # Enteros
        for col in ["c_sucu_orig", "c_sucu_dest", "c_articulo", "q_factor_vta_sucu"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            else:
                logger_prefect.warning(
                    f"Columna '{col}' no devuelta por el SP; se completará como NULL en PG."
                )
                df[col] = pd.Series([pd.NA] * len(df), dtype="Int64")

        # Decimales / floats
        float_cols = [
            "q_unid_peso_transf",
            "q_unid_peso_recep",
            "q_unid_transito",
            "vpreciovtadestd",
            "k_coef_iva",
            "i_costo_estadistico",
        ]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
            else:
                logger_prefect.warning(
                    f"Columna '{col}' no devuelta por el SP; se completará como NULL en PG."
                )
                df[col] = pd.Series([pd.NA] * len(df), dtype="Float64")

        logger_prefect.info(f"✅ {len(df)} registros leídos desde SQL Server.")
    except Exception as e:
        logger_prefect.error(f"❌ Error durante la lectura: {e}")
        raise

    try:
        insert_dataframe_postgres(df, TABLE_DESTINO)
        logger_prefect.info(f"📦 Datos cargados en PostgreSQL: {TABLE_DESTINO}")
    except Exception as e:
        logger_prefect.error(f"❌ Error al insertar en PostgreSQL: {e}")
        raise

    return df


# --- Bloque de ajuste (placeholder) ---


AJUSTE_SQL = """
    SELECT 
        f_alta_sist, c_sucu_orig, c_sucu_dest, c_articulo, n_articulo,
        q_unid_peso_transf, q_unid_peso_recep, q_unid_transito,
        vnsucursalorig, vnsucursaldest, vpreciovtadestd, k_coef_iva,
        q_factor_vta_sucu, i_costo_estadistico, fuente_origen,
        fecha_extraccion, estado_sincronizacion
    FROM src.base_productos_en_transito
    WHERE f_alta_sist < (CURRENT_DATE - INTERVAL '30 days');
"""


@task(name="ajustar_productos_transito")
def ajustar_productos_transito():
    """
    Placeholder para futuros ajustes (limpieza de registros antiguos, etc.).
    Hoy solo contabiliza filas que cumplirían la condición.
    """
    log = get_run_logger()
    total_afectadas = 0
    try:
        with open_pg_conn() as conn, conn.cursor() as cur:
            cur.execute(AJUSTE_SQL)
            total_afectadas = cur.rowcount if cur.rowcount is not None else 0
        log.info(
            f"✅ Consulta de ajuste ejecutada. Filas que cumplen condición: {total_afectadas}."
        )
    except Exception as e:
        log.error(f"❌ Error ajustando transferencias por CD: {e}")
        raise
    return total_afectadas


# ====================== FLUJO PREFECT ======================


@flow(name="obtener_base_productos_transito")
def capturar_base_productos_transito(lista_ids: Optional[list] = None):
    log = get_run_logger()
    try:
        df_resultado = (
            cargar_base_productos_transito_pg.with_options(
                name="Carga Base Productos en Tránsito"
            )
            .submit()
            .result()
        )

        # Ajuste opcional (hoy desactivado o sólo diagnóstico)
        # filas_ajustadas = ajustar_productos_transito.with_options(
        #     name="Ajustar Tránsito Productos"
        # ).submit().result()
        filas_ajustadas = 0

        log.info(
            f"✅ Proceso completado: {len(df_resultado)} filas cargadas; "
            f"{filas_ajustadas} filas ajustadas por TRANSITO."
        )
    except Exception as e:
        log.error(f"🔥 Error general en el flujo: {e}")
        raise


# ====================== EJECUCIÓN MANUAL ======================

if __name__ == "__main__":
    args = sys.argv[1:]
    lista_ids = ast.literal_eval(args[0]) if args else None
    capturar_base_productos_transito(lista_ids=lista_ids)
    logger.info("🏁 Proceso Finalizado.")
