# utils/postgres.py
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect, text
from prefect import get_run_logger

PG_CONN_STR = "postgresql+psycopg2://postgres:aladelta10$@186.158.182.54:5432/diarco_data"

PG_RAW_CONN = {
    "host": "186.158.182.54",
    "port": "5432",
    "dbname": "diarco_data",
    "user": "postgres",
    "password": "aladelta10$"
}

def validar_o_crear_tabla(schema: str, tabla: str, archivo_csv: str):
    logger = get_run_logger()
    engine = create_engine(PG_CONN_STR)
    df_csv = pd.read_csv(archivo_csv, delimiter='|', dtype=str, nrows=100)  # Sample

    with engine.connect() as conn:
        inspector = inspect(engine)
        if inspector.has_table(tabla, schema=schema):
            columnas_pg = [col['name'] for col in inspector.get_columns(tabla, schema=schema)]
            columnas_csv = df_csv.columns.tolist()

            if columnas_pg != columnas_csv:
                logger.warning(f"Estructura incompatible: eliminando tabla {schema}.{tabla}")
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tabla}" CASCADE'))
                df_csv.iloc[0:0].to_sql(tabla, engine, schema=schema, index=False)
            else:
                logger.info("Estructura de tabla válida, no se requiere recreación.")
        else:
            logger.info(f"Creando tabla {schema}.{tabla}")
            df_csv.iloc[0:0].to_sql(tabla, engine, schema=schema, index=False)

def cargar_datos_csv(schema: str, tabla: str, archivo_csv: str):
    logger = get_run_logger()
    total_lineas = sum(1 for _ in open(archivo_csv, encoding='utf-8')) - 1  # -1 por el header

    with psycopg2.connect(**PG_RAW_CONN) as conn:  # type: ignore
        with conn.cursor() as cur:
            with open(archivo_csv, "r", encoding='utf-8') as f:
                next(f)  # Saltar encabezado
                cur.copy_expert(
                    sql=f"""
                        COPY "{schema}"."{tabla}" 
                        FROM STDIN 
                        WITH CSV 
                        DELIMITER '|' 
                        NULL 'NULL' 
                        QUOTE '"' 
                        ESCAPE '"' 
                    """,
                    file=f
                )
        conn.commit()
    logger.info(f"✅ CSV cargado ({total_lineas} filas) en {schema}.{tabla}")
