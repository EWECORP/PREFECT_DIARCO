# utils/postgres.py
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect, text
from prefect import get_run_logger
import csv
import tempfile

PG_CONN_STR = "postgresql+psycopg2://postgres:aladelta10$@186.158.182.54:5432/diarco_data"

PG_RAW_CONN = {
    "host": "186.158.182.54",
    "port": "5432",
    "dbname": "diarco_data",
    "user": "postgres",
    "password": "aladelta10$"
}

def get_pg_column_types(schema: str, table: str) -> dict:
    engine = create_engine(PG_CONN_STR)
    insp = inspect(engine)
    cols = insp.get_columns(table, schema=schema)
    # {'col': 'integer', ...}
    return {c['name'].lower(): str(c['type']).lower() for c in cols}

def normalizar_booleanos_en_csv(csv_path: str, columnas_bool: list[str]):
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(tmp_fd)
    with open(csv_path, 'r', encoding='utf-8', newline='') as src, \
        open(tmp_path, 'w', encoding='utf-8', newline='') as dst:
        reader = csv.reader(src, delimiter='|', quotechar='"', escapechar='"')
        writer = csv.writer(dst, delimiter='|', quotechar='"', escapechar='"', quoting=csv.QUOTE_MINIMAL)
        header = next(reader)
        header_lower = [h.lower() for h in header]
        idx = [header_lower.index(col) for col in columnas_bool if col in header_lower]
        writer.writerow(header)
        for row in reader:
            for i in idx:
                v = row[i]
                if v in ('', 'NULL'):
                    # respetar NULL tal como está
                    continue
                lv = v.strip().lower()
                if lv in ('true','t','1','sí','si','y','yes'):
                    row[i] = '1'
                elif lv in ('false','f','0','no','n'):
                    row[i] = '0'
                else:
                    # valor inesperado -> dejarlo tal cual para detectar luego
                    row[i] = v
            writer.writerow(row)
    os.replace(tmp_path, csv_path)

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
