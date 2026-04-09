#cdc_worker_sql.py
import json
import logging
import pyodbc
from config import DB_CONFIG, TABLES, LSN_STORE

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def connect():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)

def load_last_lsn():
    try:
        with open(LSN_STORE, "r") as f:
            return json.load(f).get("last_lsn")
    except:
        return None

def save_last_lsn(lsn):
    with open(LSN_STORE, "w") as f:
        json.dump({"last_lsn": lsn}, f)

def get_max_lsn(cursor):
    cursor.execute("SELECT sys.fn_cdc_get_max_lsn()")
    return cursor.fetchone()[0]

def process_change(row):
    logging.info(f"Operacion={row.__getattribute__('__$operation')} | Datos={row}")

def run_worker():
    conn = connect()
    cursor = conn.cursor()

    last_lsn = load_last_lsn()
    logging.info(f"Último LSN procesado: {last_lsn}")

    for schema, table in TABLES:
        capture_instance = f"{schema}_{table}"

        max_lsn = get_max_lsn(cursor)

        if last_lsn is None:
            cursor.execute(f"""
                SELECT sys.fn_cdc_get_min_lsn('{capture_instance}')
            """)
            last_lsn = cursor.fetchone()[0] # type: ignore

        sql = f"""
            SELECT *
            FROM cdc.fn_cdc_get_all_changes_{capture_instance}(
                ?, ?, 'all'
            )
        """

        cursor.execute(sql, last_lsn, max_lsn)

        for row in cursor.fetchall():
            process_change(row)
            last_lsn = row.__getattribute__("__$start_lsn")

        save_last_lsn(last_lsn)

    logging.info("Ciclo completado.")
