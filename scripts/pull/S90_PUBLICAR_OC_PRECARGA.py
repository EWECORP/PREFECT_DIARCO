"""
Nombre del módulo: S90_PUBLICAR_OC_PRECARGA.py
Por la aquitectura del sistema esto se configura como un proceso PULL
De acuerdo al cronograma definido va a buscar las OK que estén en estado 90 para publicarlas en
SGM - TESTING / PRODUCCIÓN   SE MUEVE A --> ETL_DIARCO  (LOS CAMBIOS SE REALIZARÄN EN ETL_DIARCO)
Autor: EWE - Zeetrex
Fecha: 2025-05-11
"""

import pandas as pd
import pyodbc
import psycopg2 as pg2

# Cargar configuración DINAMICA de acuerdo al entorno
from dotenv import dotenv_values
import os
import sys
import time
import logging
import traceback

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

print(f"[INFO] Python ejecutado: {sys.executable}")

ENV_PATH = os.environ.get("ETL_ENV_PATH", "E:/ETL/ETL_DIARCO/.env")  # Toma Producción si está definido, o la ruta por defecto E:\ETL\ETL_DIARCO\.env
# Verificar si el archivo .env existe
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    print(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)
    
secrets = dotenv_values(ENV_PATH)
folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"
folder_logs = f"{secrets['BASE_DIR']}/{secrets['FOLDER_LOG']}"

# Funciones Locales
def Open_Connection():
    conn_str = f'DRIVER={secrets["SQLP_DRIVER"]};SERVER={secrets["SQLP_SERVER"]};PORT={secrets["SQLP_PORT"]};DATABASE={secrets["SQLP_DATABASE"]};UID={secrets["SQLP_USER"]};PWD={secrets["SQLP_PASSWORD"]}'
    # print (conn_str) 
    try:    
        conn = pyodbc.connect(conn_str)
        return conn
    except:
        print('Error en la Conexión')
        return None

def Open_Diarco_Data(): 
    conn_str = f"dbname={secrets['PG_DB']} user={secrets['PG_USER']} password={secrets['PG_PASSWORD']} host={secrets['PG_HOST']} port={secrets['PG_PORT']}"
    #print (conn_str)
    for i in range(5):
        try:    
            conn = pg2.connect(conn_str)
            return conn
        except Exception as e:
            print(f'Error en la conexión: {e}')
            time.sleep(5)
    return None  # Retorna None si todos los intentos fallan

def Close_Connection(conn): 
    if conn is not None:
        conn.close()
        # print("[OK] Conexión cerrada.")    
    return True

os.makedirs(folder_logs, exist_ok=True)
log_file = os.path.join(folder_logs, "publicacion_oc_precarga.log")

#
# Configurar logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Función para limpiar y normalizar los campos
def limpiar_campos_oc(df):
    # Normalizar textos respetando la longitud máxima del destino
    df["c_usuario_genero_oc"]   = df["c_usuario_genero_oc"].fillna("").astype(str).str[:10]
    df["c_terminal_genero_oc"]  = df["c_terminal_genero_oc"].fillna("").astype(str).str[:15]
    df["c_usuario_bloqueo"]     = df["c_usuario_bloqueo"].fillna("").astype(str).str[:10]
    df["m_procesado"]           = df["m_procesado"].fillna("N").astype(str).str[:1]
    df["c_compra_kikker"]       = df["c_compra_kikker"].fillna("").astype(str).str[:20]
    df["c_usuario_modif"]       = df["c_usuario_modif"].fillna("").astype(str).str[:20]

    # Números exactos
    df["u_prefijo_oc"] = pd.to_numeric(df["u_prefijo_oc"], errors="coerce").fillna(0).astype(int)
    df["u_sufijo_oc"]  = pd.to_numeric(df["u_sufijo_oc"], errors="coerce").fillna(0).astype(int)
    df["c_comprador"]  = pd.to_numeric(df["c_comprador"], errors="coerce").fillna(0).astype(int)

    # Timestamps (permitimos NaT)
    df["f_genero_oc"] = df["f_genero_oc"].fillna(pd.Timestamp('1900-01-01 00:00:00.000'))
    df["f_procesado"] = df["f_procesado"].fillna(pd.Timestamp('1900-01-01 00:00:00.000'))
    #df["f_genero_oc"] = pd.to_datetime(df["f_genero_oc"], errors='coerce')
    #df["f_procesado"] = pd.to_datetime(df["f_procesado"], errors='coerce')

    return df

def validar_longitudes(df):
    campos_texto = [
        "c_usuario_genero_oc", "c_terminal_genero_oc", "c_usuario_bloqueo",
        "m_procesado", "c_compra_kikker", "c_usuario_modif"
    ]
    print("\n [INFO] Validando longitudes máximas por columna de texto:")
    for col in campos_texto:
        max_len = df[col].astype(str).map(len).max()
        print(f"{col}: longitud máxima = {max_len}")


# Función principal de publicación
def publicar_oc_precarga():
    logging.info("[INFO] Iniciando publicación de OC Precarga")
    
    conn_pg = None
    conn_sql = None
    cursor_sql = None

    try:
        # 1. Conexión a PostgreSQL
        conn_pg = Open_Diarco_Data()
        if conn_pg is None:
            raise ConnectionError("[ERROR] No se pudo conectar a PostgreSQL")

        query = """
        SELECT *
        FROM public.t080_oc_precarga_kikker
        WHERE m_publicado = false
        """
        df_oc = pd.read_sql(query, conn_pg) # type: ignore

        if df_oc.empty:
            logging.warning("[WARNING] No hay registros pendientes de publicación")
            return

        total_rows = len(df_oc)
        logging.info(f"[INFO] Registros a publicar: {total_rows}")

        df_oc = limpiar_campos_oc(df_oc)
        validar_longitudes(df_oc)
        print(df_oc.head(5))

        # 2. Conexión a SQL Server
        conn_sql = Open_Connection()
        if conn_sql is None:
            raise ConnectionError("[ERROR] No se pudo conectar a SQL Server")

        cursor_sql = conn_sql.cursor()
        cursor_sql.fast_executemany = True  # validar si es soportado por tu driver

        insert_stmt = """
        INSERT INTO [dbo].[T080_OC_PRECARGA_KIKKER] (
            [C_PROVEEDOR], [C_ARTICULO], [C_SUCU_EMPR], [Q_BULTOS_KILOS_DIARCO],
            [F_ALTA_SIST], [C_USUARIO_GENERO_OC], [C_TERMINAL_GENERO_OC], [F_GENERO_OC],
            [C_USUARIO_BLOQUEO], [M_PROCESADO], [F_PROCESADO], [U_PREFIJO_OC],
            [U_SUFIJO_OC], [C_COMPRA_KIKKER], [C_USUARIO_MODIF], [C_COMPRADOR]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data_tuples = df_oc[[
            'c_proveedor', 'c_articulo', 'c_sucu_empr', 'q_bultos_kilos_diarco',
            'f_alta_sist', 'c_usuario_genero_oc', 'c_terminal_genero_oc', 'f_genero_oc',
            'c_usuario_bloqueo', 'm_procesado', 'f_procesado', 'u_prefijo_oc',
            'u_sufijo_oc', 'c_compra_kikker', 'c_usuario_modif', 'c_comprador'
        ]].itertuples(index=False, name=None)

        cursor_sql.executemany(insert_stmt, list(data_tuples))
        conn_sql.commit()

        logging.info("[INFO] Inserción completada en SQL Server")
        print(f"✔ Se insertaron {total_rows} registros en SQL Server.")

        # 3. Marcar como publicados en PostgreSQL
        with conn_pg.cursor() as cursor_pg:
            update_stmt = """
            UPDATE public.t080_oc_precarga_kikker
            SET m_publicado = true
            WHERE m_publicado = false
            """
            cursor_pg.execute(update_stmt)
            rows_updated = cursor_pg.rowcount
            conn_pg.commit()

        logging.info(f"[INFO] {rows_updated} registros marcados como publicados")
        print(f"✔ {rows_updated} registros actualizados con m_publicado = true")

    except Exception as e:
        logging.error("[ERROR] Error durante la publicación de OC Precarga")
        logging.error(traceback.format_exc())
        print("[ERROR] Error durante la ejecución:", e)

    finally:
        if cursor_sql:
            try:
                cursor_sql.close()
            except Exception as e:
                logging.warning(f"[WARNING] Error al cerrar cursor SQL: {e}")
        if conn_sql:
            try:
                conn_sql.close()
            except Exception as e:
                logging.warning(f"[WARNING] Error al cerrar conexión SQL Server: {e}")
        if conn_pg:
            try:
                conn_pg.close()
            except Exception as e:
                logging.warning(f"[WARNING] Error al cerrar conexión PostgreSQL: {e}")


if __name__ == "__main__":
    publicar_oc_precarga()
    print(f"[INFO] Proceso finalizado. Ver log en: {log_file}")