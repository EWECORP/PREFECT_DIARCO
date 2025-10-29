"""
Nombre del módulo: S90_PUBLICAR_OC_PRECARGA.py
Por la aquitectura del sistema esto se configura como un proceso PULL
De acuerdo al cronograma definido va a buscar las OK que estén en estado 90 para publicarlas en
SGM - TESTING / PRODUCCIÓN   SE MUEVE A --> ETL_DIARCO  (LOS CAMBIOS SE REALIZARÄN EN ETL_DIARCO)
Autor: EWE - Zeetrex
Fecha: 2025-10-29
Descripción:
    Este script extrae órdenes de compra (OC) pendientes de publicación desde una base de datos
    PostgreSQL y las inserta en una base de datos SQL Server. Después de la inserción, actualiza el estado
    de las OC en PostgreSQL para marcar que han sido publicas 
    NUEVO ESQUEMA MICRO SERVICIOS - CONNEXA
"""

import pandas as pd
import pyodbc
import psycopg2 as pg2
from sqlalchemy import create_engine

# Cargar configuración DINAMICA de acuerdo al entorno
from dotenv import dotenv_values
import os
import sys
import time
import logging
import traceback

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='pandas')

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

# -----------------------------------
print(f"PREPARANDO LOGS : Directorio actual: {os.getcwd()}")
print(f"[INFO] Cargando configuración desde: {ENV_PATH}")
print(f"[INFO] Carpeta de datos: {folder}") 
os.makedirs(folder_logs, exist_ok=True)
log_file = os.path.join(folder_logs, "publicacion_oc_precarga.log")

# Configurar logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Función para limpiar y normalizar los campos
def limpiar_campos_oc(df: pd.DataFrame) -> pd.DataFrame:
    # --- Texto con longitudes destino ---
    df["c_usuario_genero_oc"]  = df["c_usuario_genero_oc"].fillna("").astype(str).str[:10]
    df["c_terminal_genero_oc"] = df["c_terminal_genero_oc"].fillna("").astype(str).str[:15]
    df["c_usuario_bloqueo"]    = df["c_usuario_bloqueo"].fillna("").astype(str).str[:10]
    df["m_procesado"]          = df["m_procesado"].fillna("N").astype(str).str[:1]
    df["c_compra_connexa"]      = df["c_compra_connexa"].fillna("").astype(str).str[:20]
    df["c_usuario_modif"]      = df["c_usuario_modif"].fillna("").astype(str).str[:20]

    # --- Claves y numéricos EXACTOS como INT ---
    for col in ["c_proveedor", "c_articulo", "c_sucu_empr", "u_prefijo_oc", "u_sufijo_oc", "c_comprador"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Cantidad a publicar en bultos/kilos como INT (>=0)
    if "q_bultos_kilos_diarco" in df.columns:
        df["q_bultos_kilos_diarco"] = pd.to_numeric(df["q_bultos_kilos_diarco"], errors="coerce").fillna(0)
        df["q_bultos_kilos_diarco"] = df["q_bultos_kilos_diarco"].clip(lower=0).astype(int)

    # --- Timestamps ---
    for dcol in ["f_alta_sist", "f_genero_oc", "f_procesado"]:
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df.get(dcol), errors='coerce') # type: ignore
    if "f_genero_oc" in df.columns:
        df["f_genero_oc"] = df["f_genero_oc"].fillna(pd.Timestamp('1900-01-01 00:00:00'))
    if "f_procesado" in df.columns:
        df["f_procesado"] = df["f_procesado"].fillna(pd.Timestamp('1900-01-01 00:00:00'))

    # --- Deduplicar intratable por PK destino ---
    pk_cols = ["c_proveedor", "c_articulo", "c_sucu_empr"]
    pk_cols = [c for c in pk_cols if c in df.columns]
    if pk_cols:
        df = df.drop_duplicates(subset=pk_cols, keep="last").reset_index(drop=True)

    return df

def forzar_enteros(df: pd.DataFrame) -> pd.DataFrame:
    # Columnas clave y numéricos exactos
    int_cols = [
        "c_proveedor", "c_articulo", "c_sucu_empr",
        "u_prefijo_oc", "u_sufijo_oc", "c_comprador",
        "q_bultos_kilos_diarco", "c_proveedor_primario"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    # Booleanos
    if "m_publicado" in df.columns:
        df["m_publicado"] = df["m_publicado"].fillna(False).astype(bool)

    # Fechas (por si se degradaron)
    for dcol in ["f_alta_sist", "f_genero_oc", "f_procesado"]:
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")

    return df

def validar_longitudes(df: pd.DataFrame):
    campos_texto = [
        "c_usuario_genero_oc", "c_terminal_genero_oc", "c_usuario_bloqueo",
        "m_procesado", "c_compra_connexa", "c_usuario_modif"
    ]
    print("\n [INFO] Validando longitudes máximas por columna de texto:")
    for col in campos_texto:
        if col in df.columns:
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
        FROM public.t080_oc_precarga_connexa
        WHERE m_publicado = false
        """
        df_oc = pd.read_sql(query, conn_pg) # type: ignore

        if df_oc.empty:
            logging.warning("[WARNING] No hay registros pendientes de publicación")
            return

        df_oc = forzar_enteros(df_oc)
        
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

        # --- ANTI-JOIN con destino: evita 2627 por reintentos/concurrencia ---
        proveedores = tuple(sorted(df_oc['c_proveedor'].astype(int).unique().tolist()))
        sucursales  = tuple(sorted(df_oc['c_sucu_empr'].astype(int).unique().tolist()))

        def _fmt_in(t):
            # Formato correcto de IN para 1 o N elementos
            return f"({t[0]})" if len(t) == 1 else str(t)

        qry_exist = f"""
            SELECT C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR
            FROM dbo.T080_OC_PRECARGA_KIKKER
            WHERE C_PROVEEDOR IN {_fmt_in(proveedores)} AND C_SUCU_EMPR IN {_fmt_in(sucursales)}
        """
        cursor_sql.execute(qry_exist)
        existentes = {(int(r[0]), int(r[1]), int(r[2])) for r in cursor_sql.fetchall()}

        def _key_tuple(r):
            return (int(r['c_proveedor']), int(r['c_articulo']), int(r['c_sucu_empr']))

        mask = ~df_oc.apply(_key_tuple, axis=1).isin(existentes)
        df_insert = df_oc[mask].copy()

        total_rows_ins = len(df_insert)
        logging.info(f"[INFO] Lote total: {total_rows} | A insertar: {total_rows_ins} | Omitidas por existir: {total_rows - total_rows_ins}")

        if total_rows_ins > 0:
            insert_stmt = """
            INSERT INTO [dbo].[T080_OC_PRECARGA_KIKKER] (
                [C_PROVEEDOR], [C_ARTICULO], [C_SUCU_EMPR], [Q_BULTOS_KILOS_DIARCO],
                [F_ALTA_SIST], [C_USUARIO_GENERO_OC], [C_TERMINAL_GENERO_OC], [F_GENERO_OC],
                [C_USUARIO_BLOQUEO], [M_PROCESADO], [F_PROCESADO], [U_PREFIJO_OC],
                [U_SUFIJO_OC], [C_COMPRA_KIKKER], [C_USUARIO_MODIF], [C_COMPRADOR]
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            data_tuples = df_insert[[
                'c_proveedor', 'c_articulo', 'c_sucu_empr', 'q_bultos_kilos_diarco',
                'f_alta_sist', 'c_usuario_genero_oc', 'c_terminal_genero_oc', 'f_genero_oc',
                'c_usuario_bloqueo', 'm_procesado', 'f_procesado', 'u_prefijo_oc',
                'u_sufijo_oc', 'c_compra_connexa', 'c_usuario_modif', 'c_comprador'
            ]].itertuples(index=False, name=None)

            cursor_sql.executemany(insert_stmt, list(data_tuples))
            conn_sql.commit()
            logging.info(f"[INFO] Inserción completada en SQL Server: {total_rows_ins} filas nuevas")
            print(f"✔ Se insertaron {total_rows_ins} registros nuevos en SQL Server.")
        else:
            logging.info("[INFO] No hay filas nuevas para insertar; se continúa con el update de m_publicado en PG.")

        # 3. Marcar como publicados en PostgreSQL (por lote c_compra_connexa)
        lista_compra_kikker = df_oc['c_compra_connexa'].dropna().astype(str).unique().tolist()
        if lista_compra_kikker:
            placeholders = ', '.join(['%s'] * len(lista_compra_kikker))
            update_stmt = f"""
                UPDATE public.t080_oc_precarga_connexa
                SET m_publicado = true
                WHERE c_compra_connexa IN ({placeholders})
            """
            with conn_pg.cursor() as cursor_pg:
                cursor_pg.execute(update_stmt, lista_compra_kikker)
                rows_updated = cursor_pg.rowcount
                conn_pg.commit()

            logging.info(f"[INFO] {rows_updated} registros marcados como publicados en PG")
            print(f"✔ {rows_updated} registros actualizados con m_publicado = true")
        else:
            logging.warning("[WARNING] No se encontraron valores de c_compra_connexa para actualizar en PG")

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