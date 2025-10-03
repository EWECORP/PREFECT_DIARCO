# S90_PUBLICAR_OC_PRECARGA.py
"""
Nombre del módulo: S90_PUBLICAR_OC_PRECARGA.py
Por la aquitectura del sistema esto se configura como un proceso PULL
De acuerdo al cronograma definido va a buscar las OK que estén en estado 90 para publicarlas en
SGM - TESTING / PRODUCCIÓN   SE MUEVE A --> ETL_DIARCO  (LOS CAMBIOS SE REALIZARÄN EN ETL_DIARCO)
Autor: EWE - Zeetrex
Fecha: 2025-05-11
Modificación: 22/07 -- Se agrega función de consolidación de Articulos y Proveedores que tengan Abastecimiento = 0
            - Entrega en el CD, generando sumatoria de artículos.
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
folder = f"{secrets['FOLDER_DATOS']}"
folder_logs = f"{secrets['FOLDER_LOG']}"
timestamp = time.strftime("%Y%m%d_%H%M%S")

## Corregir Warning PANDAS
# from sqlalchemy import create_engine
# engine = create_engine("postgresql+psycopg2://usuario:clave@host:puerto/base")
# df = pd.read_sql(query, engine)
# Para PostgreSQL
# pg_url = f"postgresql+psycopg2://{secrets['PG_USER']}:{secrets['PG_PASSWORD']}@{secrets['PG_HOST']}:{secrets['PG_PORT']}/{secrets['PG_DB']}"
# # engine_pg = create_engine(pg_url)
# df_oc   = pd.read_sql(query,   engine_pg)
# df_prod = pd.read_sql(queryp,  engine_pg)
# df_stock= pd.read_sql(querystock, engine_pg)

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

def limpiar_campos_oc(df):
    # --- Texto con longitudes destino ---
    df["c_usuario_genero_oc"]   = df["c_usuario_genero_oc"].fillna("").astype(str).str[:10]
    df["c_terminal_genero_oc"]  = df["c_terminal_genero_oc"].fillna("").astype(str).str[:15]
    df["c_usuario_bloqueo"]     = df["c_usuario_bloqueo"].fillna("").astype(str).str[:10]
    df["m_procesado"]           = df["m_procesado"].fillna("N").astype(str).str[:1]
    df["c_compra_kikker"]       = df["c_compra_kikker"].fillna("").astype(str).str[:20]
    df["c_usuario_modif"]       = df["c_usuario_modif"].fillna("").astype(str).str[:20]

    # --- Claves y numéricos EXACTOS como INT ---
    for col in ["c_proveedor", "c_articulo", "c_sucu_empr", "u_prefijo_oc", "u_sufijo_oc", "c_comprador"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Cantidad a publicar en bultos/kilos como INT (>=0)
    df["q_bultos_kilos_diarco"] = pd.to_numeric(df["q_bultos_kilos_diarco"], errors="coerce").fillna(0)
    df["q_bultos_kilos_diarco"] = df["q_bultos_kilos_diarco"].clip(lower=0).astype(int)

    # --- Timestamps ---
    df["f_alta_sist"]  = pd.to_datetime(df.get("f_alta_sist"),  errors='coerce')
    df["f_genero_oc"]  = pd.to_datetime(df.get("f_genero_oc"),  errors='coerce')
    df["f_procesado"]  = pd.to_datetime(df.get("f_procesado"),  errors='coerce')
    df["f_genero_oc"]  = df["f_genero_oc"].fillna(pd.Timestamp('1900-01-01 00:00:00'))
    df["f_procesado"]  = df["f_procesado"].fillna(pd.Timestamp('1900-01-01 00:00:00'))

    # --- Deduplicar intratable por PK destino ---
    df = df.drop_duplicates(subset=["c_proveedor", "c_articulo", "c_sucu_empr"], keep="last").reset_index(drop=True)
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


def validar_longitudes(df):
    campos_texto = [
        "c_usuario_genero_oc", "c_terminal_genero_oc", "c_usuario_bloqueo",
        "m_procesado", "c_compra_kikker", "c_usuario_modif"
    ]
    print("\n [INFO] Validando longitudes máximas por columna de texto:")
    for col in campos_texto:
        max_len = df[col].astype(str).map(len).max()
        print(f"{col}: longitud máxima = {max_len}")

# Función para consolidar OC Precarga
def consolidar_oc_precarga():
    logging.info("[INFO] Iniciando consolidación por abastecimiento")  
    conn_pg = None

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
        
        df_oc = forzar_enteros(df_oc)  # <<--- NUEVO

        # Convertir a enteros antes de armar la cláusula IN
        lista_proveedores = (
            pd.to_numeric(df_oc['c_proveedor'], errors='coerce')
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )

        # Formatea la lista para usarla en la consulta SQL
        in_clause = ', '.join([f"'{prov}'" for prov in lista_proveedores])

        # 1B. Traer productos vigentes de PostgreSQL
        queryp = f"""
        SELECT c_sucu_empr, c_articulo, c_proveedor_primario, abastecimiento, cod_cd
        FROM src.base_productos_vigentes
        WHERE c_proveedor_primario IN ({in_clause})
        """
        df_prod = pd.read_sql(queryp, conn_pg) # type: ignore

        if df_prod.empty:
            logging.warning("[WARNING] No hay productos vigentes para los proveedores seleccionados")
            return
        
        df_prod = forzar_enteros(df_prod)  # <<--- NUEVO FORZAR ENTEROS

        df_completo = df_oc.merge(
            df_prod,
            how='left',
            left_on=['c_sucu_empr', 'c_articulo', 'c_proveedor'],
            right_on=['c_sucu_empr', 'c_articulo', 'c_proveedor_primario']
        )
        
        df_completo = forzar_enteros(df_completo)  # <<--- NUEVO
        df_completo.drop(columns=['c_proveedor_primario'], inplace=True)

        # Filtrar por cod_cd = '41CD'
        df_41= df_completo[df_completo['cod_cd'] == '41CD']
        if df_41.empty:
            logging.warning("[WARNING] No hay registros de cod_cd '41CD' para publicar")
        else:
            df_grouped_41 = df_41.groupby(
            ['c_proveedor', 'c_articulo'],
            as_index=False
                ).agg({
                    'q_bultos_kilos_diarco': 'sum',
                    'f_alta_sist': 'first',
                    'c_usuario_genero_oc': 'first',
                    'c_terminal_genero_oc': 'first',    
                    'f_genero_oc': 'first',
                    'c_usuario_bloqueo': 'first',
                    'm_procesado': 'first',
                    'f_procesado': 'first',
                    'u_prefijo_oc': 'first',
                    'u_sufijo_oc': 'first',
                    'c_compra_kikker': 'first',
                    'c_usuario_modif': 'first',
                    'c_comprador': 'first'
                }).reset_index(drop=True)

            df_grouped_41['c_sucu_empr'] = 41
            df_grouped_41['m_publicado'] = False
            # Borrar en Origen
            # df_merged.drop(df_merged[df_merged['cod_cd'] == '41CD'].index, inplace=True)
            # Publicar en Destino
            # df_merged = pd.concat([df_merged, df_grouped_41], ignore_index=True)

        # Filtrar por cod_cd = '82CD'
        df_82= df_completo[df_completo['cod_cd'] == '82CD']
        if df_82.empty:
            logging.warning("[WARNING] No hay registros de cod_cd '82CD' para publicar")
        else:
            df_grouped_82 = df_82.groupby(
            ['c_proveedor', 'c_articulo'],
            as_index=False
                ).agg({
                    'q_bultos_kilos_diarco': 'sum',
                    'f_alta_sist': 'first',
                    'c_usuario_genero_oc': 'first',
                    'c_terminal_genero_oc': 'first',    
                    'f_genero_oc': 'first',
                    'c_usuario_bloqueo': 'first',
                    'm_procesado': 'first',
                    'f_procesado': 'first',
                    'u_prefijo_oc': 'first',
                    'u_sufijo_oc': 'first',
                    'c_compra_kikker': 'first',
                    'c_usuario_modif': 'first',
                    'c_comprador': 'first'
                }).reset_index(drop=True)

            df_grouped_82['c_sucu_empr'] = 82
            df_grouped_82['m_publicado'] = False
            # Borrar en Origen
            #  df_merged.drop(df_merged[df_merged['cod_cd'] == '82CD'].index, inplace=True)
            # Publicar en Destino
            df_merged = pd.concat([df_grouped_41, df_grouped_82], ignore_index=True)
            df_merged = forzar_enteros(df_merged)  # <<--- NUEVO

        
        # 1C. Traer Stock CENTROS DE DISTRIBUCIÓN
        querystock = f"""
            SELECT codigo_sucursal as c_sucu_empr, 
                codigo_articulo as c_articulo,
                P.q_factor_compra,
				COALESCE(stock, 0) as stock_origen,
                COALESCE(pedido_pendiente, 0) as pedido_pendiente, 
                COALESCE(transfer_pendiente, 0) as transfer_pendiente, 
                FLOOR((COALESCE(stock, 0) + COALESCE(pedido_pendiente, 0) + COALESCE(transfer_pendiente, 0)) 
				/ COALESCE(P.q_factor_compra,1 )) AS stock
            
            FROM src.base_stock_sucursal
            LEFT JOIN src.base_productos_vigentes P ON
                codigo_sucursal = c_sucu_empr AND codigo_articulo = c_articulo

            WHERE codigo_proveedor in ({in_clause}) 
            and codigo_sucursal IN(41, 82)
            """

        df_stock = pd.read_sql(querystock, conn_pg) # type: ignore
        df_stock = forzar_enteros(df_stock)  # <<--- NUEVO

        if df_stock.empty:
            logging.warning("[WARNING] No hay stock disponible para los proveedores seleccionados")
        else:
            df_stock.rename(columns={'c_sucu_empr': 'c_sucu_empr_stock', 'c_articulo': 'c_articulo_stock'}, inplace=True)
            df_stock.to_csv(
                os.path.join(folder_logs, f"{timestamp}_stock.csv"),
                index=False,
                encoding='utf-8-sig'
            )
            # Hacemos el merge con clave múltiple
            # Esto agrega el stock a df_merged
            df_merged = df_merged.merge(
                df_stock[['c_sucu_empr_stock', 'c_articulo_stock', 'stock']],
                how='left',
                left_on=['c_sucu_empr', 'c_articulo'],
                right_on=['c_sucu_empr_stock', 'c_articulo_stock']
            )
            df_merged.drop(columns=['c_sucu_empr_stock', 'c_articulo_stock'], inplace=True)

            # Guardar el DataFrame combinado en un archivo CSV
            
            os.makedirs(folder_logs, exist_ok=True)
            logging.info(f"[INFO] Guardando DataFrame combinado en {folder_logs}/{timestamp}_merged_stock.csv")
            logging.info(f"[INFO] DataFrame combinado tiene {df_merged.shape[0]} filas y {df_merged.shape[1]} columnas")
            
            df_completo.to_csv(
                os.path.join(folder_logs, f"{timestamp}_origen_completo.csv"),
                index=False,
                encoding='utf-8-sig'
            )
            
            df_merged.to_csv(
                os.path.join(folder_logs, f"{timestamp}_merged_stock.csv"),
                index=False,
                encoding='utf-8-sig'
            )
            # Restar a q_bultos_kilos_diarco stock y tranformar a entero
            df_merged['q_bultos_kilos_diarco'] = (
            df_merged['q_bultos_kilos_diarco'].fillna(0) - df_merged['stock'].fillna(0)
            ).clip(lower=0).astype(int)

            # Eliminar columnas campos stock
            df_merged.drop(columns=['abastecimiento', 'cod_cd', 'stock'], inplace=True)
            
            # Eliminar filas donde q_bultos_kilos_diarco sea menor o igual a 0
            df_merged = df_merged[df_merged['q_bultos_kilos_diarco'] > 0].reset_index(drop=True)
            df_merged.to_csv(
            os.path.join(folder_logs, f"{timestamp}_pedido_consolidado.csv"),
            index=False,
            encoding='utf-8-sig'
            )

        conn_pg.close()
        return df_merged 

    except Exception as e:
        logging.error("[ERROR] Error durante la CONSOLIDACIÓN de OC Precarga")
        logging.error(traceback.format_exc())
        print("[ERROR] Error durante la ejecución:", e)

# Función principal de publicación
def publicar_oc_precarga():
    logging.info("[INFO] Iniciando publicación de OC Precarga")
    df_oc = consolidar_oc_precarga()
    if df_oc is None or df_oc.empty:
        logging.warning("[WARNING] No hay registros consolidados para publicar")
        return
    
    df_oc = forzar_enteros(df_oc)  # <<--- NUEVO

    # Abrir conexión a PostgreSQL SOLO para el update
    conn_pg = Open_Diarco_Data()
    if conn_pg is None:
        raise ConnectionError("[ERROR] No se pudo reconectar a PostgreSQL para actualizar publicados")

    conn_sql = None
    cursor_sql = None

    try:
        
        if df_oc.empty:
            logging.warning("[WARNING] No hay registros pendientes de publicación")
            return

        # Agrupar por c_proveedor_primario y c_articulo, sumando las cantidades
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

        # # Obtener claves ya existentes en destino para los proveedores/sucursales involucrados
        # proveedores = tuple(sorted(df_oc['c_proveedor'].unique().tolist()))
        # sucursales  = tuple(sorted(df_oc['c_sucu_empr'].unique().tolist()))
        # qry_exist = f"""
        #     SELECT C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR
        #     FROM dbo.T080_OC_PRECARGA_KIKKER
        #     WHERE C_PROVEEDOR IN {proveedores} AND C_SUCU_EMPR IN {sucursales}
        # """
        # cursor_sql.execute(qry_exist)
        # existentes = {(r[0], r[1], r[2]) for r in cursor_sql.fetchall()}

        # # Anti-join en memoria
        # mask = ~df_oc.apply(lambda r: (r['c_proveedor'], r['c_articulo'], r['c_sucu_empr']) in existentes, axis=1)
        # df_insert = df_oc[mask].copy()

        # # Insertar sólo las nuevas (no explota la PK)


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
        lista_compra_kikker = df_oc['c_compra_kikker'].dropna().unique().tolist()
        placeholders = ', '.join(['%s'] * len(lista_compra_kikker))
        update_stmt = f"""
                UPDATE public.t080_oc_precarga_kikker
                SET m_publicado = true
                WHERE c_compra_kikker IN ({placeholders})
            """

        with conn_pg.cursor() as cursor_pg:
            cursor_pg.execute(update_stmt, lista_compra_kikker)
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