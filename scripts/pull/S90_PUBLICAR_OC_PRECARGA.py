# S90_PUBLICAR_OC_PRECARGA.py
"""
Nombre del módulo: S90_PUBLICAR_OC_PRECARGA.py
Por la aquitectura del sistema esto se configura como un proceso PULL
De acuerdo al cronograma definido va a buscar las OK que estén en estado 90 para publicarlas en
SGM - TESTING / PRODUCCIÓN   SE MUEVE A --> ETL_DIARCO  (LOS CAMBIOS SE REALIZARÁN EN ETL_DIARCO)
Autor: EWE - Zeetrex
Fecha: 2025-05-11
Modificación: 22/07 -- Se agrega función de consolidación de Artículos y Proveedores que tengan Abastecimiento = 0
            - Entrega en el CD, generando sumatoria de artículos.

Correcciones (2025-10-02):
- Construcción segura de df_merged usando 'partes' (41CD/82CD) para evitar variables no definidas.
- Unicidad intra-lote por PK destino y anti-join con tabla destino para evitar PK 2627.
- Guardado opcional de CSVs de diagnóstico.
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

# Funciones Locales
def Open_Connection():
    conn_str = f'DRIVER={secrets["SQLP_DRIVER"]};SERVER={secrets["SQLP_SERVER"]};PORT={secrets["SQLP_PORT"]};DATABASE={secrets["SQLP_DATABASE"]};UID={secrets["SQLP_USER"]};PWD={secrets["SQLP_PASSWORD"]}'
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except:
        print('Error en la Conexión')
        return None

def Open_Diarco_Data():
    conn_str = f"dbname={secrets['PG_DB']} user={secrets['PG_USER']} password={secrets['PG_PASSWORD']} host={secrets['PG_HOST']} port={secrets['PG_PORT']}"
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

def limpiar_campos_oc(df: pd.DataFrame) -> pd.DataFrame:
    # --- Texto con longitudes destino ---
    df["c_usuario_genero_oc"]  = df["c_usuario_genero_oc"].fillna("").astype(str).str[:10]
    df["c_terminal_genero_oc"] = df["c_terminal_genero_oc"].fillna("").astype(str).str[:15]
    df["c_usuario_bloqueo"]    = df["c_usuario_bloqueo"].fillna("").astype(str).str[:10]
    df["m_procesado"]          = df["m_procesado"].fillna("N").astype(str).str[:1]
    df["c_compra_kikker"]      = df["c_compra_kikker"].fillna("").astype(str).str[:20]
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
        "m_procesado", "c_compra_kikker", "c_usuario_modif"
    ]
    print("\n [INFO] Validando longitudes máximas por columna de texto:")
    for col in campos_texto:
        if col in df.columns:
            max_len = df[col].astype(str).map(len).max()
            print(f"{col}: longitud máxima = {max_len}")


# BLOQUE BIANCULLI EN PROCESO DE CONSOLIDACIÓN
# -----------------------------------------------
# from logging.handlers import RotatingFileHandler
# import platform

# import psycopg2
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

    # load_dotenv()  # Esto busca un archivo .env en el mismo directorio

    # # variables de conexion
    # pass_connexa = os.getenv('CONNEXA_PLATFORM_POSTGRESQL_PASS_CONNEXA')
    # user_connexa = os.getenv('CONNEXA_PLATFORM_POSTGRESQL_USER_CONNEXA')
    # db_connexa = os.getenv('CONNEXA_PLATFORM_POSTGRESQL_DB_CONNEXA')
    # host_connexa = os.getenv('CONNEXA_PLATFORM_POSTGRESQL_HOST_CONNEXA')

    # pass_diarco = os.getenv('CONNEXA_PLATFORM_POSTGRESQL_PASS_DIARCO')
    # user_diarco = os.getenv('CONNEXA_PLATFORM_POSTGRESQL_USER_DIARCO')
    # db_diarco = os.getenv('CONNEXA_PLATFORM_POSTGRESQL_DB_DIARCO')
    # host_diarco = os.getenv('CONNEXA_PLATFORM_POSTGRESQL_HOST_DIARCO')

    # db_origen = f"dbname='{db_connexa}' user='{user_connexa}' password='{pass_connexa}' host='{host_connexa}'"
    # db_destino = f"dbname='{db_diarco}' user='{user_diarco}' password='{pass_diarco}' host='{host_diarco}'"

    # conn_origen = psycopg2.connect(db_origen)
    # cursor_origen = conn_origen.cursor()

    # conn_destino = psycopg2.connect(db_destino)
    # cursor_destino = conn_destino.cursor()

    # query para recuperar los proposal en estado 'aprobado'
    qry = """
        select 	p.id proposal_id,
                p.supply_forecast_execution_execute_id execute_id,
                s.ext_code::numeric as c_proveedor,
                product_code::numeric as c_articulo,
                site_code::numeric as c_sucu_empr,
                proposal_number as c_compra_kikker,
                quantity_purchase q_bultos_kilos_diarco,
                b.ext_code::numeric as c_comprador,
                case when coalesce(ps.proposed_quantity,0) != coalesce(ps.quantity_confirmed,0) then  b.ext_user_login 
                else null end as c_usuario_modif,
                now() as f_alta_sist
        from spl_supply_purchase_proposal p
        left join fnd_supplier s on s.id = p.supplier_id
        left join spl_supply_purchase_proposal_supplier_product_site ps 
                                on ps.supply_purchase_proposal_id = p.id
        left join fnd_site si on si.id = site_id
        left join prc_buyer b on b.user_id = p.user_id
        where status = 'aprobado'
          and quantity_purchase != 0
    """

    # genero las tuplas
    cursor_origen.execute(qry)
    resultado = cursor_origen.fetchall()
    logger.info(f"Total de lineas recuperadas: {len(resultado)}")

    # recupero los id proposal que tiene estado 'aprobado'
    # estos id se usaran para actualizar el estado de la proposal
    id_proposals = set(row[0] for row in resultado)
    logger.info(f"Total de propuestas: {len(id_proposals)}")

    # recupero los id execution execute procesados 
    # estos id se usaran despues para cambiar el estado a 90
    id_executes = set(row[1] for row in resultado)


    # Convertir a DataFrame
    column_names = [desc[0] for desc in cursor_origen.description]
    df_origen = pd.DataFrame(resultado, columns=column_names)    
    # Eliminar columnas
    df_origen.drop(columns=['proposal_id'], inplace=True)
    df_origen.drop(columns=['execute_id'], inplace=True)
    # Volver a tuplas
    tuplas_limpias = list(df_origen.itertuples(index=False, name=None))


    # insert masivo en la tabla de Kikker
    qry_insert = """
        insert into T080_OC_PRECARGA_KIKKER (c_proveedor, c_articulo, c_sucu_empr, c_compra_kikker, 
                                            q_bultos_kilos_diarco, c_comprador, c_usuario_modif, f_alta_sist)
        values %s
    """
    execute_values(cursor_destino, qry_insert, tuplas_limpias)
    conn_destino.commit() 


    #
    # actualizar el estado en spl_supply_purchase_proposal
    #

    # se recuperan los id de los proposals procesados
    ids = list(id_proposals)
    cantidad = len(ids)
    if cantidad != 0:
        logger.info('Id spl_supply_purchase_proposal:')
        logger.info(ids)


    # se actualiza la tabla proposal
    upd_proposal = f"""
        UPDATE spl_supply_purchase_proposal
        SET status = 'finalizado'
        WHERE id = ANY(%s::uuid[])
    """
    cursor_origen.execute(upd_proposal, (ids,))
    filas_afectadas = cursor_origen.rowcount  # Número de filas actualizadas

    if filas_afectadas > 0:
        print(f"Se actualizaron a estado 'finalizado' {filas_afectadas} registros en la spl_supply_purchase_proposal.")
    else:
        print("No se actualizó a estado 'finalizado' ningún registro en la spl_supply_purchase_proposal.")
    
    conn_origen.commit() 


    #
    # actualizar el estado en spl_supply_forecast_execution_execute
    #

    # se recuperan los id de los execution execute
    idsx = list(id_executes)
    cantidad = len(idsx)
    if cantidad != 0:
        logger.info('Id spl_supply_forecast_execution_execute:')
        logger.info(idsx)

    # se actualiza la tabla spl_supply_forecast_execution_execute
    upd_execute = f"""
        UPDATE spl_supply_forecast_execution_execute
        SET supply_forecast_execution_status_id = 90, last_execution=false
        WHERE id = ANY(%s::uuid[])
    """
    cursor_origen.execute(upd_execute, (idsx,))

    filas_afectadas = cursor_origen.rowcount  # Número de filas actualizadas

    if filas_afectadas > 0:
        print(f"Se actualizaron {filas_afectadas} registros en la tabla spl_supply_forecast_execution_execute.")
    else:
        print("No se actualizó ningún registro en la tabla spl_supply_forecast_execution_execute.")

    conn_origen.commit() 

# --------------------------------------------------



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
        df_oc = pd.read_sql(query, conn_pg)  # type: ignore

        if df_oc.empty:
            logging.warning("[WARNING] No hay registros pendientes de publicación")
            conn_pg.close()
            return pd.DataFrame()

        df_oc = forzar_enteros(df_oc)

        # Convertir a enteros antes de IN
        lista_proveedores = (
            pd.to_numeric(df_oc['c_proveedor'], errors='coerce')
            .dropna().astype(int).unique().tolist()
        )
        if not lista_proveedores:
            logging.warning("[WARNING] No hay proveedores válidos para procesar")
            conn_pg.close()
            return pd.DataFrame()

        in_clause = ', '.join([f"'{prov}'" for prov in lista_proveedores])

        # 1B. Productos vigentes
        queryp = f"""
        SELECT c_sucu_empr, c_articulo, c_proveedor_primario, abastecimiento, cod_cd
        FROM src.base_productos_vigentes
        WHERE c_proveedor_primario IN ({in_clause})
        """
        df_prod = pd.read_sql(queryp, conn_pg)  # type: ignore
        if df_prod.empty:
            logging.warning("[WARNING] No hay productos vigentes para los proveedores seleccionados")
            conn_pg.close()
            return pd.DataFrame()

        df_prod = forzar_enteros(df_prod)

        # Merge completo
        df_completo = df_oc.merge(
            df_prod,
            how='left',
            left_on=['c_sucu_empr', 'c_articulo', 'c_proveedor'],
            right_on=['c_sucu_empr', 'c_articulo', 'c_proveedor_primario']
        )
        df_completo = forzar_enteros(df_completo)
        if 'c_proveedor_primario' in df_completo.columns:
            df_completo.drop(columns=['c_proveedor_primario'], inplace=True)

        # -------------------------------
        # PARTES A PUBLICAR
        # -------------------------------
        partes = []

        # 41CD (consolidado a sucursal 41)
        df_41 = df_completo[df_completo['cod_cd'] == '41CD']
        if not df_41.empty:
            df_grouped_41 = df_41.groupby(['c_proveedor', 'c_articulo'], as_index=False).agg({
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
            partes.append(df_grouped_41)
            logging.info(f"[INFO] Registros consolidados para 41CD: {len(df_grouped_41)}")
        else:
            logging.warning("[WARNING] No hay registros de cod_cd '41CD' para publicar")

        # 82CD (consolidado a sucursal 82)
        df_82 = df_completo[df_completo['cod_cd'] == '82CD']
        if not df_82.empty:
            df_grouped_82 = df_82.groupby(['c_proveedor', 'c_articulo'], as_index=False).agg({
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
            partes.append(df_grouped_82)
            logging.info(f"[INFO] Registros consolidados para 82CD: {len(df_grouped_82)}")
        else:
            logging.warning("[WARNING] No hay registros de cod_cd '82CD' para publicar")

        # PASSTHROUGH (entregas directas: no consolidan)
        # Tomamos todo lo que NO sea 41CD/82CD (incluye NULL en cod_cd) y lo dejamos tal cual.
        mask_passthrough = (~df_completo['cod_cd'].isin(['41CD', '82CD'])) | (df_completo['cod_cd'].isna())
        df_passthrough = df_completo.loc[mask_passthrough].copy()

        if not df_passthrough.empty:
            # Homogeneizar columnas a las de las partes consolidadas
            cols_out = [
                'c_proveedor', 'c_articulo', 'c_sucu_empr', 'q_bultos_kilos_diarco',
                'f_alta_sist', 'c_usuario_genero_oc', 'c_terminal_genero_oc', 'f_genero_oc',
                'c_usuario_bloqueo', 'm_procesado', 'f_procesado', 'u_prefijo_oc',
                'u_sufijo_oc', 'c_compra_kikker', 'c_usuario_modif', 'c_comprador'
            ]
            # Asegurar existencia de columnas
            for c in cols_out:
                if c not in df_passthrough.columns:
                    df_passthrough[c] = pd.NA

            df_passthrough = df_passthrough[cols_out].reset_index(drop=True)
            df_passthrough['m_publicado'] = False
            df_passthrough = forzar_enteros(df_passthrough)
            partes.append(df_passthrough)
            logging.info(f"[INFO] Registros de entrega directa (passthrough): {len(df_passthrough)}")
        else:
            logging.info("[INFO] No hay registros de entrega directa (passthrough) para publicar")

        # Si no hay nada que publicar, salir
        if not partes:
            logging.warning("[WARNING] No hay registros con cod_cd {41CD,82CD} ni entregas directas para publicar")
            conn_pg.close()
            return pd.DataFrame()

        # -----------------------------------
        # CONCATENACIÓN EXPLÍCITA (grouped + passthrough)
        # -----------------------------------
        frames = []

        # Aseguramos variables y tamaños (0 si no existen o están vacías)
        n_g41 = len(df_grouped_41) if 'df_grouped_41' in locals() and isinstance(df_grouped_41, pd.DataFrame) and not df_grouped_41.empty else 0
        n_g82 = len(df_grouped_82) if 'df_grouped_82' in locals() and isinstance(df_grouped_82, pd.DataFrame) and not df_grouped_82.empty else 0
        n_dir = len(df_passthrough) if 'df_passthrough' in locals() and isinstance(df_passthrough, pd.DataFrame) and not df_passthrough.empty else 0

        if n_g41 > 0:
            frames.append(df_grouped_41)
        if n_g82 > 0:
            frames.append(df_grouped_82)
        if n_dir > 0:
            frames.append(df_passthrough)

        if not frames:
            logging.warning("[WARNING] No hay partes para concatenar (41CD agrupado, 82CD agrupado ni directos)")
            conn_pg.close()
            return pd.DataFrame()

        # Concatenar uno debajo del otro en orden: 41 → 82 → directos
        df_merged = pd.concat(frames, axis=0, ignore_index=True)

        logging.info(
            f"[INFO] Total registros concatenados: {len(df_merged)} "
            f"(41CD_agrupados={n_g41}, 82CD_agrupados={n_g82}, Directos={n_dir})"
        )
    
        # Normalización final de tipos enteros/fechas/booleanos
        df_merged = forzar_enteros(df_merged)
        
        # Paso 1: Agrupar y sumar la cantidad
        df_sumado = df_merged.groupby(['c_proveedor', 'c_articulo', 'c_sucu_empr'], as_index=False)['q_bultos_kilos_diarco'].sum()

        # Paso 2: Eliminar duplicados del original (conservando la primera ocurrencia)
        df_sin_duplicados = df_merged.drop_duplicates(subset=['c_proveedor', 'c_articulo', 'c_sucu_empr'], keep='first')

        # Paso 3: Eliminar la columna original de cantidad para evitar conflicto
        df_sin_duplicados = df_sin_duplicados.drop(columns=['q_bultos_kilos_diarco'])

        # Paso 4: Reincorporar la cantidad sumada
        df_final = df_sin_duplicados.merge(df_sumado, on=['c_proveedor', 'c_articulo', 'c_sucu_empr'], how='left')  

        # 1C. Traer Stock CENTROS DE DISTRIBUCIÓN (solo afecta 41/82)
        querystock = f"""
            SELECT 
                codigo_sucursal AS c_sucu_empr, 
                codigo_articulo AS c_articulo,
                P.q_factor_compra,
                COALESCE(stock, 0) AS stock_origen,
                COALESCE(pedido_pendiente, 0) AS pedido_pendiente, 
                COALESCE(transfer_pendiente, 0) AS transfer_pendiente, 
                FLOOR(
                    (COALESCE(stock, 0) + COALESCE(pedido_pendiente, 0) + COALESCE(transfer_pendiente, 0)) 
                    / COALESCE(P.q_factor_compra,1 )
                ) AS stock
            FROM src.base_stock_sucursal
            LEFT JOIN src.base_productos_vigentes P
                ON codigo_sucursal = c_sucu_empr 
               AND codigo_articulo  = c_articulo
            WHERE codigo_proveedor IN ({in_clause}) 
              AND codigo_sucursal IN (41, 82)
        """
        df_stock = pd.read_sql(querystock, conn_pg)  # type: ignore
        df_stock = forzar_enteros(df_stock)

        # Guardado de diagnósticos
        try:
            df_completo.to_csv(os.path.join(folder_logs, f"{timestamp}_origen_completo.csv"),
                               index=False, encoding='utf-8-sig')
        except Exception:
            pass

        if not df_stock.empty:
            df_stock.rename(columns={'c_sucu_empr': 'c_sucu_empr_stock', 'c_articulo': 'c_articulo_stock'}, inplace=True)
            try:
                df_stock.to_csv(os.path.join(folder_logs, f"{timestamp}_stock.csv"),
                                index=False, encoding='utf-8-sig')
            except Exception:
                pass

            # Merge con stock por clave (sucursal, articulo)
            df_final = df_final.merge(
                df_stock[['c_sucu_empr_stock', 'c_articulo_stock', 'stock']],
                how='left',
                left_on=['c_sucu_empr', 'c_articulo'],
                right_on=['c_sucu_empr_stock', 'c_articulo_stock']
            )
            df_final.drop(columns=['c_sucu_empr_stock', 'c_articulo_stock'], inplace=True)

            # Ajuste de cantidad por stock (solo afectará a 41/82; directas no matchean y quedan sin descuento)
            if 'stock' in df_final.columns:
                df_final['q_bultos_kilos_diarco'] = (
                    df_final['q_bultos_kilos_diarco'].fillna(0) - df_final['stock'].fillna(0)
                ).clip(lower=0).astype(int)

        # Limpieza de columnas si existen
        for col in ['abastecimiento', 'cod_cd', 'stock']:
            if col in df_final.columns:
                df_final.drop(columns=[col], inplace=True)

        # Filtrar cantidades > 0
        if 'q_bultos_kilos_diarco' in df_final.columns:
            df_final = df_final[df_final['q_bultos_kilos_diarco'] > 0].reset_index(drop=True)

        # Unicidad intra-lote por PK destino
        pk_cols = ['c_proveedor', 'c_articulo', 'c_sucu_empr']
        total = len(df_final)
        unicas = len(df_final.drop_duplicates(subset=pk_cols))
        if total != unicas:
            logging.warning(f"[WARN] Duplicados intra-batch detectados: {total - unicas}")
            try:
                df_final[df_final.duplicated(subset=pk_cols, keep=False)].to_csv(
                    os.path.join(folder_logs, f"{timestamp}_duplicados_intra_batch.csv"),
                    index=False, encoding='utf-8-sig'
                )
            except Exception:
                pass
            df_final = df_final.drop_duplicates(subset=pk_cols, keep='last').reset_index(drop=True)

        # Definir el orden deseado de columnas
        orden_columnas = [
            'c_proveedor', 'c_articulo', 'c_sucu_empr', 'q_bultos_kilos_diarco',
            'f_alta_sist', 'c_usuario_genero_oc', 'c_terminal_genero_oc', 'f_genero_oc',
            'c_usuario_bloqueo', 'm_procesado', 'f_procesado',
            'u_prefijo_oc', 'u_sufijo_oc', 'c_compra_kikker', 'c_usuario_modif',
            'c_comprador'
        ]
        # Reordenar el DataFrame
        df_final = df_final[orden_columnas]

        # Guardar pedido consolidado
        try:
            df_final.to_csv(os.path.join(folder_logs, f"{timestamp}_pedido_consolidado.csv"),
                             index=False, encoding='utf-8-sig')
        except Exception:
            pass

        conn_pg.close()
        return df_final

    except Exception as e:
        logging.error("[ERROR] Error durante la CONSOLIDACIÓN de OC Precarga")
        logging.error(traceback.format_exc())
        print("[ERROR] Error durante la ejecución:", e)
        if conn_pg:
            try:
                conn_pg.close()
            except Exception:
                pass
        return pd.DataFrame()


# Función principal de publicación
def publicar_oc_precarga():
    logging.info("[INFO] Iniciando publicación de OC Precarga")
    df_oc = consolidar_oc_precarga()
    if df_oc is None or df_oc.empty:
        logging.warning("[WARNING] No hay registros consolidados para publicar")
        return

    df_oc = forzar_enteros(df_oc)

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

        # Diagnóstico de tamaños
        total_rows = len(df_oc)
        logging.info(f"[INFO] Registros a publicar (lote consolidado): {total_rows}")

        df_oc = limpiar_campos_oc(df_oc)
        validar_longitudes(df_oc)
        print(df_oc.head(5))

        # 2. Conexión a SQL Server
        conn_sql = Open_Connection()
        if conn_sql is None:
            raise ConnectionError("[ERROR] No se pudo conectar a SQL Server")

        cursor_sql = conn_sql.cursor()
        cursor_sql.fast_executemany = True  # si el driver lo soporta, acelera el batch

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
                'u_sufijo_oc', 'c_compra_kikker', 'c_usuario_modif', 'c_comprador'
            ]].itertuples(index=False, name=None)

            cursor_sql.executemany(insert_stmt, list(data_tuples))
            conn_sql.commit()
            logging.info(f"[INFO] Inserción completada en SQL Server: {total_rows_ins} filas nuevas")
            print(f"✔ Se insertaron {total_rows_ins} registros nuevos en SQL Server.")
        else:
            logging.info("[INFO] No hay filas nuevas para insertar; se continúa con el update de m_publicado en PG.")

        # 3. Marcar como publicados en PostgreSQL (por lote c_compra_kikker)
        lista_compra_kikker = df_oc['c_compra_kikker'].dropna().astype(str).unique().tolist()
        if lista_compra_kikker:
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

            logging.info(f"[INFO] {rows_updated} registros marcados como publicados en PG")
            print(f"✔ {rows_updated} registros actualizados con m_publicado = true")
        else:
            logging.warning("[WARNING] No se encontraron valores de c_compra_kikker para actualizar en PG")

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
