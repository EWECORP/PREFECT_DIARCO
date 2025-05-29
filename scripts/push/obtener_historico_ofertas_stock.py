# obtener_historico_ofertas_stock.py
# Script para replicar datos de ventas de STOCK y OFERTAS desde SQL Server a PostgreSQL usando psycopg2 y Prefect.

import os
import sys
import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values
import logging
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import date, datetime, timedelta

# Configurar logging
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

# Cargar variables de entorno
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

# Crear engine SQL Server
def open_sql_conn():
    print(f"Conectando a SQL Server: {SQL_SERVER}")
    print(f"Conectando a SQL Server: {SQL_DATABASE}") 
    return create_engine(f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")

def open_pg_conn(): # type: ignore
    return pg2.connect(dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT)

def infer_postgres_types(df):
    type_map = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "object": "TEXT"
    }
    col_defs = [f"{col} {type_map.get(str(df[col].dtype), 'TEXT')}" for col in df.columns]
    return ", ".join(col_defs)

# Función para verificar si la fecha es válida
def es_fecha_valida(anio, mes, dia):
    try:
        return date(anio, mes, dia)
    except ValueError:
        return None
    
def open_pg_conn():
    # Retorna una conexión a PostgreSQL
    import psycopg2
    return psycopg2.connect(
        host="localhost",  # Cambia según tu configuración
        database="your_database",  # Cambia según tu base de datos
        user="your_user",  # Cambia según tu usuario
        password="your_password"  # Cambia según tu contraseña
    )

def upload_to_postgres(df_combined, table_name="src.historico_ofertas_stock", chunk_size=50000):
    # Conectar a la base de datos
    with open_pg_conn() as conn:
        cur = conn.cursor()

        # Crear la tabla si no existe
        columns = ', '.join(df_combined.columns)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f"{col} {infer_postgres_type(df_combined[col])}" for col in df_combined.columns])}
        );
        """
        cur.execute(create_sql)

        # Insertar los datos en la tabla en bloques (chunks)
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"
        total_rows = 0

        for i, chunk in enumerate(range(0, len(df_combined), chunk_size)):
            values_chunk = [tuple(row) for row in df_combined.iloc[chunk:chunk+chunk_size].itertuples(index=False, name=None)]
            execute_values(cur, insert_sql, values_chunk)
            conn.commit()
            total_rows += len(values_chunk)
            print(f"Insertados {len(values_chunk)} registros en el bloque {i+1}")

        # Cerrar cursor
        cur.close()

def infer_postgres_type(series):
    """
    Inferir el tipo de datos PostgreSQL para cada columna del DataFrame
    """
    dtype_map = {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        'object': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP'
    }
    return dtype_map.get(str(series.dtype), 'TEXT')

# FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
@task(name="cargar_historico_ofertas_stock_pg")
def cargar_ofertas_stock(lista_ids):
    ids = ','.join(map(str, lista_ids))
    print(f"-> Generando datos de STOCK para ID: {ids}")

    # Cargar STOCK por Proveedor
    """Consulta el stock y devuelve un dict {fecha: cantidad}, limitado a fechas válidas hasta ayer."""
    conn = open_sql_conn()
    query_stock = f"""
        SELECT  
            S.[C_ANIO],S.[C_MES],S.[C_SUCU_EMPR],S.[C_ARTICULO], A.[C_PROVEEDOR_PRIMARIO]
            ,S.[Q_DIA1],S.[Q_DIA2],S.[Q_DIA3],S.[Q_DIA4],S.[Q_DIA5],S.[Q_DIA6],S.[Q_DIA7],S.[Q_DIA8],S.[Q_DIA9],S.[Q_DIA10]
            ,S.[Q_DIA11],S.[Q_DIA12],S.[Q_DIA13],S.[Q_DIA14],S.[Q_DIA15],S.[Q_DIA16],S.[Q_DIA17],S.[Q_DIA18],S.[Q_DIA19],S.[Q_DIA20]
            ,S.[Q_DIA21],S.[Q_DIA22],S.[Q_DIA23],S.[Q_DIA24],S.[Q_DIA25],S.[Q_DIA26],S.[Q_DIA27],S.[Q_DIA28],S.[Q_DIA29],S.[Q_DIA30],S.[Q_DIA31]  
        FROM [repl].[T710_ESTADIS_STOCK] S
        LEFT JOIN [repl].[T050_ARTICULOS] A  
            ON S.C_ARTICULO = A.C_ARTICULO  
        WHERE C_ANIO = 2025
        AND A.C_PROVEEDOR_PRIMARIO IN ({ids});
    """
    df_stock = pd.read_sql(query_stock, conn) # type: ignore
    conn.dispose()  # Cerrar conexión SQL Server

    if df_stock.empty:
        print(f"⚠️ No se encontraron datos de stock para el proveedor {id_proveedor} en el mes actual.") # type: ignore
        # return {}
    df_stock['C_ANIO'] = df_stock['C_ANIO'].astype(int)
    df_stock['C_MES'] = df_stock['C_MES'].astype(int)
    df_stock['C_ARTICULO'] = df_stock['C_ARTICULO'].astype(int)
    df_stock['C_SUCU_EMPR'] = df_stock['C_SUCU_EMPR'].astype(int)
    df_stock['C_PROVEEDOR_PRIMARIO'] = df_stock['C_PROVEEDOR_PRIMARIO'].astype(int)
    
    # Cargar OFERTAS por Proveedor
    """Consulta el OFERTAS y devuelve un dict {fecha: flag}, limitado a fechas válidas hasta ayer."""
    conn = open_sql_conn()
    query_oferta = f"""
        SELECT  
            O.C_ANIO, O.C_MES, O.C_SUCU_EMPR, O.C_ARTICULO, A.C_PROVEEDOR_PRIMARIO,
            O.M_OFERTA_DIA1, O.M_OFERTA_DIA2, O.M_OFERTA_DIA3, O.M_OFERTA_DIA4, O.M_OFERTA_DIA5, 
            O.M_OFERTA_DIA6, O.M_OFERTA_DIA7, O.M_OFERTA_DIA8, O.M_OFERTA_DIA9, O.M_OFERTA_DIA10,
            O.M_OFERTA_DIA11, O.M_OFERTA_DIA12, O.M_OFERTA_DIA13, O.M_OFERTA_DIA14, O.M_OFERTA_DIA15, 
            O.M_OFERTA_DIA16, O.M_OFERTA_DIA17, O.M_OFERTA_DIA18, O.M_OFERTA_DIA19, O.M_OFERTA_DIA20,
            O.M_OFERTA_DIA21, O.M_OFERTA_DIA22, O.M_OFERTA_DIA23, O.M_OFERTA_DIA24, O.M_OFERTA_DIA25, 
            O.M_OFERTA_DIA26, O.M_OFERTA_DIA27, O.M_OFERTA_DIA28, O.M_OFERTA_DIA29, O.M_OFERTA_DIA30,
            O.M_OFERTA_DIA31
        FROM [repl].[T710_ESTADIS_OFERTA_FOLDER] O
        LEFT JOIN [repl].[T050_ARTICULOS] A ON O.C_ARTICULO = A.C_ARTICULO
        WHERE C_ANIO = 2025
        AND A.C_PROVEEDOR_PRIMARIO IN ({ids});
    """
    df_oferta = pd.read_sql(query_oferta, conn) # type: ignore
    conn.dispose()  # Cerrar conexión SQL Server
    if df_stock.empty:
        print(f"⚠️ No se encontraron datos de stock para el proveedor {id_proveedor} en el mes actual.") # type: ignore
        # return {}
    df_oferta['C_ANIO'] = df_oferta['C_ANIO'].astype(int)
    df_oferta['C_MES'] = df_oferta['C_MES'].astype(int)
    df_oferta['C_ARTICULO'] = df_oferta['C_ARTICULO'].astype(int)
    df_oferta['C_SUCU_EMPR'] = df_oferta['C_SUCU_EMPR'].astype(int)
    df_oferta['C_PROVEEDOR_PRIMARIO'] = df_oferta['C_PROVEEDOR_PRIMARIO'].astype(int)

    # PROCESAR los datos de STOCK
    # Creamos un DataFrame vacío para ir apilando las filas transformadas
    df_trf_stock = pd.DataFrame()

    # Iteramos sobre las columnas de STOCK
    for col in df_stock.columns:
        if col.startswith("Q_DIA"):
            # Extraemos el número del día de la columna (por ejemplo, 'Q_DIA1' -> 1)
            dia = int(col.replace("Q_DIA", ""))
            
            # Copiamos los datos necesarios para este día
            df_tmp = df_stock[['C_ANIO', 'C_MES', 'C_SUCU_EMPR', 'C_PROVEEDOR_PRIMARIO', 'C_ARTICULO', col]].copy()
            df_tmp['DIA'] = dia
            
            # Creamos la fecha a partir del año, mes y día
            df_tmp['FECHA'] = df_tmp.apply(lambda row: es_fecha_valida(int(row['C_ANIO']), int(row['C_MES']), int(row['DIA'])), axis=1) # type: ignore

            # Si quieres crear una columna con el valor de stock, puedes hacer:
            df_tmp['STOCK'] = df_tmp[col].astype(float)   
            
            # Filtramos fechas inválidas
            df_tmp = df_tmp.dropna(subset=['FECHA'])
            
            # Filtramos fechas futuras (solo fechas hasta el día anterior)
            df_tmp = df_tmp[df_tmp['FECHA'] <= (datetime.now().date() - timedelta(days=1))]

            # Creamos la columna de 'OFERTA' (1 si tiene oferta, 0 si no)
            df_tmp['OFERTA'] = df_tmp[col].apply(lambda x: 1 if x == 'S' else 0)
            
            # Ahora solo conservamos las columnas relevantes
            df_tmp = df_tmp[['C_PROVEEDOR_PRIMARIO','C_ARTICULO', 'C_SUCU_EMPR', 'FECHA', 'STOCK']]
            
            # Agregamos al DataFrame final
            df_trf_stock = pd.concat([df_trf_stock, df_tmp], ignore_index=True)
    
    # PROCESAR los datos de OFERTAS
    # Creamos un DataFrame vacío para ir apilando las filas transformadas
    df_transformado = pd.DataFrame()

    # Iteramos sobre las columnas de ofertas
    for col in df_oferta.columns:
        if col.startswith("M_OFERTA_DIA"):
            # Extraemos el número del día de la columna (por ejemplo, 'M_OFERTA_DIA1' -> 1)
            dia = int(col.replace("M_OFERTA_DIA", ""))
            
            # Copiamos los datos necesarios para este día
            df_tmp = df_oferta[['C_ANIO', 'C_MES', 'C_SUCU_EMPR', 'C_PROVEEDOR_PRIMARIO', 'C_ARTICULO', col]].copy()
            df_tmp['DIA'] = dia
            
            # Creamos la fecha a partir del año, mes y día
            df_tmp['FECHA'] = df_tmp.apply(lambda row: es_fecha_valida(row['C_ANIO'], row['C_MES'], row['DIA']), axis=1) # type: ignore
            
            # Filtramos fechas inválidas
            df_tmp = df_tmp.dropna(subset=['FECHA'])
            
            # Filtramos fechas futuras (solo fechas hasta el día anterior)
            df_tmp = df_tmp[df_tmp['FECHA'] <= (datetime.now().date() - timedelta(days=1))]

            # Creamos la columna de 'OFERTA' (1 si tiene oferta, 0 si no)
            df_tmp['OFERTA'] = df_tmp[col].apply(lambda x: 1 if x == 'S' else 0)
            
            # Ahora solo conservamos las columnas relevantes
            df_tmp = df_tmp[['C_PROVEEDOR_PRIMARIO','C_ARTICULO', 'C_SUCU_EMPR', 'FECHA', 'OFERTA']]
            
            # Agregamos al DataFrame final
            df_transformado = pd.concat([df_transformado, df_tmp], ignore_index=True)
    
    # Realizamos el merge para agregar las columnas de df_transformado_stock a df_transformado
    df_combined = pd.merge(df_transformado, df_trf_stock[['C_PROVEEDOR_PRIMARIO','C_ARTICULO', 'C_SUCU_EMPR', 'FECHA', 'STOCK']], 
                    on=['C_PROVEEDOR_PRIMARIO','C_ARTICULO', 'C_SUCU_EMPR', 'FECHA'], how='left')
    df_combined.fillna(0, inplace=True)  # Rellenar NaN con 0 para las columnas de stock y ofertas
    
    # FILTRAR ÚLTIMOS 60 DIAS
    # Obtener la fecha actual
    fecha_actual = datetime.now().date()
    fecha_limite = fecha_actual - timedelta(days=60)
    df_filtered = df_combined[df_combined['FECHA'] >= fecha_limite]
    
    # SUBIR DATOS A PostgreSQL
    # Llamada a la función para subir el DataFrame a PostgreSQL
    upload_to_postgres(df_filtered)
    
    logger.info(f"Datos cargados en PostgreSQL → historico_ofertas_stock ({len(df_filtered)} filas).")
    return 


@flow(name="historico_ofertas_stock_proveedores")
def obtener_historico_sofertas(lista_ids):
    log = get_run_logger()
    try:
        filas_ventas = cargar_ofertas_stock.with_options(name="Historico Ofertas y Stock").submit(lista_ids).result()
        log.info(f"Ventas: {filas_ventas} filas insertadas")
    except Exception as e:
        log.error(f"Error cargando Ofertas y Stock: {e}")


if __name__ == "__main__":
    ids = list(map(int, sys.argv[1:]))  # ← lee los proveedores como argumentos
    obtener_historico_sofertas(ids)
    logger.info("--------------->  Flujo de replicación FINALIZADO.")
