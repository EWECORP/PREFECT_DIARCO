
import os
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from dotenv import load_dotenv

print(os.getcwd())  # Muestra el directorio actual

# Cargar variables del entorno
load_dotenv()

# Conexiones
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

print(f"SQL://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
print(f"PG:/{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

sql_engine_str = f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
data_sync = create_engine(sql_engine_str)

pg_conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

print(pg_conn_str)
diarco_data = create_engine(pg_conn_str)

@task
def leer_configuracion(path):
    df = pd.read_excel(path).fillna("")
    return df.to_dict(orient="records")

@task
def replicar_en_sqlserver(linkedserver, base, schema, tabla,  filtro, esquema_destino):
    nombre_destino = f"{esquema_destino}.{tabla}"
    nombre_origen = f"[{base}].[{schema}].[{tabla}]"
    if linkedserver:
        nombre_origen = f"[{linkedserver}].[{base}].[{schema}].[{tabla}]"
    
    query = f"SELECT * INTO {nombre_destino} FROM {nombre_origen}"
    if filtro:
        query += f" WHERE {filtro}"
    
    print(f"PARA BORRAR: --> IF OBJECT_ID('{nombre_destino}', 'U') IS NOT NULL DROP TABLE {nombre_destino}")
    print(f"PARA INSERTAR: --> 🛠 SQL EXEC: {query}")
    print("-------------------------------------------")
    
    # with data_sync.connect() as conn:
    #     conn.execute(text(f"IF OBJECT_ID('{nombre_destino}', 'U') IS NOT NULL DROP TABLE {nombre_destino}"))
    #     conn.execute(text(query))
    # print(f"✅ Réplica local en {nombre_destino}")
    
    raw_conn = data_sync.raw_connection()

    cursor = raw_conn.cursor()
    drop_stmt = f"IF OBJECT_ID('{nombre_destino}', 'U') IS NOT NULL DROP TABLE {nombre_destino}"
    print(f"▶ Ejecutando: {drop_stmt}")
    cursor.execute(drop_stmt)

    print(f"▶ Ejecutando: {query}")
    cursor.execute(query)

    raw_conn.commit()
    raw_conn.close()
    print(f"✅ Réplica local en {nombre_destino}")


@task
def cargar_en_postgres(esquema_origen, esquema_destino, tabla):
    # Cargar datos desde SQL Server a PostgreSQL        
    df = pd.read_sql(f"SELECT * FROM {esquema_origen}.{tabla}", data_sync)
    print(f"📦 {len(df)} filas leídas de {esquema_origen}.{tabla}")
    df.to_sql(tabla.lower(), diarco_data, schema=esquema_destino, if_exists="replace", index=False)
    print(f"📥 Cargado en PostgreSQL → {esquema_destino}].{tabla.lower()}")

@flow(name="replicacion_parametrizada_desde_excel")
def replicacion_parametrizada_desde_excel():
    config_path = "./config/tablas_para_replicar.xlsx"
    registros = leer_configuracion(config_path)
    for row in registros:
        replicar_en_sqlserver(
            row["LinkedServer"],
            row["Base"],
            row["Schema"],
            row["Tabla"],
            row["Filtro"],
            row["EsquemaSql"]            
        )
        cargar_en_postgres(row["EsquemaSql"] ,row["EsquemaPg"], row["Tabla"])

if __name__ == "__main__":
    replicacion_parametrizada_desde_excel()
