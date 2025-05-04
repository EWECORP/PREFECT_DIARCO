# Esto es para ejecutar el script desde cualquier directorio
import os
import sys

# Agregar raíz del proyecto al path para que funcione en cualquier contexto de ejecución
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#Evita error de ejecución interactiva desde VSCode

from prefect import flow, task
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
from scripts.logger import log_event

# Cargar variables del entorno
load_dotenv(".env")

@task
def extract_from_sql_server():
    try:
        log_event("START", "Extracción iniciada.")
        server = os.getenv("SQL_SERVER")
        database = os.getenv("SQL_DATABASE")
        username = os.getenv("SQL_USER")
        password = os.getenv("SQL_PASSWORD")
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str)

        query = """
        SELECT TOP (1000) [C_ARTICULO],[C_FAMILIA],[C_RUBRO],[C_SUBRUBRO_1],[C_SUBRUBRO_2],[C_FAMILIA_ANTERIOR],[D_RUBRO_ANTERIOR],[C_PROVEEDOR_PRIMARIO],[F_ALTA],[N_ARTICULO]
        ,[N_ARTICULO_FACT],[C_MARCA],[C_IB],[C_IVA],[C_ENVASE],[Q_PESO_UNIT_ART],[M_VENDE_POR_PESO],[M_EXPORTACION],[M_LISTO_PARA_VENTA],[M_PROMOCION],[M_IMPORTADO],[M_A_DAR_DE_BAJA]
        ,[M_INSTITUCIONAL],[C_EAN],[C_DUN14],[Q_FACTOR_VENTA_ESP],[M_BAJA],[F_BAJA],[D_CARTEL_LIN1],[D_CARTEL_LIN2],[D_CARTEL_LIN3],[C_USUARIO],[C_TERMINAL],[M_OCASION],[N_PAIS_ORIGEN]
        ,[M_BEBIDA_ALCOHOLICA],[C_UNIDAD_MEDIDA],[Q_UNIDAD_MEDIDA],[M_CONSIGNACION],[M_TARJETA_DE_CREDITO],[C_PROVINCIA_TRANSP_1],[C_UNICO_PRODUCTO_TRANSP_1],[C_UNID_MEDIDA_TRANSP_1],[C_CODIGO_EQUIV],[M_CELIACO]
        ,[M_ENVASE_VACIO],[C_COMPRADOR],[C_SUBRUBRO_3],[C_SUBRUBRO_4],[D_AVISO_PROMOCION],[M_SENSIBLE_VENTA],[OK_COMPRA],[M_TOP],[M_VARIEDAD_EXTRA],[M_SIN_VENTA],[M_RESTO_TOP],[C_CLASIFICACION_COMPRA],[C_ARTICULO_ALTERNATIVO]
        ,[C_PRESENTACION],[C_EAN_ALTERNATIVO_1],[C_EAN_ALTERNATIVO_2],[C_EAN_ALTERNATIVO_3],[C_EAN_ALTERNATIVO_4],[M_PRECIO_CUIDADO],[M_H12],[M_BULTO_TRANSPARENTE],[M_CONTROLO_SIEMPRE],[M_H12_DIAS],[M_PALLETS],[M_ARTICULO_PADRE_PRECIO]
        ,[M_ARTICULO_CON_SUSTITUTOS],[M_H18],[M_H18_DIAS],[M_MERCADOLIBRE],[M_H_NACION],[M_H_NACION_DIAS],[M_COSTO_LOGISTICO],[C_IVA_VENTAS],[C_IVA_VENTAS_CF],[M_LIQ_PRODUCTO],[F_LIQ_PRODUCTO_DESDE]
        ,[F_LIQ_PRODUCTO_HASTA],[M_KOSHER],[F_MODIF],[C_USUARIO_MODIF]
        FROM [data-sync].[dbo].[T050_ARTICULOS]
        """
        df = pd.read_sql(query, engine)
        log_event("SUCCESS", "Extracción exitosa.")
        return df
    except Exception as e:
        log_event("ERROR", f"Error en extracción: {e}")
        raise

@task
def load_to_postgres(df):
    try:
        log_event("START", "Carga iniciada en PostgreSQL.")
        pg_host = os.getenv("PG_HOST")
        pg_port = os.getenv("PG_PORT")
        pg_db = os.getenv("PG_DB")
        pg_user = os.getenv("PG_USER")
        pg_pass = os.getenv("PG_PASSWORD")

        pg_conn_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        pg_engine = create_engine(pg_conn_str)

        df.to_sql("t050_articulos_tmp", pg_engine, if_exists="replace", index=False)
        log_event("SUCCESS", "Carga exitosa en PostgreSQL.")
    except Exception as e:
        log_event("ERROR", f"Error en carga: {e}")
        raise

@flow(name="sync_sql_env_with_logging")
def sync_sql_env_with_logging():
    df = extract_from_sql_server()
    load_to_postgres(df)

if __name__ == "__main__":
    sync_sql_env_with_logging()
