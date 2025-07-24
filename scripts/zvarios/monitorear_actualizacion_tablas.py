
# monitorear_actualizacion_tablas.py
# Objetivo: Verificar que las tablas críticas del esquema `src` estén actualizadas con datos recientes.
# Autor: [Zeetrex]
# Fecha: [Auto-generado]

import psycopg2 as pg2
from datetime import date
import time
import logging
import os
import sys
from dotenv import dotenv_values

# Configurar logging
logging.basicConfig(
    filename='./logs/monitoreo_tablas.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

ENV_PATH = os.environ.get("ETL_ENV_PATH", "C:/ETL/ETL_DIARCO/.env")  # Toma Producción si está definido, o la ruta por defecto E:\ETL\ETL_DIARCO\.env
# Verificar si el archivo .env existe
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    print(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)
    
secrets = dotenv_values(ENV_PATH)

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

# Tablas a controlar
tablas_a_controlar = [
    "sucursales_excluidas", "base_stock_sucursal", "t100_empresa_suc",
    "t055_articulos_condcompra_costos", "historico_stock_sucursal",
    "t085_articulos_ean_edi", "t702_est_vtas_por_articulo", "t710_estadis_stock",
    "t114_rubros", "t060_stock", "t117_compradores", "t020_proveedor",
    "base_productos_vigentes", "base_forecast_oc_demoradas", "t702_est_vtas_por_articulo_dbarrio",
    "base_forecast_stock", "base_forecast_ventas", "base_forecast_articulos",
    "m_91_sucursales", "base_forecast_precios", "t080_oc_cabe",
    "t051_articulos_sucursal", "t020_proveedor_gestion_compra",
    "t055_articulos_param_stock", "t710_estadis_reposicion",
    "t052_articulos_proveedor", "m_3_articulos", "t055_lead_time_b2_sucursales",
    "t710_estadis_oferta_folder", "t710_estadis_precios", "t050_articulos"
]

def controlar_tablas(conn):
    cur = conn.cursor()
    for tabla in tablas_a_controlar:
        try:
            query = f"""
                SELECT
                    MAX(fecha_extraccion) as ultima_fecha,
                    COUNT(*) as cantidad
                FROM src.{tabla}
                WHERE fecha_extraccion::date = CURRENT_DATE
            """
            cur.execute(query)
            resultado = cur.fetchone()
            ultima_fecha, cantidad = resultado

            cur.execute("""
                INSERT INTO src.monitoreo_estado_tablas (tabla, ultima_fecha_extraccion, cantidad_registros)
                VALUES (%s, %s, %s)
                ON CONFLICT (tabla) DO UPDATE SET
                    ultima_fecha_extraccion = EXCLUDED.ultima_fecha_extraccion,
                    cantidad_registros = EXCLUDED.cantidad_registros,
                    fecha_control = NOW()
            """, (tabla, ultima_fecha, cantidad))
            logging.info(f"✔ Tabla {tabla}: {cantidad} registros con fecha {ultima_fecha}")
        except Exception as e:
            logging.error(f"❌ Error en la tabla {tabla}: {str(e)}")
            continue

    conn.commit()
    cur.close()

if __name__ == "__main__":
    try:
        conn = Open_Diarco_Data()
        controlar_tablas(conn)
        conn.close()
        logging.info("✅ Monitoreo completado correctamente.")
    except Exception as e:
        logging.critical(f"🚨 Fallo de conexión o ejecución: {str(e)}")
