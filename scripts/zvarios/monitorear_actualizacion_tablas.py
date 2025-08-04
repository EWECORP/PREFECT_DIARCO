
# monitorear_actualizacion_tablas.py
# Objetivo: Verificar que las tablas críticas del esquema `src` estén actualizadas con datos recientes.
# Autor: [Zeetrex]
# Fecha: [Auto-generado]

import psycopg2 as pg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection as PGConnection
from datetime import date
import time
import logging
import os
import sys
from dotenv import dotenv_values

import pandas as pd
from typing import List, Dict
from psycopg2 import sql


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


def obtener_control_interfaces(tablas_por_fecha: Dict[str, str],
                               max_retries: int = 3,
                               retry_delay: float = 2.0) -> pd.DataFrame:
    """
    Realiza control de interfaces sobre múltiples tablas con diferentes campos de fecha.
    Retorna un DataFrame con columnas: tabla, campo_fecha, ultima_fecha_extraccion, cantidad_registros.
    """
    registros = []
    conn = Open_Diarco_Data()
    conn.autocommit = True

    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SET statement_timeout = 0;")

        for tabla, campo_fecha in tablas_por_fecha.items():
            logging.info(f"Procesando tabla: {tabla} (campo: {campo_fecha})")

            query = sql.SQL("""
                SELECT
                    MAX({campo_fecha}) AS ultima_fecha,
                    COUNT(*) AS cantidad
                FROM src.{tabla}
                WHERE {campo_fecha}::date = (
                    SELECT MAX({campo_fecha}::date)
                    FROM src.{tabla}
                )
            """).format(
                tabla=sql.Identifier(tabla),
                campo_fecha=sql.Identifier(campo_fecha)
            )

            for intento in range(1, max_retries + 1):
                try:
                    cur.execute(query)
                    fila = cur.fetchone()
                    registros.append({
                        "tabla": tabla,
                        "campo_fecha": campo_fecha,
                        "ultima_fecha_extraccion": fila["ultima_fecha"],
                        "cantidad_registros": fila["cantidad"]
                    })
                    break
                except Exception as e:
                    logging.error(f"Error en tabla {tabla}, intento {intento}: {e}", exc_info=True)
                    if intento < max_retries:
                        time.sleep(retry_delay)
                    else:
                        registros.append({
                            "tabla": tabla,
                            "campo_fecha": campo_fecha,
                            "ultima_fecha_extraccion": None,
                            "cantidad_registros": None
                        })

    conn.close()
    return pd.DataFrame(registros,
                        columns=["tabla", "campo_fecha", "ultima_fecha_extraccion", "cantidad_registros"])

def grabar_estado_tablas(df: pd.DataFrame, conn: PGConnection) -> None:
    """
    Graba el contenido del DataFrame en la tabla src.monitoreo_estado_tablas.

    Parámetros:
    - df: DataFrame con columnas ['tabla', 'ultima_fecha_extraccion', 'cantidad_registros']
    - conn: conexión abierta a PostgreSQL (psycopg2)
    """
    insert_sql = """
        INSERT INTO src.monitoreo_estado_tablas (tabla, ultima_fecha_extraccion, cantidad_registros)
        VALUES (%s, %s, %s)
        ON CONFLICT (tabla) DO UPDATE SET
            ultima_fecha_extraccion = EXCLUDED.ultima_fecha_extraccion,
            cantidad_registros = EXCLUDED.cantidad_registros,
            fecha_control = NOW()
    """

    with conn.cursor() as cur:
        for _, fila in df.iterrows():
            try:
                cur.execute(insert_sql, (
                    fila["tabla"],
                    fila["ultima_fecha_extraccion"],
                    fila["cantidad_registros"]
                ))
            except Exception as e:
                logging.error(f"❌ Error al insertar tabla {fila['tabla']}: {e}", exc_info=True)
                continue

        conn.commit()
        logging.info(f"✅ Control de estado insertado exitosamente para {len(df)} tablas.")


# Configuración de las tablas a controlar
# 1. Tablas con campo 'fecha_extraccion'
tablas_fechas_estandar = [
    "base_forecast_articulos", "base_productos_vigentes", "base_stock_sucursal", "t020_proveedor",
    "m_3_articulos", "t020_proveedor_gestion_compra", "t050_articulos", "t051_articulos_sucursal", 
    "t060_stock", "t080_oc_cabe", "t081_oc_deta", "t100_empresa_suc", "t052_articulos_proveedor",
    "t710_estadis_oferta_folder", "t710_estadis_precios",
    "t710_estadis_reposicion"
]
tablas_dict_1 = {tabla: "fecha_extraccion" for tabla in tablas_fechas_estandar}

# 2. Tablas con campo personalizado
tablas_dict_2 = {
    "t114_rubros": "f_alta",
    "t117_compradores": "f_modif",
    "m_91_sucursales": "f_proc",
    "m_92_depositos": "f_proc",
    "m_93_sustitutos": "f_proc",
    "m_94_alternativos": "f_proc",
    "m_95_sensibles": "f_proc",
    "m_96_stock_seguridad": "f_proc",
    "t702_est_vtas_por_articulo" : "f_venta", 
    "t702_est_vtas_por_articulo_dbarrio": "f_venta"
}

# 3. Tablas sin campo para controlar
tablas_dict_3 = {
    "t020_proveedor_gestion_compra": "?",
   
}


# Unificar en un solo diccionario
tablas_total = {**tablas_dict_1, **tablas_dict_2}


if __name__ == "__main__":
    try:
        logging.info("🚀 Iniciando monitoreo de actualización de tablas críticas del esquema src...")
        t0 = time.time()

        conn_pg = Open_Diarco_Data()
        if conn_pg is None:
            raise RuntimeError("No se pudo establecer conexión con la base de datos.")

        df_control = obtener_control_interfaces(tablas_total)
        print(df_control)

        grabar_estado_tablas(df_control, conn_pg)

        errores = df_control[df_control["ultima_fecha_extraccion"].isnull()]
        if not errores.empty:
            logging.warning(f"⚠️ Tablas con errores o sin datos: {errores['tabla'].tolist()}")
        else:
            logging.info("✅ Todas las tablas procesadas correctamente.")

        conn_pg.close()
        elapsed = round(time.time() - t0, 2)
        logging.info(f"🕒 Monitoreo completado en {elapsed} segundos.")

    except Exception as e:
        logging.critical(f"🚨 Fallo de conexión o ejecución general: {str(e)}", exc_info=True)
