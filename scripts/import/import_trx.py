import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import shutil  # Para mover archivos
from prometheus_client import Gauge, start_http_server, Summary
import psutil
import traceback

tota_file_size_mb = 0
tota_file_size_mb_error = 0

CPU_USAGE = Gauge('cpu_usage', 'CPU Usage')

# Crear una métrica Summary para medir la latencia de las funciones
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')

start_http_server(8001)

def monitor_cpu():
    CPU_USAGE.set(psutil.cpu_percent())

csv_file_size = Gauge('diarco_inbound_upload_csv_files_mb_total', 'Size of processed CSV files in MB', ['status'])

# Directorio donde se encuentran los archivos CSV
directorio_archivos = '/sftp/archivos/usr_diarco/data-trx'
directorio_archivos_backup = '/sftp/archivos/usr_diarco/data-trx/backup'

# Función para buscar archivos ZIP en un directorio
@REQUEST_TIME.time()
def buscar_archivos_zip(directorio):
    archivos_zip = []
    for archivo in os.listdir(directorio):
        if archivo.lower().endswith('.zip'):
            archivos_zip.append(os.path.join(directorio, archivo))
    return archivos_zip

# Función para descomprimir archivos ZIP
@REQUEST_TIME.time()
def descomprimir_archivo_zip(ruta_zip, destino):
    with zipfile.ZipFile(ruta_zip, 'r') as archivo_zip:
        archivo_zip.extractall(destino)
    print(f'Archivo {ruta_zip} descomprimido en {destino}')
    
    # Eliminar el archivo ZIP después de descomprimir
    try:
        os.remove(ruta_zip)
        print(f"Archivo ZIP {ruta_zip} eliminado.")
    except Exception as e:
        print(f"Error al eliminar el archivo ZIP {ruta_zip}: {e}")

# Procesar todos los archivos ZIP encontrados en el directorio
archivos_zip = buscar_archivos_zip(directorio_archivos)
if archivos_zip:
    for archivo_zip in archivos_zip:
        descomprimir_archivo_zip(archivo_zip, directorio_archivos)

# Conexión a la base de datos PostgreSQL
engine = create_engine('postgresql+psycopg2://diarco_data:aladelta10$@localhost/diarco_data')
engine = engine.execution_options(autocommit=True)

# Función para contar registros en una tabla
@REQUEST_TIME.time()
def contar_registros(nombre_tabla, engine):
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{nombre_tabla}";'))  # Usar comillas dobles para el nombre de la tabla
        row_count = result.scalar()  # Obtener el valor único
        print(f"Cantidad de registros en {nombre_tabla}: {row_count}")
        return row_count

# Función para truncar una tabla si el nombre comienza con "m_"
@REQUEST_TIME.time()
def truncar_tabla_si_m(nombre_tabla, engine):
    if nombre_tabla.startswith("m_"):
        with engine.connect() as conn:
            trans = conn.begin()  # Iniciar una transacción
            try:
                print(f"Truncating table {nombre_tabla}...")
                conn.execute(text(f'TRUNCATE TABLE "{nombre_tabla}";'))
                trans.commit()  # Confirmar la transacción
                print(f"Table {nombre_tabla} truncated successfully.")
            except Exception as e:
                trans.rollback()  # Revertir la transacción en caso de error
                print(f"Error truncating table {nombre_tabla}: {e}")

# Función para insertar datos
@REQUEST_TIME.time()
def insertar_datos(nombre_tabla, dataframe, engine):
    dataframe.to_sql(nombre_tabla, engine, if_exists='append', index=False)

# Función para obtener los nombres de las columnas de la tabla
def obtener_columnas_tabla(nombre_tabla, engine):
    inspector = inspect(engine)
    columnas = [col['name'] for col in inspector.get_columns(nombre_tabla)]
    return columnas

# Función para mover un archivo después de procesarlo
def mover_archivo(archivo_origen, destino):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo, extension = os.path.splitext(os.path.basename(archivo_origen))
    nuevo_nombre_archivo = f"{nombre_archivo}_{timestamp}{extension}"
    archivo_destino = os.path.join(destino, nuevo_nombre_archivo)
    
    shutil.move(archivo_origen, archivo_destino)
    print(f"Archivo {archivo_origen} movido a {archivo_destino}")

# Procesar cada archivo CSV
chunksize = 10000  # Número de filas a procesar por chunk
for archivo in os.listdir(directorio_archivos):
    if archivo.endswith(".csv"):
        nombre_tabla = archivo.split('.')[0].lower()  # Convertir el nombre de la tabla a minúsculas
        archivo_ruta = os.path.join(directorio_archivos, archivo)

        file_size_bytes = os.path.getsize(archivo_ruta)
        file_size_mb = file_size_bytes / (1024 * 1024)  # Convertir a megabytes
        tota_file_size_mb += file_size_mb

        try:
            monitor_cpu()
            print(f"Leer csv {nombre_tabla} ...")

            # Obtener las columnas de la tabla en la base de datos
            columnas_tabla = obtener_columnas_tabla(nombre_tabla, engine)
            
            # Truncar la tabla si su nombre comienza con "m_"
            # truncar_tabla_si_m(nombre_tabla, engine)

            # Contar la cantidad de registros en la tabla antes del insert
            contar_registros(nombre_tabla, engine)

            monitor_cpu()
            print(f"Procesando chunks para la tabla {nombre_tabla} ...")

            # Leer el archivo CSV por chunks y procesar cada uno
            for chunk in pd.read_csv(archivo_ruta, sep='|', encoding='latin1', header=None, low_memory=False, chunksize=chunksize, dtype=str):
                # Validar que las columnas coinciden
                if len(chunk.columns) == len(columnas_tabla):
                    chunk.columns = columnas_tabla  # Asignar nombres de las columnas
                else:
                    raise ValueError(f"El número de columnas del archivo CSV no coincide con la tabla '{nombre_tabla}' en la base de datos.")

                # Insertar el chunk en la tabla
                insertar_datos(nombre_tabla, chunk, engine)

            # Contar la cantidad de registros en la tabla después del insert
            contar_registros(nombre_tabla, engine)

            monitor_cpu()
            print(f"Datos insertados en la tabla {nombre_tabla}.")

            csv_file_size.labels(status='success').set(tota_file_size_mb)

            # Mover el archivo procesado a la carpeta de respaldo
            mover_archivo(archivo_ruta, directorio_archivos_backup)

        except Exception as e:
            tota_file_size_mb_error += file_size_mb
            csv_file_size.labels(status='error').set(tota_file_size_mb_error)

            # Obtiene el detalle del error con traceback
            error_details = traceback.format_exc()
            print(f"Error procesando el archivo {archivo}: {e}")
            print("Detalle del error:")
            print(error_details)  # Muestra el stack trace completo

            # Continuar con el siguiente archivo
            continue
