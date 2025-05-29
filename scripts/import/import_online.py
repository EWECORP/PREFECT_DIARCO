import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import shutil
from prometheus_client import Gauge, start_http_server, Summary
import psutil # type: ignore
import traceback

# MÉTRICAS PROMETHEUS
tota_file_size_mb = 0
tota_file_size_mb_error = 0
CPU_USAGE = Gauge('cpu_usage', 'CPU Usage')
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
csv_file_size = Gauge('diarco_inbound_upload_csv_files_mb_total', 'Size of processed CSV files in MB', ['status'])

start_http_server(8000)

# FUNCIONES DE MONITOREO
def monitor_cpu():
    CPU_USAGE.set(psutil.cpu_percent())

# DIRECTORIOS
directorio_archivos = '/sftp/archivos/usr_diarco/data-online'
directorio_archivos_backup = os.path.join(directorio_archivos, 'backup')
directorio_archivos_errores = os.path.join(directorio_archivos, 'errores_csv')
os.makedirs(directorio_archivos_backup, exist_ok=True)
os.makedirs(directorio_archivos_errores, exist_ok=True)

# VALIDACIÓN ESTRUCTURA CSV
def validar_estructura_csv(path, columnas_esperadas, sep='|'):
    lineas_invalidas = []
    with open(path, 'r', encoding='latin1') as f:
        for i, line in enumerate(f, start=1):
            columnas = line.strip().split(sep)
            if len(columnas) != columnas_esperadas:
                lineas_invalidas.append((i, len(columnas)))
    return lineas_invalidas

# LOG DE ERRORES y EXPORTACIÓN CSV
def registrar_error_log(nombre_archivo, errores):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(directorio_archivos_errores, f"error_{nombre_archivo}_{timestamp}.log")
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(f"Errores en archivo {nombre_archivo}\n\n")
        for linea, columnas in errores:
            f.write(f"Línea {linea}: {columnas} columnas\n")
    return log_path

def exportar_lineas_invalidas_csv(path_csv_original, errores, destino_dir, columnas_esperadas, sep='|'):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_nombre = os.path.basename(path_csv_original).split('.')[0]
    archivo_salida = os.path.join(destino_dir, f"{archivo_nombre}_lineas_invalidas_{timestamp}.csv")

    with open(path_csv_original, 'r', encoding='latin1') as f_in, open(archivo_salida, 'w', encoding='utf-8') as f_out:
        for i, line in enumerate(f_in, start=1):
            if any(error_linea == i for error_linea, _ in errores):
                columnas = line.strip().split(sep)
                columnas += [''] * (columnas_esperadas - len(columnas))
                columnas = columnas[:columnas_esperadas]
                f_out.write('|'.join(columnas) + '\n')

    return archivo_salida

# ZIP
@REQUEST_TIME.time()
def buscar_archivos_zip(directorio):
    return [os.path.join(directorio, a) for a in os.listdir(directorio) if a.lower().endswith('.zip')]

@REQUEST_TIME.time()
def descomprimir_archivo_zip(ruta_zip, destino):
    with zipfile.ZipFile(ruta_zip, 'r') as archivo_zip:
        archivo_zip.extractall(destino)
    os.remove(ruta_zip)

# BD
engine = create_engine('postgresql+psycopg2://diarco_data:aladelta10$@localhost/diarco_data')
engine = engine.execution_options(autocommit=True)

@REQUEST_TIME.time()
def contar_registros(nombre_tabla, engine):
    with engine.connect() as conn:
        return conn.execute(text(f'SELECT COUNT(*) FROM "{nombre_tabla}";')).scalar()

@REQUEST_TIME.time()
def truncar_tabla_si_m(nombre_tabla, engine):
    if nombre_tabla.startswith("m_"):
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(text(f'TRUNCATE TABLE "{nombre_tabla}";'))
                trans.commit()
            except Exception:
                trans.rollback()

@REQUEST_TIME.time()
def insertar_datos(nombre_tabla, dataframe, engine):
    dataframe.to_sql(nombre_tabla, engine, if_exists='append', index=False)

def obtener_columnas_tabla(nombre_tabla, engine):
    inspector = inspect(engine)
    return [col['name'] for col in inspector.get_columns(nombre_tabla)]

def mover_archivo(archivo_origen, destino):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo, extension = os.path.splitext(os.path.basename(archivo_origen))
    nuevo_nombre_archivo = f"{nombre_archivo}_{timestamp}{extension}"
    shutil.move(archivo_origen, os.path.join(destino, nuevo_nombre_archivo))

# PROCESO ZIP
archivos_zip = buscar_archivos_zip(directorio_archivos)
for archivo_zip in archivos_zip:
    descomprimir_archivo_zip(archivo_zip, directorio_archivos)

# PROCESAR CSVs
chunksize = 10000
for archivo in os.listdir(directorio_archivos):
    if archivo.endswith(".csv"):
        nombre_tabla = archivo.split('.')[0].lower()
        archivo_ruta = os.path.join(directorio_archivos, archivo)

        file_size_mb = os.path.getsize(archivo_ruta) / (1024 * 1024)
        tota_file_size_mb += file_size_mb

        try:
            monitor_cpu()
            print(f"📄 Procesando archivo CSV: {archivo}")

            columnas_tabla = obtener_columnas_tabla(nombre_tabla, engine)
            errores_estructura = validar_estructura_csv(archivo_ruta, len(columnas_tabla))

            if errores_estructura:
                print(f"❌ Estructura inválida en {archivo}: {len(errores_estructura)} líneas incorrectas.")
                log_path = registrar_error_log(nombre_tabla, errores_estructura)
                csv_invalidas_path = exportar_lineas_invalidas_csv(
                    archivo_ruta, errores_estructura,
                    destino_dir=directorio_archivos_errores,
                    columnas_esperadas=len(columnas_tabla)
                )
                print(f"🧾 Log generado en: {log_path}")
                print(f"📤 CSV con líneas inválidas: {csv_invalidas_path}")
                mover_archivo(archivo_ruta, directorio_archivos_errores)
                tota_file_size_mb_error += file_size_mb
                csv_file_size.labels(status='error').set(tota_file_size_mb_error)
                continue

            truncar_tabla_si_m(nombre_tabla, engine)
            contar_registros(nombre_tabla, engine)

            for chunk in pd.read_csv(archivo_ruta, sep='|', encoding='latin1', header=None,
                                    low_memory=False, chunksize=chunksize, skiprows=1, dtype=str):
                if len(chunk.columns) == len(columnas_tabla):
                    chunk.columns = columnas_tabla
                    insertar_datos(nombre_tabla, chunk, engine)
                else:
                    raise ValueError(f"Columnas inesperadas en chunk de {archivo}")

            contar_registros(nombre_tabla, engine)
            mover_archivo(archivo_ruta, directorio_archivos_backup)
            csv_file_size.labels(status='success').set(tota_file_size_mb)
            print(f"✅ Archivo {archivo} procesado con éxito.")

        except Exception as e:
            tota_file_size_mb_error += file_size_mb
            csv_file_size.labels(status='error').set(tota_file_size_mb_error)
            traceback.print_exc()
            mover_archivo(archivo_ruta, directorio_archivos_errores)
