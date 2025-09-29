# utils/postgres.py

import os
import csv
import zipfile
import shutil
import tempfile
from datetime import datetime

import pandas as pd
import psycopg2
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text, inspect

PG_CONN_STR = "postgresql+psycopg2://postgres:aladelta10$@localhost:5432/diarco_data"

PG_RAW_CONN = {
    "host": "localhost",
    "port": "5432",
    "dbname": "diarco_data",
    "user": "postgres",
    "password": "aladelta10$"
}

# Directorio donde se encuentran los archivos CSV
dir_archivos = '/sftp/archivos/usr_diarco/orquestador'
dir_procesado = '/sftp/archivos/usr_diarco/orquestador/backup'
dir_unzip = '/sftp/archivos/usr_diarco/orquestador/unzip'


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

# Función para mover un archivo después de procesarlo
def mover_archivo(archivo_origen: str, destino: str) -> None:
    ensure_dir(destino)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo, extension = os.path.splitext(os.path.basename(archivo_origen))
    nuevo_nombre_archivo = f"{nombre_archivo}_{timestamp}{extension}"
    shutil.move(archivo_origen, os.path.join(destino, nuevo_nombre_archivo))
    print(f"Archivo {archivo_origen} movido a {os.path.join(destino, nuevo_nombre_archivo)}")
    
# --- Normalización de booleanos ---
TRUE_TOKENS  = {'true','t','1','sí','si','y','yes'}
FALSE_TOKENS = {'false','f','0','no','n'}

def tipos_destino_pg(esquema: str, tabla: str) -> dict[str, str]:
    """Devuelve {columna: tipo_en_pg} en minúsculas."""
    with psycopg2.connect(**PG_RAW_CONN) as conn: # type: ignore
        with conn.cursor() as cur:
            cur.execute("""
                SELECT lower(column_name), lower(data_type)
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (esquema, tabla.lower()))
            return dict(cur.fetchall())

def normalizar_booleanos_en_csv(csv_path: str, mapping: dict[str, str], null_token: str = 'NULL') -> None:
    """
    Reescribe el CSV convirtiendo banderas segun {col: 'int'|'bool'}.
    - 'int'  -> '1' / '0' / 'NULL'
    - 'bool' -> 'true' / 'false' / 'NULL'
    """
    fd, tmp = tempfile.mkstemp(suffix=".csv")
    os.close(fd)

    with open(csv_path, 'r', encoding='utf-8', newline='') as src, \
        open(tmp,      'w', encoding='utf-8', newline='') as dst:

        r = csv.reader(src, delimiter='|', quotechar='"', escapechar='"')
        w = csv.writer(dst, delimiter='|', quotechar='"', escapechar='"')

        header = next(r)
        header_lower = [h.lower() for h in header]
        pos = {i: mapping[h] for i, h in enumerate(header_lower) if h in mapping}

        w.writerow(header)
        for row in r:
            for i, target in pos.items():
                v = (row[i] or '').strip()
                if v == '' or v.upper() == null_token:
                    row[i] = null_token
                    continue
                lv = v.lower()
                if target == 'int':
                    if lv in TRUE_TOKENS:   row[i] = '1'
                    elif lv in FALSE_TOKENS: row[i] = '0'
                    else:                    row[i] = null_token
                else:  # 'bool'
                    if lv in TRUE_TOKENS:   row[i] = 'true'
                    elif lv in FALSE_TOKENS: row[i] = 'false'
                    else:                    row[i] = null_token
            w.writerow(row)

    os.replace(tmp, csv_path)


@task
def descomprimir_archivo(zip_path: str) -> str:
    logger = get_run_logger()

    # Carpeta específica del ZIP dentro de dir_unzip
    base = os.path.splitext(os.path.basename(zip_path))[0]
    dest_dir = os.path.join(dir_unzip, base)
    ensure_dir(dest_dir)

    with zipfile.ZipFile(zip_path, 'r') as zipf:
        nombres_archivos = [f for f in zipf.namelist() if f.lower().endswith(".csv")]
        if not nombres_archivos:
            raise ValueError("El ZIP no contiene archivos .csv")
        archivo_csv_original = nombres_archivos[0]
        zipf.extract(archivo_csv_original, path=dest_dir)

    # Renombrar a minúsculas (ruta completa)
    src = os.path.join(dest_dir, archivo_csv_original)
    dst = os.path.join(dest_dir, os.path.basename(archivo_csv_original).lower())
    if src != dst:
        os.replace(src, dst)

    logger.info(f"CSV extraído en: {dst}")
    return dst  # ruta completa al CSV

@task
def validar_o_crear_tabla(schema: str, tabla: str, archivo_csv: str):
    logger = get_run_logger()
    engine = create_engine(PG_CONN_STR)

    # Muestra para columnas (y tipos si hubiera que crear)
    df_csv = pd.read_csv(archivo_csv, delimiter='|', nrows=100)
    tabla = tabla.lower()
    df_csv.columns = [col.lower() for col in df_csv.columns]
    columnas_csv = df_csv.columns.tolist()

    with engine.begin() as conn:
        # MUY IMPORTANTE: inspeccionar con la MISMA conexión/tx
        inspector = inspect(conn)

        tabla_existe = inspector.has_table(tabla, schema=schema)
        if tabla_existe:
            columnas_pg = [c['name'].lower() for c in inspector.get_columns(tabla, schema=schema)]

            # Si el conjunto de columnas es el mismo, la consideramos compatible,
            # aunque el orden difiera (evitamos recrear y el error visto).
            mismo_conjunto = set(columnas_pg) == set(columnas_csv)

            if not mismo_conjunto:
                logger.warning(f"Estructura incompatible: eliminando tabla {schema}.{tabla}")
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tabla}" CASCADE'))

                # Crear tabla vacía con la misma CONEXIÓN (misma tx)
                df_csv.iloc[0:0].to_sql(tabla, con=conn, schema=schema, index=False)
                logger.info(f"Tabla {schema}.{tabla} recreada a partir del CSV")
            else:
                logger.info("Estructura de tabla compatible (mismo conjunto de columnas). No se recrea.")
                # Opcional: si quieren alinear orden físico, pueden recrear,
                # pero NO es necesario para el COPY que ustedes hacen.
        else:
            logger.info(f"Creando tabla {schema}.{tabla}")
            # Crear tabla vacía con la MISMA conexión
            df_csv.iloc[0:0].to_sql(tabla, con=conn, schema=schema, index=False)

@task
def cargar_csv_postgres(csv_path: str, esquema: str, tabla: str):
    logger = get_run_logger()
    tabla = tabla.lower()

    # Columnas del CSV (respetar orden)
    df_temp = pd.read_csv(csv_path, delimiter='|', dtype=str, nrows=1)
    columnas = [col.lower() for col in df_temp.columns]

    # Tipos en PG y mapeo de banderas
    tipos_pg = tipos_destino_pg(esquema, tabla)
    candidatos_bool = {
        'habilitado',
        'promocion',
        'active_for_purchase',
        'active_for_sale',
        'active_on_mix',
        'own_production',
    }
    presentes = [c for c in columnas if c in candidatos_bool]
    mapping: dict[str, str] = {}
    for c in presentes:
        t = tipos_pg.get(c, '')
        if t.startswith(('int','bigint','smallint')):
            mapping[c] = 'int'
        elif 'boolean' in t:
            mapping[c] = 'bool'

    if mapping:
        normalizar_booleanos_en_csv(csv_path, mapping, null_token='NULL')

    # (Opcional) si su exportador deja campos vacíos y quieren que cuenten como NULL,
    # cambien abajo a:  NULL ''   en lugar de  NULL 'NULL'
    try:
        total_lineas = sum(1 for _ in open(csv_path, encoding='utf-8')) - 1
    except Exception:
        total_lineas = None

    with psycopg2.connect(**PG_RAW_CONN) as conn: # type: ignore
        with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as f:
            next(f)  # Skip header
            cur.copy_expert(
                sql=f"""
                    COPY "{esquema}"."{tabla}" ({','.join(f'"{col}"' for col in columnas)})
                    FROM STDIN 
                    WITH CSV 
                    DELIMITER '|' 
                    NULL 'NULL' 
                    QUOTE '"' 
                    ESCAPE '"' 
                """,
                file=f
            )
        conn.commit()

    logger.info(
        f"✅ CSV cargado en {esquema}.{tabla}" +
        (f" ({total_lineas} filas)" if total_lineas is not None else "")
    )

    # Limpiar CSV extraído
    os.remove(csv_path)

@flow(name="importar_csv_pg")
def importar_csv_pg(esquema: str, tabla: str, nombre_zip: str):
    logger = get_run_logger()
    logger.info(f"[PARAMS] esquema={esquema} tabla={tabla} zip={nombre_zip}")

    zip_path = nombre_zip if os.path.isabs(nombre_zip) else os.path.join(dir_archivos, nombre_zip)
    csv_file = descomprimir_archivo(zip_path)
    validar_o_crear_tabla(esquema, tabla, csv_file)
    cargar_csv_postgres(csv_file, esquema, tabla)
    mover_archivo(zip_path, dir_procesado)

if __name__ == "__main__":
    importar_csv_pg("repl", "t055_articulos_param_stock", "repl_T055_ARTICULOS_PARAM_STOCK_20250527_150000.zip")
