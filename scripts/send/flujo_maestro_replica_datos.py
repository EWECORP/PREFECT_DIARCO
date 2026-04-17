# flujo_maestro_replica_datos.py
# VERSIÓN: 1.0.0
# DESCRIPCIÓN: Flujo maestro para replicar datos entre SFTP y PostgreSQL.   

from datetime import datetime
from prefect import flow, task
from prefect.deployments import run_deployment
import os
import time
import paramiko  # SFTP remoto
from dotenv import load_dotenv

load_dotenv()

# 1. Generar nombre del archivo ZIP
@task
def generar_nombre_archivo(esquema: str, tabla: str) -> str:
    fecha = datetime.today().strftime("%Y%m%d_%H%M%S")
    return f"{esquema}_{tabla}_{fecha}.zip"

# 2. Esperar a que el archivo aparezca en el servidor SFTP remoto
@task(retries=0)
def esperar_archivo_en_sftp_remoto(nombre_zip: str, espera_maxima: int = 1680, intervalo: int = 15):
    ruta_remota = f"./archivos/usr_diarco/orquestador/{nombre_zip}"

    host = os.getenv("SFTP_HOST")
    port = int(os.getenv("SFTP_PORT", "22"))
    user = os.getenv("SFTP_USER")
    password = os.getenv("SFTP_PASSWORD")

    if not all([host, user, password]):
        raise ValueError("❌ Variables de entorno faltantes para conexión SFTP: SFTP_HOST, SFTP_USER, SFTP_PASSWORD.")

    tiempo = 0
    print(f"🔐 Conectando al SFTP remoto: {host}:{port} como {user}")
    while tiempo < espera_maxima:
        try:
            with paramiko.Transport((host, port)) as transport: # pyright: ignore[reportArgumentType]
                transport.connect(username=user, password=password) # pyright: ignore[reportArgumentType]
                sftp = paramiko.SFTPClient.from_transport(transport)
                sftp.stat(ruta_remota) # pyright: ignore[reportOptionalMemberAccess]
                print(f"✅ Archivo disponible en el SFTP remoto: {ruta_remota}")
                return True
        except FileNotFoundError:
            print(f"⏳ [{tiempo}s] Archivo aún no disponible: {ruta_remota}")
        except Exception as e:
            print(f"⚠️ Error en conexión SFTP: {e}")

        time.sleep(intervalo)
        tiempo += intervalo

    raise FileNotFoundError(f"❌ Archivo no encontrado tras {espera_maxima}s en el SFTP remoto: {ruta_remota}")

# 3. Flujo maestro
@flow(name="flujo_maestro_replica_datos")
def flujo_maestro(esquema: str, tabla: str, filtro_sql: str):
    print(f"🚀 Iniciando replicación para {esquema}.{tabla}")

    nombre_zip = generar_nombre_archivo(esquema, tabla)
    print(f"📦 Nombre de archivo generado: {nombre_zip}")

    print(f"📤 Ejecutando flujo exportador...")
    export_result = run_deployment(
        name="exportar_tabla_sql_sftp/exportar_tabla_sql_sftp",
        parameters={
            "esquema": esquema,
            "tabla": tabla,
            "filtro_sql": filtro_sql,
            "nombre_zip": nombre_zip
        },
        timeout=600
    )
    print(f"✅ Exportación completada con estado: {export_result.state.name}")  # type: ignore

    print(f"🔍 Esperando disponibilidad del archivo en el SFTP remoto...")
    esperar_archivo_en_sftp_remoto(nombre_zip)

    print(f"📥 Ejecutando flujo importador...")
    import_result = run_deployment(
        name="importar_csv_pg/importar_csv_pg",
        parameters={
            "esquema": "src",
            "tabla": tabla,
            "nombre_zip": nombre_zip
        },
        timeout=600
    )
    print(f"✅ Importación completada con estado: {import_result.state.name}")  # type: ignore

    print("🎯 Flujo maestro finalizado.")

