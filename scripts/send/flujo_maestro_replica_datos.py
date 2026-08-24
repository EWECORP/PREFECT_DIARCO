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


def validar_timeout_espera(timeout_segundos: float | None, etapa: str) -> None:
    """Valida timeouts que deben esperar un estado final del deployment hijo."""
    if timeout_segundos is not None and timeout_segundos <= 0:
        raise ValueError(
            f"El timeout de {etapa} debe ser mayor que cero o None para esperar sin límite."
        )


def describir_timeout(timeout_segundos: float | None) -> str:
    if timeout_segundos is None:
        return "sin límite de espera"
    return f"con un máximo de {timeout_segundos:g}s"


def validar_estado_deployment(
    resultado,
    etapa: str,
    recurso: str,
    timeout_segundos: float | None,
) -> None:
    """Diferencia un fallo final de un deployment que aún sigue ejecutándose."""
    estado = resultado.state
    if estado.is_completed():
        return

    if not estado.is_final():
        espera = (
            "aunque se configuró una espera sin límite"
            if timeout_segundos is None
            else f"después de esperar {timeout_segundos:g}s"
        )
        raise RuntimeError(
            f"La {etapa} de {recurso} sigue en {estado.name} {espera}. "
            "El deployment hijo puede continuar ejecutándose."
        )

    raise RuntimeError(f"La {etapa} de {recurso} terminó en {estado.name}")

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
def flujo_maestro(
    esquema: str,
    tabla: str,
    filtro_sql: str,
    tabla_destino: str | None = None,
    timeout_exportacion: float | None = None,
    timeout_importacion: float | None = None,
):
    print(f"🚀 Iniciando replicación para {esquema}.{tabla}")

    destino = (tabla_destino or tabla).lower()
    validar_timeout_espera(timeout_exportacion, "exportación")
    validar_timeout_espera(timeout_importacion, "importación")

    nombre_zip = generar_nombre_archivo(esquema, tabla)
    print(f"📦 Nombre de archivo generado: {nombre_zip}")

    print(f"📤 Ejecutando flujo exportador {describir_timeout(timeout_exportacion)}...")
    export_result = run_deployment(
        name="exportar_tabla_sql_sftp/exportar_tabla_sql_sftp",
        parameters={
            "esquema": esquema,
            "tabla": tabla,
            "filtro_sql": filtro_sql,
            "nombre_zip": nombre_zip
        },
        timeout=timeout_exportacion,
    )
    validar_estado_deployment(
        export_result,
        etapa="exportación",
        recurso=f"{esquema}.{tabla}",
        timeout_segundos=timeout_exportacion,
    )
    print(f"✅ Exportación completada con estado: {export_result.state.name}")  # type: ignore

    print(f"🔍 Esperando disponibilidad del archivo en el SFTP remoto...")
    esperar_archivo_en_sftp_remoto(nombre_zip)

    print(f"📥 Ejecutando flujo importador {describir_timeout(timeout_importacion)}...")
    import_result = run_deployment(
        name="importar_csv_pg/importar_csv_pg",
        parameters={
            "esquema": "src",
            "tabla": destino,
            "nombre_zip": nombre_zip
        },
        timeout=timeout_importacion,
    )
    validar_estado_deployment(
        import_result,
        etapa="importación",
        recurso=f"src.{destino}",
        timeout_segundos=timeout_importacion,
    )
    print(f"✅ Importación completada con estado: {import_result.state.name}")  # type: ignore

    print("🎯 Flujo maestro finalizado.")
    return {
        "source": f"{esquema}.{tabla}",
        "target": f"src.{destino}",
        "archive": nombre_zip,
        "export_state": export_result.state.name,  # type: ignore
        "import_state": import_result.state.name,  # type: ignore
    }

