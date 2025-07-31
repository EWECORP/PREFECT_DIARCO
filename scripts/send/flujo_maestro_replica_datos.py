# flujo_maestro_replica_datos.py

from datetime import datetime
from prefect import flow, task
from prefect.deployments import run_deployment
import os
import time

@task
def generar_nombre_archivo(esquema: str, tabla: str) -> str:
    fecha = datetime.today().strftime("%Y%m%d_%H%M%S")
    return f"{esquema}_{tabla}_{fecha}.zip"

@task(retries=0)
def esperar_archivo_disponible(nombre_zip: str, carpeta_destino: str = "/sftp/archivos/usr_diarco/orquestador", espera_maxima: int = 180, intervalo: int = 10):
    """
    Espera activa hasta que el archivo ZIP esté disponible, o se alcance el timeout.
    """
    ruta_completa = os.path.join(carpeta_destino, nombre_zip)
    tiempo_esperado = 0

    print(f"🕒 Esperando que aparezca el archivo: {ruta_completa}")

    while not os.path.exists(ruta_completa):
        if tiempo_esperado >= espera_maxima:
            raise FileNotFoundError(f"❌ Timeout de espera alcanzado. Archivo no encontrado: {ruta_completa}")
        print(f"⏳ Espera acumulada: {tiempo_esperado}s - Archivo aún no disponible...")
        time.sleep(intervalo)
        tiempo_esperado += intervalo

    print(f"✅ Archivo encontrado: {ruta_completa}")
    return ruta_completa

@flow(name="flujo_maestro_replica_datos")
def flujo_maestro(esquema: str, tabla: str, filtro_sql: str):
    print(f"🚀 Iniciando replicación para {esquema}.{tabla}")

    # 1. Generar nombre de archivo ZIP
    nombre_zip = generar_nombre_archivo(esquema, tabla)
    print(f"📦 Nombre de archivo generado: {nombre_zip}")

    # 2. Ejecutar flujo exportador (esperar a que termine)
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

    # 3. Verificar disponibilidad del archivo ZIP
    print(f"🔍 Verificando existencia del archivo transferido...")
    esperar_archivo_disponible(nombre_zip)

    # 4. Ejecutar flujo importador
    print(f"📥 Ejecutando flujo importador...")
    import_result = run_deployment(
        name="importar_csv_pg/importar_csv_pg",
        parameters={
            "esquema": "src",  # Esquema de destino en PostgreSQL
            "tabla": tabla,
            "nombre_zip": nombre_zip
        },
        timeout=600
    )
    print(f"✅ Importación completada con estado: {import_result.state.name}")  # type: ignore

    print("🎯 Flujo maestro finalizado.")

