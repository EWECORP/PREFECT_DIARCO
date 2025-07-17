from datetime import datetime
from prefect import flow, task
from prefect.deployments import run_deployment
import time  # <-- se importa para poder usar sleep

@task
def generar_nombre_archivo(esquema: str, tabla: str) -> str:
    fecha = datetime.today().strftime("%Y%m%d_%H%M%S")
    return f"{esquema}_{tabla}_{fecha}.zip"

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
        timeout=600  # opcional, 10 minutos
    )
    print(f"✅ Exportación completada con estado: {export_result.state.name}") # type: ignore

    # 3. Pausa para dar tiempo al SFTP
    pausa_entre_flujos = 30  # segundos
    print(f"⏳ Esperando {pausa_entre_flujos} segundos antes de iniciar la importación...")
    time.sleep(pausa_entre_flujos)

    # 4. Ejecutar flujo importador solo después
    print(f"📥 Ejecutando flujo importador...")
    import_result = run_deployment(
        name="importar_csv_pg/importar_csv_pg",
        parameters={
            "esquema": "src",  # En postgres, el esquema de destino es 'src'
            "tabla": tabla,
            "nombre_zip": nombre_zip
        },
        timeout=600  # opcional
    )
    print(f"✅ Importación completada con estado: {import_result.state.name}") # type: ignore

    print("🎯 Flujo maestro finalizado.")


