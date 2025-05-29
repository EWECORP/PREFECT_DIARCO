from datetime import datetime
from prefect import flow, task
from prefect.deployments import run_deployment

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

    # 2. Ejecutar flujo exportador (exportar + comprimir + enviar por SFTP)
    print(f"📤 Ejecutando flujo exportador...")
    run_deployment(
        name="exportar_tabla_sqlserver_sftp/exportar_tabla_sql_sftp",
        parameters={
            "esquema": esquema,
            "tabla": tabla,
            "filtro_sql": filtro_sql,
            "nombre_zip": nombre_zip
        }
    )

    # 3. Ejecutar flujo importador (descargar + descomprimir + cargar en PG)
    print(f"📥 Ejecutando flujo importador...")
    run_deployment(
        name="importar_csv_pg/importar_csv_pg",
        parameters={
            "esquema": esquema,
            "tabla": tabla,
            "nombre_zip": nombre_zip
        }
    )

    print("✅ Flujo maestro completado.")

if __name__ == "__main__":
    flujo_maestro(
        esquema="repl",
        tabla="T055_ARTICULOS_PARAM_STOCK",
        filtro_sql=""
    )


