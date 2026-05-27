# flujo_refresh_datos_forecast.py

from prefect import flow, task, get_run_logger
from utils.logger import setup_logger
import subprocess
import sys
from pathlib import Path
from time import perf_counter

@task(log_prints=True, retries=2, retry_delay_seconds=60)
def ejecutar_script(nombre):
    logger = get_run_logger()
    venv_python = Path(__file__).resolve().parents[2] / "venv" / "Scripts" / "python.exe"
    script_path = Path("scripts/push") / nombre
    argumentos = [str(venv_python), str(script_path)]
    inicio = perf_counter()

    logger.info(f"Lanzando script: {nombre}")
    logger.info(f"Ruta script: {script_path}")

    try:
        result = subprocess.run(
            argumentos,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        duracion = perf_counter() - inicio
        logger.info(f"Script finalizado OK: {nombre} | tiempo_utilizado={duracion:.2f}s")

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
    except subprocess.CalledProcessError as e:
        duracion = perf_counter() - inicio
        logger.error(
            f"Script con error: {nombre} | exit_code={e.returncode} | "
            f"tiempo_transcurrido={duracion:.2f}s"
        )
        if e.stdout:
            print(e.stdout)
        if e.stderr:
            print(e.stderr)
        raise Exception(
            f"[FAIL] Script con error: {nombre} | exit_code={e.returncode} | "
            f"tiempo_transcurrido={duracion:.2f}s"
        ) from e

@flow(name="Refrescar Datos para FORECAST")
def forecast_flow():
    logger = get_run_logger()

    scripts = [
        "obtener_base_productos_vigentes.py",  ## Salida del SP_BASE_PRODUCTOS_SUCURSAL  
        "obtener_base_stock.py",  ## Salida del SP_BASE_PRODUCTOS_SUCURSAL 
        "obtener_oc_demoradas_proveedor.py" ,               ##  Genera Base_Forecast_Oc_Demoradas
        "obtener_base_transferencias_pendientes.py",        ##  Genera Base_Transferencias_Pendientes
        "obtener_base_productos_transito.py"        ##  Genera Base_Productos_En_Transito
    ]

    logger.info(f"Inicio flujo forecast. Scripts a ejecutar: {len(scripts)}")
    for script in scripts:
        ejecutar_script(script)
    logger.info("Flujo forecast finalizado. Todos los scripts terminaron correctamente.")

if __name__ == "__main__":
    forecast_flow()
