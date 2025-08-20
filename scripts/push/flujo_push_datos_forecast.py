# flujo_push_datos_forecast.py

from prefect import flow, task, get_run_logger
from utils.logger import setup_logger
import subprocess
import traceback
import sys
from pathlib import Path

@task(log_prints=True, retries=2, retry_delay_seconds=60)
def ejecutar_script(nombre):
    venv_python = Path(__file__).resolve().parents[2] / "venv" / "Scripts" / "python.exe"
    script_path = Path("scripts/push") / nombre
    argumentos = [str(venv_python), str(script_path)]

    try:
        result = subprocess.run(
            argumentos,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        print(result.stdout)
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(e.stdout)
        print(e.stderr)
        raise Exception(f"[FAIL] Script con error: {nombre}")


from typing import Optional

@flow(name="Push Datos para FORECAST")
def forecast_flow():

    scripts = [
        "obtener_base_productos_vigentes.py",  ## Salida del SP_BASE_PRODUCTOS_SUCURSAL  
        "obtener_base_stock.py",  ## Salida del SP_BASE_PRODUCTOS_SUCURSAL 
        "obtener_oc_demoradas_proveedor.py"                ##  Genera Base_Forecast_Oc_Demoradas
        
    ]
    for script in scripts:
        ejecutar_script(script)

if __name__ == "__main__":
    forecast_flow()
