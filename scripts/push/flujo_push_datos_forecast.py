from prefect import flow, task, get_run_logger
from scripts.utils.logger import setup_logger
import subprocess
import traceback
import sys
from pathlib import Path
import ast

@task(log_prints=True, retries=2, retry_delay_seconds=60)
def ejecutar_script(nombre, lista_ids):
    venv_python = Path(__file__).resolve().parents[2] / "venv" / "Scripts" / "python.exe"
    script_path = Path("scripts/push") / nombre
    argumentos = [str(venv_python), str(script_path)] + list(map(str, lista_ids))

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
def forecast_flow(lista_ids: Optional[list] = None):
    if lista_ids is None:
        lista_ids = [190, 2676, 3835, 6363, 1074, 20, 8449]  # default
    print(f"[INFO] Proveedores: {lista_ids}")
    scripts = [
        "obtener_articulos_proveedor.py",
        "obtener_oc_demoradas_proveedor.py",
        "obtener_precios_proveedor.py",
        "obtener_stock_proveedor.py",
        "obtener_ventas_proveedor.py",
        "obtener_historico_ofertas_stock.py"
    ]
    for script in scripts:
        ejecutar_script(script, lista_ids=lista_ids)

if __name__ == "__main__":
    args = sys.argv[1:]
    lista_ids = ast.literal_eval(args[0]) if args else None
    forecast_flow(lista_ids=lista_ids)
