from prefect import flow, task, get_run_logger
from utils.logger import setup_logger
import subprocess
import traceback
import sys
import os

from pathlib import Path

# TEMPORAL hasta CONFIGURAR EL
print(f"[INFO] Python usado en esta ejecución: {sys.executable}")


# def ejecutar_script(nombre):
#     ruta = f"./scripts/pull/{nombre}"
#     print(f"▶ Ejecutando: {ruta} con {sys.executable}")
    
#     resultado = subprocess.run(
#         [sys.executable, ruta],
#         capture_output=True,
#         text=True
#     )
    
#     if resultado.returncode != 0:
#         print(f"[WARNING] Error en {nombre}:\n{resultado.stderr}")
#         raise Exception(f"Error ejecutando {nombre}")
    
#     print(f"✅ {nombre} completado:\n{resultado.stdout}")
#     return resultado.stdout

@task(log_prints=True, retries=2, retry_delay_seconds=60)
def ejecutar_script(nombre):
    # Ruta absoluta al Python del entorno virtual
    venv_python = Path(__file__).resolve().parents[2] / "venv" / "Scripts" / "python.exe"
    if not venv_python.exists():
        raise FileNotFoundError(f"Python no encontrado en: {venv_python}")

    script_path = Path("scripts/pull") / nombre

    print(f"[INFO] Ejecutando con Python: {venv_python}")
    print(f"[INFO] Ejecutando script: {script_path}")

    try:
        result = subprocess.run(
            [str(venv_python), str(script_path)],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',  # <- clave
            errors='replace'   # <- reemplaza caracteres problemáticos
        )
        print(result.stdout)
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print("[ERROR] Error durante la ejecución del script:")
        print(e.stdout)
        print(e.stderr)
        traceback.print_exc()
        raise Exception(f"Error ejecutando {nombre}")



@flow(name="Pull Datos OC PRECARGA desde CONNEXA")
def precargar_OC_connexa():
    scripts = [
        # "S90_PUBLICAR_OC_PRECARGA.py",  # Deshabilitado Luego de que THOMAS escribiera directamente en la tabla de PRECARGA_OC
        "S90_PUBLICAR_PRECARGA_CONNEXA.py",
        "S90_PUBLICAR_COMPRAS_DIRECTAS.py",
        "publicar_transferencias_sgm.py"
    ]
    for script in scripts:
        ejecutar_script(script)

if __name__ == "__main__":
    precargar_OC_connexa()
    
    
# 🔁 Patrón recomendado
# Todo script que se ejecute como subproceso debe seguir este patrón:

# resultado = subprocess.run(
#     [sys.executable, "/ruta/al/script.py"],
#     capture_output=True,
#     text=True
# )