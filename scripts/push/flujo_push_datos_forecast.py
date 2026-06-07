# flujo_push_datos_forecast.py

import os
import subprocess
from pathlib import Path
from time import perf_counter

from prefect import flow, get_run_logger, task


@task(log_prints=True, retries=2, retry_delay_seconds=60)
def ejecutar_script(nombre: str, orden: int, total: int):
    logger = get_run_logger()
    project_root = Path(__file__).resolve().parents[2]
    venv_python = project_root / "venv" / "Scripts" / "python.exe"
    script_path = Path(__file__).resolve().parent / nombre
    argumentos = [str(venv_python), "-u", str(script_path)]
    inicio = perf_counter()

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    logger.info(
        "Iniciando script %s/%s | nombre=%s | ruta=%s",
        orden,
        total,
        nombre,
        script_path,
    )

    if not venv_python.exists():
        raise FileNotFoundError(f"No se encontró el intérprete esperado: {venv_python}")

    try:
        with subprocess.Popen(
            argumentos,
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        ) as process:
            assert process.stdout is not None
            for line in process.stdout:
                mensaje = line.rstrip()
                if mensaje:
                    logger.info("[%s] %s", nombre, mensaje)

            return_code = process.wait()

        duracion = perf_counter() - inicio
        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, argumentos)

        logger.info(
            "Script finalizado OK | nombre=%s | posicion=%s/%s | duracion=%.2fs",
            nombre,
            orden,
            total,
            duracion,
        )
        return {"script": nombre, "seconds": duracion}
    except subprocess.CalledProcessError as exc:
        duracion = perf_counter() - inicio
        logger.error(
            "Script con error | nombre=%s | exit_code=%s | posicion=%s/%s | duracion=%.2fs",
            nombre,
            exc.returncode,
            orden,
            total,
            duracion,
        )
        raise Exception(
            f"[FAIL] Script con error: {nombre} | exit_code={exc.returncode} | "
            f"tiempo_transcurrido={duracion:.2f}s"
        ) from exc


@flow(name="Push Datos para FORECAST", persist_result=False)
def forecast_flow():
    logger = get_run_logger()
    started_at = perf_counter()

    scripts = [
        "obtener_base_stock.py",
        "obtener_oc_demoradas_proveedor.py",
        "obtener_base_transferencias_pendientes.py",
        "obtener_base_productos_transito.py",
        "obtener_base_productos_vigentes.py"
    ]

    logger.info("Inicio flujo forecast | scripts=%s", len(scripts))

    resultados = []
    for posicion, script in enumerate(scripts, start=1):
        resultado = ejecutar_script.submit(script, posicion, len(scripts)).result()
        resultados.append(resultado)

    duracion_total = perf_counter() - started_at
    logger.info(
        "Flujo forecast finalizado OK | scripts_ejecutados=%s | duracion_total=%.2fs",
        len(resultados),
        duracion_total,
    )


if __name__ == "__main__":
    forecast_flow()
