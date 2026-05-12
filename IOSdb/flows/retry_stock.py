from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from IOSdb.clients.api_client import get_api_client
from IOSdb.config.settings import load_settings


def _find_failed_files(logs_dir: Path) -> list[Path]:
    if not logs_dir.exists():
        return []

    prefixes = ("mayorista_failed_", "barrio_failed_", "cadena_failed_")
    return sorted(
        path
        for path in logs_dir.iterdir()
        if path.is_file() and path.name.startswith(prefixes) and path.suffix == ".json"
    )


@task(name="iosdb_retry_find_failed_files")
def find_failed_files() -> list[str]:
    logger = get_run_logger()
    logs_dir = load_settings().runtime.logs_dir
    files = [str(path) for path in _find_failed_files(logs_dir)]
    logger.info("[Retry] %s archivos encontrados para reintentar", len(files))
    return files


@task(name="iosdb_retry_send_file")
def retry_file(filepath: str) -> bool:
    logger = get_run_logger()
    client = get_api_client()
    target_path = Path(filepath)

    with target_path.open("r", encoding="utf-8") as handle:
        payload: list[dict[str, Any]] = json.load(handle)

    try:
        client.post_json(load_settings().api.stock_url, payload)
        logger.info("[Retry] %s enviado OK", filepath)
        target_path.unlink(missing_ok=True)
        logger.info("[Retry] %s eliminado", filepath)
        return True
    except Exception as exc:
        logger.error("[Retry] %s fallo: %s", filepath, exc)
        response = getattr(exc, "response", None)
        if response is not None:
            logger.error("[Retry] Detalle respuesta: %s", response.text)
        return False


@flow(name="iosdb_retry_products", log_prints=True)
def retry_products() -> dict[str, int]:
    logger = get_run_logger()
    files = find_failed_files()

    if not files:
        logger.info("[Retry] No hay archivos pendientes")
        return {"files": 0, "sent": 0, "failed": 0}

    results = [retry_file(path) for path in files]
    sent = sum(1 for result in results if result)
    failed = sum(1 for result in results if not result)
    logger.info("[Retry] Completado | Enviados: %s | Fallidos: %s", sent, failed)
    return {"files": len(files), "sent": sent, "failed": failed}


if __name__ == "__main__":
    retry_products()
