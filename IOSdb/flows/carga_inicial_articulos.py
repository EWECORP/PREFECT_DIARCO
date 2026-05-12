from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from IOSdb.clients.api_client import get_api_client
from IOSdb.clients.sqlserver import open_sqlserver_connection
from IOSdb.config.settings import load_settings

QUERY_SQLSERVER_FULL = """
SELECT DISTINCT
    CAST(art.C_ARTICULO AS varchar(50))  AS id,
    CAST(art.N_ARTICULO AS varchar(255)) AS name,
    CAST(art.C_EAN      AS varchar(50))  AS barcode,
    CAST(NULL AS varchar(500))           AS photo_url,
    CASE
        WHEN art.M_VENDE_POR_PESO = 'S' THEN 'true'
        WHEN art.M_VENDE_POR_PESO = 'N' THEN 'false'
        ELSE 'false'
    END AS is_weighable,
    CASE
        WHEN art.F_ALTA = '1900-01-01' THEN NULL
        ELSE art.F_ALTA
    END AS created_at,
    CASE
        WHEN art.F_BAJA = '1900-01-01' THEN NULL
        ELSE art.F_BAJA
    END AS removed_at,
    CAST(
        CASE
            WHEN art.C_SUBRUBRO_4 IS NOT NULL AND art.C_SUBRUBRO_4 <> 0 THEN art.C_SUBRUBRO_4
            WHEN art.C_SUBRUBRO_3 IS NOT NULL AND art.C_SUBRUBRO_3 <> 0 THEN art.C_SUBRUBRO_3
            WHEN art.C_SUBRUBRO_2 IS NOT NULL AND art.C_SUBRUBRO_2 <> 0 THEN art.C_SUBRUBRO_2
            WHEN art.C_SUBRUBRO_1 IS NOT NULL AND art.C_SUBRUBRO_1 <> 0 THEN art.C_SUBRUBRO_1
            WHEN art.C_RUBRO       IS NOT NULL AND art.C_RUBRO       <> 0 THEN art.C_RUBRO
            WHEN art.C_FAMILIA     IS NOT NULL AND art.C_FAMILIA     <> 0 THEN art.C_FAMILIA
            ELSE NULL
        END AS varchar(50)
    ) AS category_id
FROM [DIARCOP001].[DiarcoP].[dbo].T050_ARTICULOS art
WHERE art.C_ARTICULO > 33
AND CAST(
        CASE
            WHEN art.C_SUBRUBRO_4 IS NOT NULL AND art.C_SUBRUBRO_4 <> 0 THEN art.C_SUBRUBRO_4
            WHEN art.C_SUBRUBRO_3 IS NOT NULL AND art.C_SUBRUBRO_3 <> 0 THEN art.C_SUBRUBRO_3
            WHEN art.C_SUBRUBRO_2 IS NOT NULL AND art.C_SUBRUBRO_2 <> 0 THEN art.C_SUBRUBRO_2
            WHEN art.C_SUBRUBRO_1 IS NOT NULL AND art.C_SUBRUBRO_1 <> 0 THEN art.C_SUBRUBRO_1
            WHEN art.C_RUBRO       IS NOT NULL AND art.C_RUBRO       <> 0 THEN art.C_RUBRO
            WHEN art.C_FAMILIA     IS NOT NULL AND art.C_FAMILIA     <> 0 THEN art.C_FAMILIA
            ELSE NULL
        END AS varchar(50)
    ) NOT IN ('2009','2011','2013','2017','2021','2023','2024')
ORDER BY id
"""


def format_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def build_payload(row: Any) -> dict[str, Any]:
    return {
        "id": str(row.id) if row.id else "0",
        "name": row.name if row.name else "",
        "barcode": row.barcode if row.barcode else "",
        "photo_url": None,
        "is_weighable": row.is_weighable == "true",
        "created_at": format_datetime(row.created_at) or "1900-01-01 00:00:00",
        "removed_at": format_datetime(row.removed_at),
        "category_id": str(row.category_id) if row.category_id else None,
    }


def _chunked(items: list[dict[str, Any]], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


@task(name="iosdb_products_initial_fetch")
def fetch_all_products() -> list[dict[str, Any]]:
    logger = get_run_logger()
    logger.info("[Productos Inicial] Consultando SQL Server...")

    conn = open_sqlserver_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(QUERY_SQLSERVER_FULL)
        rows = cursor.fetchall()
    finally:
        conn.close()

    items = [build_payload(row) for row in rows]
    logger.info("[Productos Inicial] %s productos obtenidos", len(items))
    return items


@task(name="iosdb_products_initial_send")
def send_all_products(items: list[dict[str, Any]]) -> dict[str, int]:
    logger = get_run_logger()
    settings = load_settings()
    client = get_api_client()
    failed: list[dict[str, Any]] = []
    batches = list(_chunked(items, settings.runtime.product_batch_size))

    for batch_index, batch in enumerate(batches, start=1):
        try:
            client.post_json(settings.api.products_url, batch)
            logger.info("[Productos Inicial] Batch %s/%s OK", batch_index, len(batches))
        except Exception as exc:
            logger.error("[Productos Inicial] Batch %s fallo: %s", batch_index, exc)
            response = getattr(exc, "response", None)
            if response is not None:
                logger.error("[Productos Inicial] Detalle respuesta: %s", response.text)
            failed.extend(batch)

    if failed:
        settings.runtime.logs_dir.mkdir(parents=True, exist_ok=True)
        target_path = settings.runtime.logs_dir / (
            f"failed_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with target_path.open("w", encoding="utf-8") as handle:
            json.dump(failed, handle, ensure_ascii=False, indent=2, default=str)
        logger.warning(
            "[Productos Inicial] %s productos fallidos guardados en %s",
            len(failed),
            target_path,
        )

    return {"total": len(items), "sent": len(items) - len(failed), "failed": len(failed)}


@flow(name="iosdb_products_initial_load", log_prints=True)
def initial_load_products() -> dict[str, int]:
    logger = get_run_logger()
    items = fetch_all_products()
    result = send_all_products(items)
    logger.info(
        "[Productos Inicial] Completado | Enviados: %s/%s | Fallidos: %s",
        result["sent"],
        result["total"],
        result["failed"],
    )
    return result


if __name__ == "__main__":
    initial_load_products()
