from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from prefect import flow, get_run_logger, task

from IOSdb.clients.api_client import get_api_client
from IOSdb.clients.sqlserver import open_sqlserver_connection
from IOSdb.config.settings import load_settings


@dataclass(frozen=True)
class StockFlowDefinition:
    entity_key: str
    entity_label: str
    branches_query: Optional[str]
    stock_query: str
    stock_url: str
    failed_prefix: str


def format_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    return str(value)


def build_stock_payload(row: Any) -> dict[str, Any]:
    tags_raw = row.tags if hasattr(row, "tags") and row.tags else ""
    tags_array = [item.strip() for item in tags_raw.strip("[]").split(",") if item.strip()]

    return {
        "product_id": str(row.id) if row.id else "0",
        "last_sale_date": format_date(row.last_sale_date),
        "first_sale_date": format_date(row.first_sale_date),
        "last_restock_date": format_date(row.last_restock_date),
        "stock": float(row.stock) if row.stock else 0.0,
        "price": float(row.price) if row.price else 0.0,
        "price_with_tax": float(row.price_with_tax) if row.price_with_tax else 0.0,
        "price_retail": float(row.price_retail) if row.price_retail else 0.0,
        "price_retail_with_tax": (
            float(row.price_retail_with_tax) if row.price_retail_with_tax else 0.0
        ),
        "total_sales": 0,
        "last_stock_date": format_date(row.last_sale_date),
        "out_of_stock": 0,
        "days_of_stock": int(row.days_of_stock) if row.days_of_stock is not None else 0,
        "days_of_stock_pending": (
            int(row.days_of_stock_pending)
            if row.days_of_stock_pending is not None
            else 0
        ),
        "recommended_days_of_stock": (
            int(row.recommended_days_of_stock)
            if row.recommended_days_of_stock is not None
            else 0
        ),
        "tags": tags_array,
        "units": int(row.units) if row.units is not None else 0,
        "units_weighted": (
            float(row.units_weighted) if row.units_weighted is not None else 0.0
        ),
        "provider": {
            "id": str(row.supplier_code) if row.supplier_code else "0",
            "name": row.supplier_name if row.supplier_name else "Sin Nombre",
        },
    }


def _chunked(items: list[dict[str, Any]], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _failed_file_path(prefix: str, suffix: str) -> Path:
    settings = load_settings()
    settings.runtime.logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return settings.runtime.logs_dir / f"{prefix}_{suffix}_{timestamp}.json"


def _write_failed_payload(path: Path, payload: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


@task
def fetch_branches_task(branches_query: str, entity_label: str) -> list[tuple[int, str]]:
    logger = get_run_logger()
    logger.info("[%s] Consultando sucursales...", entity_label)

    conn = open_sqlserver_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(branches_query)
        rows = cursor.fetchall()
        branches = [(int(row.C_SUCU_EMPR), row.N_SUCURSAL) for row in rows]
    finally:
        conn.close()

    logger.info("[%s] %s sucursales encontradas", entity_label, len(branches))
    return branches


@task(retries=2, retry_delay_seconds=30)
def process_branch_stock_task(
    definition: StockFlowDefinition,
    branch_id: int,
    branch_name: str,
) -> dict[str, Any]:
    logger = get_run_logger()
    settings = load_settings()

    logger.info("[%s | %s] Consultando SQL...", definition.entity_label, branch_name)
    conn = open_sqlserver_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(definition.stock_query, branch_id, branch_id, branch_id, branch_id)
        rows = cursor.fetchall()
    finally:
        conn.close()

    items = [build_stock_payload(row) for row in rows]
    logger.info(
        "[%s | %s] %s items obtenidos",
        definition.entity_label,
        branch_name,
        len(items),
    )

    failed: list[dict[str, Any]] = []
    api_client = get_api_client()
    batches = list(_chunked(items, settings.runtime.batch_size))

    for batch_index, batch in enumerate(batches, start=1):
        payload = [
            {
                "branch_office": {
                    "id": str(branch_id),
                    "name": branch_name.strip() if branch_name else "",
                },
                "products": batch,
            }
        ]
        try:
            api_client.post_json(definition.stock_url, payload)
            logger.info(
                "[%s | %s] Batch %s/%s OK",
                definition.entity_label,
                branch_name,
                batch_index,
                len(batches),
            )
        except Exception as exc:
            logger.warning(
                "[%s | %s] Batch %s fallo, reintentando 1x1...",
                definition.entity_label,
                branch_name,
                batch_index,
            )
            for item in batch:
                single_payload = [
                    {
                        "branch_office": {
                            "id": str(branch_id),
                            "name": branch_name.strip() if branch_name else "",
                        },
                        "products": [item],
                    }
                ]
                try:
                    api_client.post_json(definition.stock_url, single_payload)
                except Exception as single_exc:
                    logger.error(
                        "[%s | %s] Producto %s fallo: %s",
                        definition.entity_label,
                        branch_name,
                        item.get("product_id", "?"),
                        single_exc,
                    )
                    response = getattr(single_exc, "response", None)
                    if response is not None:
                        logger.error(
                            "[%s | %s] Detalle respuesta: %s",
                            definition.entity_label,
                            branch_name,
                            response.text,
                        )
                    failed.append(item)
            logger.debug(
                "[%s | %s] Error batch original: %s",
                definition.entity_label,
                branch_name,
                exc,
            )

    if failed:
        failed_payload = [
            {
                "branch_office": {
                    "id": str(branch_id),
                    "name": branch_name.strip() if branch_name else "",
                },
                "products": failed,
            }
        ]
        failed_path = _failed_file_path(definition.failed_prefix, str(branch_id))
        _write_failed_payload(failed_path, failed_payload)
        logger.warning(
            "[%s | %s] %s productos fallidos guardados en %s",
            definition.entity_label,
            branch_name,
            len(failed),
            failed_path,
        )

    return {
        "branch_id": branch_id,
        "branch_name": branch_name,
        "total": len(items),
        "sent": len(items) - len(failed),
        "failed": len(failed),
    }


@task(retries=2, retry_delay_seconds=30)
def process_chain_stock_task(definition: StockFlowDefinition) -> dict[str, Any]:
    logger = get_run_logger()
    settings = load_settings()

    logger.info("[%s] Consultando SQL...", definition.entity_label)
    conn = open_sqlserver_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(definition.stock_query)
        rows = cursor.fetchall()
    finally:
        conn.close()

    items = [build_stock_payload(row) for row in rows]
    logger.info("[%s] %s items obtenidos", definition.entity_label, len(items))

    api_client = get_api_client()
    failed: list[dict[str, Any]] = []
    batches = list(_chunked(items, settings.runtime.batch_size))

    for batch_index, batch in enumerate(batches, start=1):
        try:
            api_client.post_json(definition.stock_url, batch)
            logger.info(
                "[%s] Batch %s/%s OK",
                definition.entity_label,
                batch_index,
                len(batches),
            )
        except Exception as exc:
            logger.error(
                "[%s] Batch %s fallo: %s",
                definition.entity_label,
                batch_index,
                exc,
            )
            response = getattr(exc, "response", None)
            if response is not None:
                logger.error("[%s] Detalle respuesta: %s", definition.entity_label, response.text)
            failed.extend(batch)

    if failed:
        failed_path = _failed_file_path(definition.failed_prefix, "chain")
        _write_failed_payload(failed_path, failed)
        logger.warning(
            "[%s] %s productos fallidos guardados en %s",
            definition.entity_label,
            len(failed),
            failed_path,
        )

    return {
        "total": len(items),
        "sent": len(items) - len(failed),
        "failed": len(failed),
    }


def run_branch_stock_flow(definition: StockFlowDefinition, max_workers: Optional[int] = None) -> dict[str, int]:
    logger = get_run_logger()
    settings = load_settings()

    branches = fetch_branches_task.with_options(
        name=f"{definition.entity_key}_get_branches"
    )(definition.branches_query or "", definition.entity_label)

    futures = [
        process_branch_stock_task.with_options(
            name=f"{definition.entity_key}_process_branch"
        ).submit(definition, branch_id, branch_name)
        for branch_id, branch_name in branches
    ]

    # Fuerza la materializacion de los resultados para que el flujo falle si falla una rama.
    results = [future.result() for future in futures]

    total = sum(result["total"] for result in results)
    sent = sum(result["sent"] for result in results)
    failed = sum(result["failed"] for result in results)

    logger.info(
        "[%s] Completado | Total: %s | Enviados: %s | Fallidos: %s | Max workers config: %s",
        definition.entity_label,
        total,
        sent,
        failed,
        max_workers or settings.runtime.max_workers,
    )
    return {"total": total, "sent": sent, "failed": failed}


def run_chain_stock_flow(definition: StockFlowDefinition) -> dict[str, int]:
    logger = get_run_logger()
    result = process_chain_stock_task.with_options(
        name=f"{definition.entity_key}_process_chain"
    )(definition)
    logger.info(
        "[%s] Completado | Total: %s | Enviados: %s | Fallidos: %s",
        definition.entity_label,
        result["total"],
        result["sent"],
        result["failed"],
    )
    return result
