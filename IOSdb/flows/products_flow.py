from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

from prefect import flow, get_run_logger, task

from IOSdb.clients.api_client import get_api_client
from IOSdb.clients.postgres import open_postgres_connection
from IOSdb.clients.sqlserver import open_sqlserver_connection
from IOSdb.config.settings import load_settings

QUERY_SQLSERVER_COMPARE = """
WITH ArticulosProcesados AS (
    SELECT DISTINCT
        CAST(art.C_ARTICULO AS INT) AS id,
        CONVERT(VARCHAR(10),
            CASE
                WHEN art.F_BAJA = '1900-01-01' THEN NULL
                ELSE art.F_BAJA
            END, 120) AS removed_at,
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
)
SELECT *
FROM ArticulosProcesados
WHERE category_id NOT IN ('2009','2011','2013','2017','2021','2023','2024')
ORDER BY id
"""

QUERY_PRODUCTS_DETAILS_TEMPLATE = """
SELECT DISTINCT
    CAST(art.C_ARTICULO AS varchar(50))  AS id,
    TRIM(CAST(art.N_ARTICULO AS varchar(255))) AS name,
    TRIM(CAST(art.C_EAN AS varchar(50))) AS barcode,
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
WHERE CAST(art.C_ARTICULO AS varchar(50)) IN ({placeholders})
ORDER BY id
"""

QUERY_POSTGRES = """
SELECT id::integer, TO_CHAR(removed_at, 'YYYY-MM-DD') AS removed_at, category_id
FROM public.products
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


def _chunked(items: list[str], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _chunked_payload(items: list[dict[str, Any]], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


@task(name="iosdb_products_fetch_sqlserver_compare")
def fetch_sqlserver_products() -> dict[str, dict[str, str | None]]:
    logger = get_run_logger()
    logger.info("[Productos] Consultando SQL Server para comparación...")

    conn = open_sqlserver_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(QUERY_SQLSERVER_COMPARE)
        rows = cursor.fetchall()
    finally:
        conn.close()

    products = {
        str(row.id): {
            "removed_at": row.removed_at,
            "category_id": str(row.category_id) if row.category_id else None,
        }
        for row in rows
    }
    logger.info("[Productos] %s productos en SQL Server", len(products))
    return products


@task(name="iosdb_products_fetch_postgres")
def fetch_postgres_products() -> dict[str, dict[str, str | None]]:
    logger = get_run_logger()
    logger.info("[Productos] Consultando PostgreSQL IOS...")

    conn = open_postgres_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(QUERY_POSTGRES)
        rows = cursor.fetchall()
    finally:
        conn.close()

    products = {
        str(row[0]): {
            "removed_at": row[1],
            "category_id": str(row[2]) if row[2] else None,
        }
        for row in rows
    }
    logger.info("[Productos] %s productos en PostgreSQL IOS", len(products))
    return products


@task(name="iosdb_products_compare")
def compare_products(
    sqlserver: dict[str, dict[str, str | None]],
    postgres: dict[str, dict[str, str | None]],
) -> tuple[list[str], list[str]]:
    logger = get_run_logger()
    to_insert_ids: list[str] = []
    to_update_ids: list[str] = []

    for product_id, sql_data in sqlserver.items():
        if product_id not in postgres:
            to_insert_ids.append(product_id)
            continue

        pg_data = postgres[product_id]
        if (
            sql_data["removed_at"] != pg_data["removed_at"]
            or sql_data["category_id"] != pg_data["category_id"]
        ):
            to_update_ids.append(product_id)

    logger.info(
        "[Productos] Nuevos: %s | A actualizar: %s",
        len(to_insert_ids),
        len(to_update_ids),
    )
    return to_insert_ids, to_update_ids


@task(name="iosdb_products_fetch_details")
def fetch_sqlserver_product_details(ids: list[str]) -> dict[str, dict[str, Any]]:
    logger = get_run_logger()
    if not ids:
        return {}

    logger.info("[Productos] Trayendo datos completos de %s productos...", len(ids))
    products: dict[str, dict[str, Any]] = {}

    conn = open_sqlserver_connection()
    try:
        cursor = conn.cursor()
        for id_chunk in _chunked(ids, 500):
            placeholders = ",".join("?" for _ in id_chunk)
            query = QUERY_PRODUCTS_DETAILS_TEMPLATE.format(placeholders=placeholders)
            cursor.execute(query, *id_chunk)
            for row in cursor.fetchall():
                products[str(row.id)] = build_payload(row)
    finally:
        conn.close()

    logger.info("[Productos] %s productos con datos completos obtenidos", len(products))
    return products


@task(name="iosdb_products_send")
def send_products(items: list[dict[str, Any]], label: str) -> dict[str, int]:
    logger = get_run_logger()
    settings = load_settings()
    client = get_api_client()
    failed: list[dict[str, Any]] = []
    batches = list(_chunked_payload(items, settings.runtime.product_batch_size))

    for batch_index, batch in enumerate(batches, start=1):
        try:
            client.post_json(settings.api.products_url, batch)
            logger.info("[Productos | %s] Batch %s/%s OK", label, batch_index, len(batches))
        except Exception as exc:
            logger.error("[Productos | %s] Batch %s fallo: %s", label, batch_index, exc)
            response = getattr(exc, "response", None)
            if response is not None:
                logger.error("[Productos | %s] Detalle respuesta: %s", label, response.text)
            failed.extend(batch)

    return {"total": len(items), "sent": len(items) - len(failed), "failed": len(failed)}


@flow(name="iosdb_products_sync", log_prints=True)
def sync_products() -> dict[str, int]:
    logger = get_run_logger()

    sqlserver_products = fetch_sqlserver_products()
    postgres_products = fetch_postgres_products()
    to_insert_ids, to_update_ids = compare_products(sqlserver_products, postgres_products)

    result_insert = {"total": 0, "sent": 0, "failed": 0}
    result_update = {"total": 0, "sent": 0, "failed": 0}
    all_ids = sorted(set(to_insert_ids + to_update_ids))

    if all_ids:
        full_products = fetch_sqlserver_product_details(all_ids)

        if to_insert_ids:
            insert_items = [
                full_products[product_id]
                for product_id in to_insert_ids
                if product_id in full_products
            ]
            logger.info("[Productos] Enviando %s productos nuevos...", len(insert_items))
            result_insert = send_products(insert_items, "nuevos")

        if to_update_ids:
            update_items = [
                full_products[product_id]
                for product_id in to_update_ids
                if product_id in full_products
            ]
            logger.info("[Productos] Actualizando %s productos...", len(update_items))
            result_update = send_products(update_items, "actualizados")
    else:
        logger.info("[Productos] Todo sincronizado, no hay cambios.")

    logger.info(
        "[Productos] Completado | Nuevos: %s/%s | Actualizados: %s/%s | Fallidos: %s",
        result_insert["sent"],
        result_insert["total"],
        result_update["sent"],
        result_update["total"],
        result_insert["failed"] + result_update["failed"],
    )
    return {
        "nuevos": result_insert["sent"],
        "actualizados": result_update["sent"],
        "failed": result_insert["failed"] + result_update["failed"],
    }


if __name__ == "__main__":
    sync_products()
