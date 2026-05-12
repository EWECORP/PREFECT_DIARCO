from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task

from IOSdb.clients.api_client import get_api_client
from IOSdb.clients.sqlserver import open_sqlserver_connection
from IOSdb.config.settings import load_settings

QUERY = """
WITH Jerarquia AS (
    SELECT
        C_RUBRO,
        LTRIM(RTRIM(D_RUBRO)) AS D_RUBRO,
        C_RUBRO_PADRE,
        CAST(C_RUBRO AS VARCHAR(255)) AS ext_code,
        F_ALTA,
        M_BAJA,
        C_RUBRO_NIVEL AS nivel,
        0 AS depth
    FROM DIARCOP001.DiarcoP.dbo.T114_RUBROS
    WHERE C_RUBRO_PADRE = 0 OR C_RUBRO_PADRE IS NULL
    UNION ALL
    SELECT
        h.C_RUBRO,
        LTRIM(RTRIM(h.D_RUBRO)) AS D_RUBRO,
        h.C_RUBRO_PADRE,
        CAST(p.ext_code + '-' + CAST(h.C_RUBRO AS VARCHAR(255)) AS VARCHAR(255)) AS ext_code,
        h.F_ALTA,
        h.M_BAJA,
        h.C_RUBRO_NIVEL AS nivel,
        p.depth + 1 AS depth
    FROM DIARCOP001.DiarcoP.dbo.T114_RUBROS h
    JOIN Jerarquia p ON h.C_RUBRO_PADRE = p.C_RUBRO
)
SELECT
    C_RUBRO AS id,
    D_RUBRO AS name,
    C_RUBRO_PADRE AS parent_code,
    nivel,
    depth
FROM Jerarquia
WHERE nivel NOT IN (1)
ORDER BY depth ASC, ext_code ASC
"""


def build_payload(row: Any) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "name": row.name if row.name else "",
        "path": None,
        "parent_id": str(row.parent_code) if row.parent_code else "0",
    }


def _chunked(items: list[dict[str, Any]], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


@task(name="iosdb_categories_fetch")
def fetch_categories() -> list[dict[str, Any]]:
    logger = get_run_logger()
    logger.info("[Categorias] Consultando SQL...")

    conn = open_sqlserver_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(QUERY)
        rows = cursor.fetchall()
    finally:
        conn.close()

    items = [build_payload(row) for row in rows]
    logger.info("[Categorias] %s categorias obtenidas", len(items))
    return items


@task(name="iosdb_categories_send")
def send_categories(items: list[dict[str, Any]]) -> dict[str, int]:
    logger = get_run_logger()
    settings = load_settings()
    client = get_api_client()
    failed: list[dict[str, Any]] = []
    batches = list(_chunked(items, settings.runtime.category_batch_size))

    for batch_index, batch in enumerate(batches, start=1):
        try:
            client.post_json(settings.api.categories_url, batch)
            logger.info("[Categorias] Batch %s/%s OK", batch_index, len(batches))
        except Exception as exc:
            logger.error("[Categorias] Batch %s fallo: %s", batch_index, exc)
            response = getattr(exc, "response", None)
            if response is not None:
                logger.error("[Categorias] Detalle respuesta: %s", response.text)
            failed.extend(batch)

    return {"total": len(items), "sent": len(items) - len(failed), "failed": len(failed)}


@flow(name="iosdb_categories_sync", log_prints=True)
def sync_categories() -> dict[str, int]:
    logger = get_run_logger()
    items = fetch_categories()
    result = send_categories(items)
    logger.info(
        "[Categorias] Completado | Total: %s | Enviados: %s | Fallidos: %s",
        result["total"],
        result["sent"],
        result["failed"],
    )
    return result


if __name__ == "__main__":
    sync_categories()
