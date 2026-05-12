from __future__ import annotations

from typing import Optional

from prefect import flow, get_run_logger

from IOSdb.flows.barrio.sync_flow import sync_all as barrio_sync
from IOSdb.flows.cadena.sync_flow import sync_all as cadena_sync
from IOSdb.flows.categories_flow import sync_categories
from IOSdb.flows.carga_inicial_articulos import initial_load_products
from IOSdb.flows.mayorista.sync_flow import sync_all as mayorista_sync
from IOSdb.flows.notifications import build_summary_message, notify_discord
from IOSdb.flows.products_flow import sync_products
from IOSdb.flows.retry_stock import retry_products


@flow(name="iosdb_mayorista_with_notify", log_prints=True)
def mayorista_flow(notify: bool = True, max_workers: Optional[int] = None) -> dict[str, int]:
    result = mayorista_sync(max_workers=max_workers)
    if notify:
        notify_discord(build_summary_message("IOSdb Mayorista", result))
    return result


@flow(name="iosdb_barrio_with_notify", log_prints=True)
def barrio_flow(notify: bool = True, max_workers: Optional[int] = None) -> dict[str, int]:
    result = barrio_sync(max_workers=max_workers)
    if notify:
        notify_discord(build_summary_message("IOSdb Barrio", result))
    return result


@flow(name="iosdb_cadena_with_notify", log_prints=True)
def cadena_flow(notify: bool = True) -> dict[str, int]:
    result = cadena_sync()
    if notify:
        notify_discord(build_summary_message("IOSdb Cadena", result))
    return result


@flow(name="iosdb_products_with_notify", log_prints=True)
def products_flow(notify: bool = True) -> dict[str, int]:
    result = sync_products()
    if notify:
        notify_discord(build_summary_message("IOSdb Productos", result))
    return result


@flow(name="iosdb_categories_with_notify", log_prints=True)
def categories_flow(notify: bool = False) -> dict[str, int]:
    result = sync_categories()
    if notify:
        notify_discord(build_summary_message("IOSdb Categorias", result))
    return result


@flow(name="iosdb_retry_with_notify", log_prints=True)
def retry_flow(notify: bool = True) -> dict[str, int]:
    result = retry_products()
    if notify:
        notify_discord(build_summary_message("IOSdb Retry", result))
    return result


@flow(name="iosdb_initial_products_with_notify", log_prints=True)
def initial_products_flow(notify: bool = False) -> dict[str, int]:
    result = initial_load_products()
    if notify:
        notify_discord(build_summary_message("IOSdb Carga Inicial Productos", result))
    return result


@flow(name="iosdb_master_flow", log_prints=True)
def iosdb_master_flow(
    run_products: bool = True,
    run_mayorista: bool = True,
    run_barrio: bool = True,
    run_retry: bool = True,
    run_categories: bool = False,
    run_cadena: bool = False,
    notify: bool = True,
) -> dict[str, dict[str, int]]:
    logger = get_run_logger()
    result: dict[str, dict[str, int]] = {}

    if run_categories:
        result["categories"] = categories_flow(notify=False)
    if run_products:
        result["products"] = products_flow(notify=False)
    if run_mayorista:
        result["mayorista"] = mayorista_flow(notify=False)
    if run_barrio:
        result["barrio"] = barrio_flow(notify=False)
    if run_cadena:
        result["cadena"] = cadena_flow(notify=False)
    if run_retry:
        result["retry"] = retry_flow(notify=False)

    logger.info("[Master] Ejecucion completada | %s", result)
    if notify:
        summary = {
            key: value.get("sent", value.get("total", 0))
            for key, value in result.items()
        }
        notify_discord(build_summary_message("IOSdb Master", summary))
    return result


if __name__ == "__main__":
    iosdb_master_flow()
