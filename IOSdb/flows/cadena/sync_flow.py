from __future__ import annotations

from prefect import flow

from IOSdb.config.settings import load_settings
from IOSdb.flows.cadena.query import GET_STOCK
from IOSdb.flows.stock_shared import StockFlowDefinition, run_chain_stock_flow


DEFINITION = StockFlowDefinition(
    entity_key="iosdb_cadena",
    entity_label="Cadena",
    branches_query=None,
    stock_query=GET_STOCK,
    stock_url=load_settings().api.stock_url,
    failed_prefix="cadena_failed",
)


@flow(name="iosdb_cadena_sync", log_prints=True)
def sync_all() -> dict[str, int]:
    return run_chain_stock_flow(DEFINITION)


if __name__ == "__main__":
    sync_all()
