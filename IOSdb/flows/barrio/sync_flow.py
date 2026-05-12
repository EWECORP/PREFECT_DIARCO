from __future__ import annotations

from typing import Optional

from prefect import flow

from IOSdb.config.settings import load_settings
from IOSdb.flows.barrio.query import GET_BRANCHES, GET_STOCK
from IOSdb.flows.stock_shared import StockFlowDefinition, run_branch_stock_flow


DEFINITION = StockFlowDefinition(
    entity_key="iosdb_barrio",
    entity_label="Barrio",
    branches_query=GET_BRANCHES,
    stock_query=GET_STOCK,
    stock_url=load_settings().api.stock_url,
    failed_prefix="barrio_failed",
)


@flow(name="iosdb_barrio_sync", log_prints=True)
def sync_all(max_workers: Optional[int] = None) -> dict[str, int]:
    return run_branch_stock_flow(DEFINITION, max_workers=max_workers)


if __name__ == "__main__":
    sync_all()
