from __future__ import annotations

from typing import Optional

from prefect import flow

from IOSdb.config.settings import load_settings
from IOSdb.flows.stock_shared import StockFlowDefinition, run_branch_stock_flow
from IOSdb.flows.mayorista.query import GET_BRANCHES, GET_STOCK


DEFINITION = StockFlowDefinition(
    entity_key="iosdb_mayorista",
    entity_label="Mayorista",
    branches_query=GET_BRANCHES,
    stock_query=GET_STOCK,
    stock_url=load_settings().api.stock_url,
    failed_prefix="mayorista_failed",
)


@flow(name="iosdb_mayorista_sync", log_prints=True)
def sync_all(max_workers: Optional[int] = None) -> dict[str, int]:
    return run_branch_stock_flow(DEFINITION, max_workers=max_workers)


if __name__ == "__main__":
    sync_all()
