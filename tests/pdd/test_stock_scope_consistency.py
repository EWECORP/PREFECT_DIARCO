from pathlib import Path


def test_stock_snapshot_and_assortment_use_same_branch_exclusion_policy() -> None:
    root = Path(__file__).resolve().parents[2]
    stock_sql = (root / "scripts/sql/sp_SP_BASE_STOCK_EXTEND.sql").read_text(
        encoding="utf-8"
    ).upper()
    assortment = (root / "scripts/push/obtener_base_productos_vigentes.py").read_text(
        encoding="utf-8"
    ).upper()

    assert "FROM DBO.SUCURSALES_EXCLUIDAS" in stock_sql
    assert "FROM DBO.SUCURSALES_EXCLUIDAS" in assortment
    assert "FROM REPL.SUCURSALES_EXCLUIDAS" not in stock_sql
