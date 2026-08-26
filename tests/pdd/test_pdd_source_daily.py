from __future__ import annotations

import unittest
import importlib
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

from scripts.pdd.pdd_source_daily import evaluate_source_state
from scripts.send.actualizar_bases_ventas import (
    find_mismatched_dates,
    inclusive_dates,
    replica_refresh_start,
    rolling_dates,
)


ROOT = Path(__file__).resolve().parents[2]


def ready_state() -> dict:
    return {
        "raw_diarco_date": date(2026, 8, 21),
        "raw_diarco_rows": 80,
        "raw_barrio_date": date(2026, 8, 21),
        "raw_barrio_rows": 20,
        "raw_sales_date": date(2026, 8, 21),
        "raw_sales_rows": 100,
        "enriched_sales_date": date(2026, 8, 21),
        "enriched_sales_rows": 100,
        "historical_stock_date": date(2026, 8, 21),
        "historical_stock_rows": 50,
        "historical_stock_as_of_ts": datetime(2026, 8, 22, tzinfo=timezone.utc),
        # La foto extraída en D representa el stock al cierre de D-1.
        "branch_stock_date": date(2026, 8, 21),
        "branch_stock_rows": 500,
        "branch_stock_as_of_ts": datetime(2026, 8, 22, tzinfo=timezone.utc),
        "branch_stock_nulls": 0,
        "assortment_rows": 600,
        "assortment_as_of_ts": datetime(2026, 8, 22, tzinfo=timezone.utc),
        "assortment_cd_purchase_articles": 2500,
        "logistics_rows": 13000,
        "logistics_as_of_ts": datetime(2026, 8, 21, tzinfo=timezone.utc),
        "logistics_missing_weight": 13000,
        "logistics_missing_volume": 13000,
        "logistics_missing_pallet": 125,
        "logistics_invalid_quality": 0,
        "open_po_rows": 4000,
        "open_po_as_of_ts": datetime(2026, 8, 21, tzinfo=timezone.utc),
        "open_po_refresh_at": None,
        "open_po_positive_lines": 3992,
        "open_po_negative_lines": 8,
        "article_master_rows": 13000,
        "category_master_rows": 1000,
        "excluded_branch_rows": 89,
    }


class EvaluateSourceStateTests(unittest.TestCase):
    def test_complete_cutoff_is_ready_despite_known_logistics_gaps(self) -> None:
        observed_at = datetime(2026, 8, 22, 20, tzinfo=timezone.utc)
        result = evaluate_source_state(
            ready_state(),
            date(2026, 8, 22),
            {
                "ASSORTMENT": observed_at,
                "PRODUCT_LOGISTICS": observed_at,
                "OPEN_PURCHASE_ORDERS": observed_at,
            },
        )

        self.assertEqual(result.status, "READY")
        self.assertEqual(result.common_closed_date, date(2026, 8, 21))
        self.assertEqual(result.recommended_business_date, date(2026, 8, 22))
        self.assertFalse(result.blocker_codes)
        self.assertIn("PRODUCT_LOGISTICS:WEIGHT_INCOMPLETE", result.warning_codes)
        self.assertIn("OPEN_PURCHASE_ORDERS:NEGATIVE_LINES_EXCLUDED", result.warning_codes)

    def test_enriched_sales_behind_cutoff_blocks(self) -> None:
        state = ready_state()
        state["enriched_sales_date"] = date(2026, 8, 20)

        result = evaluate_source_state(
            state,
            date(2026, 8, 22),
            {"OPEN_PURCHASE_ORDERS": datetime(2026, 8, 22, tzinfo=timezone.utc)},
        )

        self.assertEqual(result.status, "BLOCKED")
        self.assertIn("ENRICHED_SALES:SOURCE_DATE_BEHIND", result.blocker_codes)
        self.assertEqual(result.recommended_business_date, date(2026, 8, 21))

    def test_materialized_view_refresh_evidence_is_authoritative(self) -> None:
        state = ready_state()
        without_refresh = evaluate_source_state(state, date(2026, 8, 22), {})
        with_refresh = evaluate_source_state(
            state,
            date(2026, 8, 22),
            {
                "OPEN_PURCHASE_ORDERS": datetime(
                    2026, 8, 22, 18, tzinfo=timezone.utc
                )
            },
        )

        self.assertIn(
            "OPEN_PURCHASE_ORDERS:REFRESH_NOT_PROVEN",
            without_refresh.blocker_codes,
        )
        self.assertNotIn(
            "OPEN_PURCHASE_ORDERS:REFRESH_NOT_PROVEN",
            with_refresh.blocker_codes,
        )
        self.assertNotIn(
            "OPEN_PURCHASE_ORDERS:REFRESH_DATE_BEHIND",
            with_refresh.blocker_codes,
        )

    def test_prior_audited_materialized_view_refresh_is_accepted(self) -> None:
        state = ready_state()
        state["open_po_refresh_at"] = datetime(
            2026, 8, 22, 1, tzinfo=timezone.utc
        )

        result = evaluate_source_state(state, date(2026, 8, 22), {})

        self.assertNotIn(
            "OPEN_PURCHASE_ORDERS:REFRESH_DATE_BEHIND",
            result.blocker_codes,
        )

    def test_source_extraction_timestamp_does_not_prove_view_refresh(self) -> None:
        state = ready_state()
        state["open_po_as_of_ts"] = datetime(
            2026, 8, 22, 23, tzinfo=timezone.utc
        )

        result = evaluate_source_state(state, date(2026, 8, 22), {})

        self.assertIn(
            "OPEN_PURCHASE_ORDERS:REFRESH_NOT_PROVEN",
            result.blocker_codes,
        )
        self.assertNotIn(
            "OPEN_PURCHASE_ORDERS:SOURCE_EMPTY",
            result.blocker_codes,
        )

    def test_null_branch_stock_is_a_hard_blocker(self) -> None:
        state = ready_state()
        state["branch_stock_nulls"] = 3
        result = evaluate_source_state(
            state,
            date(2026, 8, 22),
            {"OPEN_PURCHASE_ORDERS": datetime(2026, 8, 22, tzinfo=timezone.utc)},
        )

        self.assertIn("BRANCH_STOCK:NULL_PHYSICAL_STOCK", result.blocker_codes)

    def test_branch_stock_from_previous_close_extracted_today_is_ready(self) -> None:
        result = evaluate_source_state(
            ready_state(),
            date(2026, 8, 22),
            {"OPEN_PURCHASE_ORDERS": datetime(2026, 8, 22, tzinfo=timezone.utc)},
        )

        self.assertNotIn("BRANCH_STOCK:SOURCE_DATE_BEHIND", result.blocker_codes)
        self.assertNotIn(
            "BRANCH_STOCK:REFRESH_NOT_PROVEN_FOR_BUSINESS_DATE",
            result.blocker_codes,
        )

    def test_branch_stock_two_closes_behind_is_blocked(self) -> None:
        state = ready_state()
        state["branch_stock_date"] = date(2026, 8, 20)

        result = evaluate_source_state(
            state,
            date(2026, 8, 22),
            {"OPEN_PURCHASE_ORDERS": datetime(2026, 8, 22, tzinfo=timezone.utc)},
        )

        self.assertIn("BRANCH_STOCK:SOURCE_DATE_BEHIND", result.blocker_codes)

    def test_branch_stock_without_current_extraction_evidence_is_blocked(self) -> None:
        state = ready_state()
        state["branch_stock_as_of_ts"] = datetime(
            2026, 8, 21, 23, 59, tzinfo=timezone.utc
        )

        result = evaluate_source_state(
            state,
            date(2026, 8, 22),
            {"OPEN_PURCHASE_ORDERS": datetime(2026, 8, 22, tzinfo=timezone.utc)},
        )

        self.assertIn(
            "BRANCH_STOCK:REFRESH_NOT_PROVEN_FOR_BUSINESS_DATE",
            result.blocker_codes,
        )

    def test_missing_branch_stock_remains_blocked(self) -> None:
        state = ready_state()
        state["branch_stock_date"] = None
        state["branch_stock_rows"] = 0
        state["branch_stock_as_of_ts"] = None

        result = evaluate_source_state(
            state,
            date(2026, 8, 22),
            {"OPEN_PURCHASE_ORDERS": datetime(2026, 8, 22, tzinfo=timezone.utc)},
        )

        self.assertIn("BRANCH_STOCK:SOURCE_EMPTY", result.blocker_codes)


class SourceSyncContractTests(unittest.TestCase):
    def test_daily_source_modules_import_from_project_root(self) -> None:
        modules = (
            "scripts.send.actualizar_tablas_tabulares",
            "scripts.send.actualizar_bases_ventas",
            "scripts.push.obtener_base_stock",
            "scripts.push.obtener_base_productos_vigentes",
            "scripts.pdd.cargar_base_articulos_logistica",
        )

        for module_name in modules:
            with self.subTest(module=module_name):
                importlib.import_module(module_name)

    def test_audit_migration_defines_run_and_detail(self) -> None:
        ddl = (
            ROOT / "scripts" / "sql" / "pdd" / "002_create_pdd_source_sync_audit.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE audit.pdd_source_sync_run", ddl)
        self.assertIn("CREATE TABLE audit.pdd_source_sync_detail", ddl)
        self.assertIn("FOREIGN KEY (source_sync_run_uuid)", ddl)

    def test_tabular_refresh_waits_for_delete_and_returns_results(self) -> None:
        source = (
            ROOT / "scripts" / "send" / "actualizar_tablas_tabulares.py"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "vaciar_registros_tabla.submit(tabla_pg, valor_filtro).result()",
            source,
        )
        self.assertIn("return resultados", source)

    def test_month_boundary_uses_cutoff_month_for_enrichment(self) -> None:
        source = (ROOT / "scripts" / "pdd" / "pdd_source_daily.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("enriched_start = cutoff_date.replace(day=1)", source)
        self.assertIn("fecha_desde=enriched_start", source)
        self.assertIn("fecha_hasta=business_date", source)

    def test_historical_repair_rebuilds_from_month_start(self) -> None:
        source = (ROOT / "scripts" / "pdd" / "pdd_source_daily.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("oldest_repaired.replace(day=1)", source)

    def test_monthly_baseline_procedure_is_range_safe(self) -> None:
        ddl = (
            ROOT / "scripts" / "datamart" / "sp_procesar_promos_mes.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("v_desde_efectivo := date_trunc('month', p_desde)::date", ddl)
        self.assertIn("v_mes_hasta := date_trunc('month', p_hasta - 1)::date", ddl)
        self.assertIn("WHERE mes >= v_desde_efectivo", ddl)
        self.assertIn("AND mes <= v_mes_hasta", ddl)
        self.assertIn("promo_fuerte IS DISTINCT FROM false", ddl)
        self.assertIn("v.promo_fuerte IS DISTINCT FROM true", ddl)
        self.assertGreaterEqual(ddl.count("b.unidades_mediana > 0"), 6)
        self.assertIn("RAISE LOG '[PDD_BVE]", ddl)

    def test_enriched_sales_connection_uses_keepalives(self) -> None:
        source = (
            ROOT / "scripts" / "pull" / "flujo_procesar_promos_bve.py"
        ).read_text(encoding="utf-8")

        self.assertIn('"application_name": "etl_diarco_bve_promos"', source)
        self.assertIn('"keepalives_idle": 60', source)
        self.assertIn('"keepalives_interval": 30', source)
        self.assertIn('"keepalives_count": 5', source)

    def test_advisory_unlock_cannot_mask_pipeline_error(self) -> None:
        source = (ROOT / "scripts" / "pdd" / "pdd_source_daily.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("not lock_connection.invalidated", source)
        self.assertIn("No se pudo liberar explicitamente el lock PDD", source)

    def test_source_daily_connection_uses_keepalives(self) -> None:
        source = (ROOT / "scripts" / "pdd" / "pdd_source_daily.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('"application_name": "etl_diarco:pdd_source_daily"', source)
        self.assertIn('"keepalives_idle": 60', source)
        self.assertIn('"keepalives_interval": 30', source)
        self.assertIn('"keepalives_count": 5', source)
        self.assertIn("lock_connection.commit()", source)

    def test_daily_master_runs_two_hours_before_operational_pdd(self) -> None:
        config = yaml.safe_load((ROOT / "prefect.yaml").read_text(encoding="utf-8"))
        daily = next(
            item
            for item in config["deployments"]
            if item["name"] == "PDD_SOURCE_DAILY_MASTER_PROD"
        )
        readiness = next(
            item
            for item in config["deployments"]
            if item["name"] == "PDD_SOURCE_READINESS_MANUAL"
        )

        self.assertEqual(daily["schedule"]["cron"], "30 18 * * *")
        self.assertEqual(
            daily["schedule"]["timezone"],
            "America/Argentina/Buenos_Aires",
        )
        self.assertTrue(daily["parameters"]["fail_if_not_ready"])
        self.assertEqual(readiness["schedules"], [])
        self.assertFalse(readiness["parameters"]["fail_if_not_ready"])

    def test_enriched_sales_reprocess_is_manual(self) -> None:
        config = yaml.safe_load((ROOT / "prefect.yaml").read_text(encoding="utf-8"))
        deployment = next(
            item
            for item in config["deployments"]
            if item["name"] == "PDD_ENRICHED_SALES_REPROCESS_MANUAL"
        )

        self.assertEqual(deployment["schedules"], [])
        self.assertTrue(deployment["parameters"]["actualizar_base_original"])

    def test_sales_overlap_contains_exactly_three_dates(self) -> None:
        self.assertEqual(
            rolling_dates(date(2026, 8, 21), 3),
            [date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21)],
        )

    def test_sql_replica_window_includes_three_old_days_and_new_days(self) -> None:
        previous_max = date(2026, 8, 20)
        current_max = date(2026, 8, 21)
        refresh_from = replica_refresh_start(previous_max, "DIARCO")

        self.assertEqual(refresh_from, date(2026, 8, 18))
        self.assertEqual(
            inclusive_dates(refresh_from, current_max),
            [
                date(2026, 8, 18),
                date(2026, 8, 19),
                date(2026, 8, 20),
                date(2026, 8, 21),
            ],
        )

    def test_sales_reconciliation_detects_missing_and_changed_dates(self) -> None:
        source = {
            date(2026, 8, 10): {"rows": 10, "units": 20, "amount": 100},
            date(2026, 8, 11): {"rows": 11, "units": 22, "amount": 110},
        }
        target = {
            date(2026, 8, 10): {"rows": 10, "units": 20, "amount": 100},
            date(2026, 8, 11): {"rows": 10, "units": 20, "amount": 100},
            date(2026, 8, 12): {"rows": 1, "units": 1, "amount": 1},
        }

        self.assertEqual(
            find_mismatched_dates(source, target),
            [date(2026, 8, 11), date(2026, 8, 12)],
        )

    def test_sales_refresh_uses_staging_and_atomic_publication(self) -> None:
        sales_source = (
            ROOT / "scripts" / "send" / "actualizar_bases_ventas.py"
        ).read_text(encoding="utf-8")
        self.assertIn("CREATE UNLOGGED TABLE", sales_source)
        self.assertIn("_load_staging_direct", sales_source)
        self.assertIn("copy_expert", sales_source)
        self.assertIn('"mode": "DIRECT_COPY"', sales_source)
        self.assertIn("_publish_staging_atomically", sales_source)
        self.assertIn("reconcile_lookback_days", sales_source)
        self.assertIn("upstream_table", sales_source)
        self.assertIn("_repair_sql_replica_dates", sales_source)
        self.assertIn("historical_mismatch_dates", sales_source)

    def test_weekly_sales_reconciliation_is_configured_for_sunday(self) -> None:
        config = yaml.safe_load((ROOT / "prefect.yaml").read_text(encoding="utf-8"))
        daily = next(
            item
            for item in config["deployments"]
            if item["name"] == "PDD_SOURCE_DAILY_MASTER_PROD"
        )
        weekly = next(
            item
            for item in config["deployments"]
            if item["name"] == "PDD_SALES_RECONCILIATION_WEEKLY"
        )

        self.assertEqual(daily["parameters"]["sales_overlap_days"], 3)
        self.assertEqual(daily["parameters"]["sales_reconciliation_days"], 0)
        self.assertEqual(daily["parameters"]["sales_reconciliation_weekday"], 6)
        self.assertFalse(daily["parameters"]["force_sales_reconciliation"])
        self.assertEqual(
            weekly["entrypoint"],
            "scripts/pdd/pdd_source_daily.py:pdd_sales_reconciliation_flow",
        )
        self.assertEqual(weekly["parameters"]["sales_overlap_days"], 3)
        self.assertEqual(weekly["parameters"]["sales_reconciliation_days"], 45)
        self.assertEqual(weekly["schedule"]["cron"], "0 10 * * 0")
        self.assertEqual(
            weekly["schedule"]["timezone"],
            "America/Argentina/Buenos_Aires",
        )

    def test_weekly_reconciliation_is_separate_and_shares_daily_lock(self) -> None:
        source = (ROOT / "scripts" / "pdd" / "pdd_source_daily.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("def pdd_sales_reconciliation_flow(", source)
        self.assertIn(
            '"Ya existe una sincronizacion diaria o reconciliacion PDD en ejecucion"',
            source,
        )
        self.assertIn("results = _refresh_sales_pipeline(", source)

    def test_sql_server_sales_upgrade_is_atomic_and_serialized(self) -> None:
        upgrade = (
            ROOT / "scripts" / "sql" / "PDD_UPGRADE_T702_REPLICA_ATOMICA_V2.sql"
        ).read_text(encoding="utf-8")

        self.assertEqual(upgrade.count("CREATE OR ALTER PROCEDURE"), 2)
        self.assertEqual(upgrade.count("SET XACT_ABORT ON"), 2)
        self.assertEqual(upgrade.count("BEGIN TRANSACTION"), 2)
        self.assertEqual(upgrade.count("COMMIT TRANSACTION"), 2)
        self.assertEqual(upgrade.count("sys.sp_getapplock"), 2)


if __name__ == "__main__":
    unittest.main()
