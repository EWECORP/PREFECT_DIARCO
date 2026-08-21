from __future__ import annotations

import copy
import inspect
import os
import re
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from scripts.pdd.cargar_base_articulos_logistica import (
    BUSINESS_FIELDS,
    SnapshotValidationError,
    apply_scd2,
    assert_sqlserver_procedure_name,
    base_articulos_logistica_scd2_flow,
    calculate_input_checksum,
    normalize_snapshot,
    read_snapshot_file,
    read_snapshot_sqlserver,
)


EFFECTIVE_AT = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


def valid_row(**overrides):
    row = {
        "c_articulo": 101,
        "c_unidad_base": "UNIT",
        "m_vende_por_peso": False,
        "q_unidades_por_bulto": "12",
        "fuente_origen": "MAESTRO_LOGISTICO",
    }
    row.update(overrides)
    return row


class NormalizeSnapshotTests(unittest.TestCase):
    def test_minimum_row_gets_defaults_and_contract_checksum(self):
        [row] = normalize_snapshot([valid_row()], effective_at=EFFECTIVE_AT)

        self.assertEqual(row["c_configuracion_logistica"], "DEFAULT")
        self.assertEqual(row["c_calidad_embalaje"], "SOURCE")
        self.assertEqual(row["c_calidad_peso"], "MISSING")
        self.assertEqual(row["fecha_extraccion"], EFFECTIVE_AT)
        self.assertRegex(row["input_checksum"], r"^[0-9a-f]{64}$")

    def test_derives_volume_and_pallet_without_silent_mismatch(self):
        [row] = normalize_snapshot(
            [valid_row(
                q_largo_bulto_cm="40", q_ancho_bulto_cm="30", q_alto_bulto_cm="20",
                q_bultos_por_capa=8, q_capas_por_pallet=5,
            )],
            effective_at=EFFECTIVE_AT,
        )

        self.assertEqual(row["q_volumen_bulto_m3"], Decimal("0.024"))
        self.assertEqual(row["c_metodo_volumen"], "SOURCE_DIMENSIONS")
        self.assertEqual(row["q_bultos_por_pallet"], 40)

    def test_derived_volume_is_rounded_to_canonical_scale(self):
        [row] = normalize_snapshot(
            [valid_row(
                q_largo_bulto_cm="40.123", q_ancho_bulto_cm="30.456", q_alto_bulto_cm="20.789"
            )],
            effective_at=EFFECTIVE_AT,
        )
        self.assertLessEqual(-row["q_volumen_bulto_m3"].as_tuple().exponent, 9)

    def test_rejects_invalid_gtin_and_pallet_instead_of_correcting(self):
        with self.assertRaisesRegex(SnapshotValidationError, "GTIN|gtin|pallet"):
            normalize_snapshot(
                [valid_row(
                    c_gtin_unidad="123456789",
                    q_bultos_por_capa=8,
                    q_capas_por_pallet=5,
                    q_bultos_por_pallet=39,
                )],
                effective_at=EFFECTIVE_AT,
            )

    def test_rejects_duplicate_natural_key_with_null_supplier(self):
        with self.assertRaisesRegex(SnapshotValidationError, "clave natural duplicada"):
            normalize_snapshot([valid_row(), valid_row()], effective_at=EFFECTIVE_AT)

    def test_rejects_multiple_active_defaults_per_article(self):
        with self.assertRaisesRegex(SnapshotValidationError, "default activa"):
            normalize_snapshot(
                [
                    valid_row(c_configuracion_logistica="DEFAULT"),
                    valid_row(c_configuracion_logistica="SUPPLIER_20", c_proveedor=20),
                ],
                effective_at=EFFECTIVE_AT,
            )

    def test_checksum_is_deterministic_and_excludes_extraction_time(self):
        [row] = normalize_snapshot([valid_row()], effective_at=EFFECTIVE_AT)
        reordered = {key: row[key] for key in reversed(BUSINESS_FIELDS)}
        self.assertEqual(calculate_input_checksum(row), calculate_input_checksum(reordered))

        changed = copy.deepcopy(row)
        changed["q_unidades_por_bulto"] += 1
        self.assertNotEqual(calculate_input_checksum(row), calculate_input_checksum(changed))

        changed_only_audit = copy.deepcopy(row)
        changed_only_audit["fecha_extraccion"] = datetime(2026, 8, 22, tzinfo=timezone.utc)
        self.assertEqual(calculate_input_checksum(row), calculate_input_checksum(changed_only_audit))

    def test_rejects_supplied_checksum_that_does_not_match(self):
        with self.assertRaisesRegex(SnapshotValidationError, "input_checksum no coincide"):
            normalize_snapshot(
                [valid_row(input_checksum="0" * 64)], effective_at=EFFECTIVE_AT
            )

    def test_loader_fields_are_present_in_canonical_ddl(self):
        ddl = Path("scripts/sql/pdd/001_create_base_articulos_logistica.sql").read_text(
            encoding="utf-8"
        )
        for field in BUSINESS_FIELDS:
            self.assertIn(field, ddl, msg=f"Campo sin respaldo en DDL: {field}")

    def test_rejects_unknown_columns_even_when_empty(self):
        with self.assertRaisesRegex(SnapshotValidationError, "columnas desconocidas"):
            normalize_snapshot(
                [valid_row(campo_con_typo="")], effective_at=EFFECTIVE_AT
            )

    def test_csv_reader_preserves_leading_zero_gtin(self):
        contents = (
            "c_articulo,c_unidad_base,m_vende_por_peso,c_gtin_unidad\n"
            "101,UNIT,false,0123456789012\n"
        )
        with (
            patch.object(Path, "resolve", return_value=Path("snapshot.csv")),
            patch.object(Path, "is_file", return_value=True),
            patch.object(Path, "open", mock_open(read_data=contents)),
        ):
            [row] = read_snapshot_file("snapshot.csv")
        self.assertEqual(row["c_gtin_unidad"], "0123456789012")


class ApplyScd2Tests(unittest.TestCase):
    @patch("scripts.pdd.cargar_base_articulos_logistica._post_load_validation")
    @patch("scripts.pdd.cargar_base_articulos_logistica._preview_changes")
    @patch("scripts.pdd.cargar_base_articulos_logistica._validate_effective_time")
    @patch("scripts.pdd.cargar_base_articulos_logistica._validate_proposed_defaults")
    @patch("scripts.pdd.cargar_base_articulos_logistica._stage_rows")
    @patch("scripts.pdd.cargar_base_articulos_logistica._assert_target_contract")
    def test_validate_only_rolls_back_without_mutating_target(
        self, _contract, _stage, _defaults, _time, preview, post
    ):
        preview.return_value = {"new": 1, "changed": 0, "unchanged": 0, "closed_missing": 0}
        post.return_value = {"current_rows": 0, "active_rows": 0, "default_rows": 0}
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        [row] = normalize_snapshot([valid_row()], effective_at=EFFECTIVE_AT)

        result = apply_scd2(conn, [row], full_snapshot=True, validate_only=True)

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        post.assert_called_once()
        self.assertEqual(result["status"], "VALIDATED")
        mutation_sql = " ".join(call.args[0] for call in cursor.execute.call_args_list if call.args)
        self.assertNotIn("UPDATE src.base_articulos_logistica", mutation_sql)
        self.assertNotIn("INSERT INTO src.base_articulos_logistica", mutation_sql)

    @patch("scripts.pdd.cargar_base_articulos_logistica._post_load_validation")
    @patch("scripts.pdd.cargar_base_articulos_logistica._preview_changes")
    @patch("scripts.pdd.cargar_base_articulos_logistica._validate_effective_time")
    @patch("scripts.pdd.cargar_base_articulos_logistica._validate_proposed_defaults")
    @patch("scripts.pdd.cargar_base_articulos_logistica._stage_rows")
    @patch("scripts.pdd.cargar_base_articulos_logistica._assert_target_contract")
    def test_apply_commits_close_and_insert_atomically(
        self, _contract, _stage, _defaults, _time, preview, post
    ):
        preview.return_value = {"new": 0, "changed": 1, "unchanged": 0, "closed_missing": 1}
        post.return_value = {"current_rows": 1, "active_rows": 1, "default_rows": 1}
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.rowcount = 1
        [row] = normalize_snapshot([valid_row()], effective_at=EFFECTIVE_AT)

        result = apply_scd2(conn, [row], full_snapshot=True, validate_only=False)

        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        mutation_sql = " ".join(call.args[0] for call in cursor.execute.call_args_list if call.args)
        self.assertIn("UPDATE src.base_articulos_logistica", mutation_sql)
        self.assertIn("INSERT INTO src.base_articulos_logistica", mutation_sql)
        self.assertEqual(result["status"], "APPLIED")

    def test_implementation_does_not_recreate_or_alter_final_table(self):
        source = inspect.getsource(apply_scd2).upper()
        self.assertNotIn("DROP TABLE", source)
        self.assertNotIn("ALTER TABLE", source)
        self.assertNotIn("CREATE TABLE SRC.BASE_ARTICULOS_LOGISTICA", source)


class SqlServerSourceTests(unittest.TestCase):
    def test_rejects_unsafe_stored_procedure_name(self):
        with self.assertRaisesRegex(ValueError, "stored procedure inválido"):
            assert_sqlserver_procedure_name("dbo.proc; DROP TABLE x")

    @patch("scripts.pdd.cargar_base_articulos_logistica.build_sql_server_engine")
    def test_reads_contract_rows_in_chunks_with_existing_connection_settings(self, build_engine):
        engine = MagicMock()
        connection = MagicMock()
        cursor = MagicMock()
        build_engine.return_value = engine
        engine.raw_connection.return_value = connection
        connection.cursor.return_value = cursor
        cursor.description = [
            ("C_ARTICULO",), ("C_UNIDAD_BASE",), ("M_VENDE_POR_PESO",)
        ]
        cursor.fetchmany.side_effect = [[(100001, "UNIT", False)], []]
        environment = {
            "SQL_SERVER": "sql.example.internal",
            "SQL_USER": "test_user",
            "SQL_PASSWORD": "test_password",
            "SQL_DATABASE": "data-sync",
        }

        with patch.dict(os.environ, environment, clear=False):
            rows = read_snapshot_sqlserver("[dbo].[SP_BASE_ARTICULOS_LOGISTICA_DMZ]")

        self.assertEqual(rows, [{
            "c_articulo": 100001,
            "c_unidad_base": "UNIT",
            "m_vende_por_peso": False,
        }])
        cursor.execute.assert_called_once_with("EXEC [dbo].[SP_BASE_ARTICULOS_LOGISTICA_DMZ]")
        cursor.close.assert_called_once()
        connection.close.assert_called_once()
        engine.dispose.assert_called_once()

    def test_sqlserver_mode_requires_complete_snapshot(self):
        with self.assertRaisesRegex(ValueError, "full_snapshot=true"):
            base_articulos_logistica_scd2_flow.fn(
                source_mode="sqlserver_sp", full_snapshot=False
            )

    def test_stored_procedure_keeps_unconfirmed_weight_out_of_canonical_fields(self):
        sql = Path("scripts/sql/pdd/SP_BASE_ARTICULOS_LOGISTICA_DMZ.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("q_peso_neto_unitario_kg = CAST(NULL", sql)
        self.assertIn("q_peso_bruto_unitario_kg = CAST(NULL", sql)
        self.assertIn("q_peso_bruto_bulto_kg = CAST(NULL", sql)
        self.assertIn('"q_peso_unit_art_candidate"', sql)

    def test_stored_procedure_exposes_exactly_the_loader_contract(self):
        sql = Path("scripts/sql/pdd/SP_BASE_ARTICULOS_LOGISTICA_DMZ.sql").read_text(
            encoding="utf-8"
        )
        aliases = set(re.findall(r"^\s{8}([a-z][a-z0-9_]*)\s*=", sql, re.MULTILINE))
        expected = set(BUSINESS_FIELDS) | {
            "fecha_extraccion", "cdc_lsn", "estado_sincronizacion"
        }
        self.assertEqual(aliases, expected)

    def test_stored_procedure_uses_only_primary_ean13_and_dun14(self):
        sql = Path("scripts/sql/pdd/SP_BASE_ARTICULOS_LOGISTICA_DMZ.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("LEN(codes.c_ean) = 13", sql)
        self.assertIn("LEN(codes.c_dun14) = 14", sql)
        self.assertIn("art.C_EAN", sql)
        self.assertIn("art.C_DUN14", sql)
        executable_sql = sql.split("CREATE OR ALTER PROCEDURE", 1)[1]
        self.assertNotIn("T085_ARTICULOS_EAN_EDI", executable_sql)


if __name__ == "__main__":
    unittest.main()
