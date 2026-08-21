"""Carga canónica SCD2 de ``src.base_articulos_logistica``.

El DDL es responsabilidad de ``scripts/sql/pdd``. Este módulo nunca crea,
reemplaza ni altera la tabla final: recibe un snapshot contractual, lo valida y
aplica altas/cambios/cierres en una única transacción PostgreSQL.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from contextlib import closing
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from psycopg2.extras import Json, execute_values

from scripts.push.etl_chunk_utils import build_sql_server_engine, open_pg_conn


load_dotenv()

TARGET_TABLE = "src.base_articulos_logistica"
STAGING_TABLE = "tmp_base_articulos_logistica_ingesta"
NATURAL_KEY = ("c_articulo", "c_proveedor", "c_configuracion_logistica")
DEFAULT_SOURCE_MODE = os.getenv("PDD_LOGISTICS_SOURCE_MODE", "file").strip().lower()
DEFAULT_SQLSERVER_PROCEDURE = os.getenv(
    "PDD_LOGISTICS_SQLSERVER_PROCEDURE",
    "[dbo].[SP_BASE_ARTICULOS_LOGISTICA_DMZ]",
).strip()
SQLSERVER_FETCH_SIZE = int(os.getenv("PDD_LOGISTICS_SQLSERVER_FETCH_SIZE", "5000"))

QUALITY_VALUES = {"VERIFIED", "SOURCE", "ESTIMATED", "MISSING", "INVALID"}
UNIT_VALUES = {"UNIT", "KG", "LITER", "METER", "M2", "M3", "OTHER"}
PACKAGE_VALUES = {
    "CASE", "PACK", "BAG", "TRAY", "BOTTLE", "CAN", "DRUM", "BALE", "UNIT", "OTHER"
}
VOLUME_METHOD_VALUES = {
    "MEASURED_DIMENSIONS", "SOURCE_DIMENSIONS", "SOURCE_REPORTED",
    "SUPPLIER_REPORTED", "ESTIMATED",
}
TEMPERATURE_VALUES = {"AMBIENT", "CHILLED", "FROZEN", "CONTROLLED", "UNKNOWN"}
ORIENTATION_VALUES = {"ANY", "UPRIGHT", "THIS_SIDE_UP", "OTHER"}
GTIN_LENGTHS = {8, 12, 13, 14}

STRING_FIELDS = {
    "c_configuracion_logistica", "c_unidad_base", "c_gtin_unidad", "c_tipo_bulto",
    "c_gtin_bulto", "c_metodo_volumen", "c_tipo_pallet", "c_zona_temperatura",
    "c_orientacion", "observaciones_manipulacion", "c_calidad_embalaje",
    "c_calidad_peso", "c_calidad_volumen", "c_calidad_pallet",
    "observaciones_calidad", "verificado_por", "fuente_origen", "referencia_origen",
}
BOOLEAN_FIELDS = {
    "m_configuracion_default", "m_activo", "m_vende_por_peso", "m_apilable",
    "m_fragil", "m_peligroso",
}
INTEGER_FIELDS = {
    "c_articulo", "c_proveedor", "q_bultos_por_capa", "q_capas_por_pallet",
    "q_bultos_por_pallet", "q_max_niveles_apilado", "estado_sincronizacion",
}
DECIMAL_FIELDS = {
    "q_unidades_por_bulto", "q_peso_neto_unitario_kg", "q_peso_bruto_unitario_kg",
    "q_peso_bruto_bulto_kg", "q_largo_bulto_cm", "q_ancho_bulto_cm",
    "q_alto_bulto_cm", "q_volumen_bulto_m3", "q_largo_pallet_cm",
    "q_ancho_pallet_cm", "q_alto_pallet_cargado_cm", "q_peso_bruto_pallet_kg",
    "q_temperatura_min_c", "q_temperatura_max_c",
}
NUMERIC_LIMITS = {
    "q_unidades_por_bulto": (18, 6),
    "q_peso_neto_unitario_kg": (18, 6),
    "q_peso_bruto_unitario_kg": (18, 6),
    "q_peso_bruto_bulto_kg": (18, 6),
    "q_largo_bulto_cm": (12, 3),
    "q_ancho_bulto_cm": (12, 3),
    "q_alto_bulto_cm": (12, 3),
    "q_volumen_bulto_m3": (18, 9),
    "q_largo_pallet_cm": (12, 3),
    "q_ancho_pallet_cm": (12, 3),
    "q_alto_pallet_cargado_cm": (12, 3),
    "q_peso_bruto_pallet_kg": (18, 6),
    "q_temperatura_min_c": (6, 2),
    "q_temperatura_max_c": (6, 2),
}
DATETIME_FIELDS = {"verificado_en", "fecha_extraccion"}

BUSINESS_FIELDS = (
    "c_articulo", "c_proveedor", "c_configuracion_logistica",
    "m_configuracion_default", "m_activo", "c_unidad_base", "m_vende_por_peso",
    "c_gtin_unidad", "c_tipo_bulto", "c_gtin_bulto", "q_unidades_por_bulto",
    "q_peso_neto_unitario_kg", "q_peso_bruto_unitario_kg", "q_peso_bruto_bulto_kg",
    "q_largo_bulto_cm", "q_ancho_bulto_cm", "q_alto_bulto_cm", "q_volumen_bulto_m3",
    "c_metodo_volumen", "q_bultos_por_capa", "q_capas_por_pallet",
    "q_bultos_por_pallet", "c_tipo_pallet", "q_largo_pallet_cm",
    "q_ancho_pallet_cm", "q_alto_pallet_cargado_cm", "q_peso_bruto_pallet_kg",
    "m_apilable", "q_max_niveles_apilado", "m_fragil", "m_peligroso",
    "c_zona_temperatura", "q_temperatura_min_c", "q_temperatura_max_c",
    "c_orientacion", "observaciones_manipulacion", "c_calidad_embalaje",
    "c_calidad_peso", "c_calidad_volumen", "c_calidad_pallet",
    "observaciones_calidad", "verificado_en", "verificado_por", "fuente_origen",
    "referencia_origen", "atributos_adicionales",
)
LOAD_FIELDS = BUSINESS_FIELDS + (
    "fecha_extraccion", "cdc_lsn", "estado_sincronizacion", "input_checksum",
    "f_vigencia_desde", "f_vigencia_hasta",
)

DEFAULTS: dict[str, Any] = {
    "c_configuracion_logistica": "DEFAULT",
    "m_configuracion_default": True,
    "m_activo": True,
    "c_calidad_embalaje": None,
    "c_calidad_peso": None,
    "c_calidad_volumen": None,
    "c_calidad_pallet": None,
    "atributos_adicionales": {},
    "estado_sincronizacion": 1,
}


class SnapshotValidationError(ValueError):
    """El snapshot no cumple el contrato canónico."""


def _is_null(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _parse_bool(value: Any, field: str) -> bool | None:
    if _is_null(value):
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().upper()
    if normalized in {"1", "TRUE", "T", "YES", "Y", "SI", "S"}:
        return True
    if normalized in {"0", "FALSE", "F", "NO", "N"}:
        return False
    raise SnapshotValidationError(f"{field}: booleano inválido {value!r}")


def _parse_int(value: Any, field: str) -> int | None:
    if _is_null(value):
        return None
    try:
        decimal = Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise SnapshotValidationError(f"{field}: entero inválido {value!r}") from exc
    if decimal != decimal.to_integral_value():
        raise SnapshotValidationError(f"{field}: debe ser entero, se recibió {value!r}")
    return int(decimal)


def _parse_decimal(value: Any, field: str) -> Decimal | None:
    if _is_null(value):
        return None
    try:
        decimal = Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise SnapshotValidationError(f"{field}: decimal inválido {value!r}") from exc
    if not decimal.is_finite():
        raise SnapshotValidationError(f"{field}: debe ser finito")
    return decimal


def _parse_datetime(value: Any, field: str) -> datetime | None:
    if _is_null(value):
        return None
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            result = datetime.fromisoformat(text)
        except ValueError as exc:
            raise SnapshotValidationError(f"{field}: timestamp ISO-8601 inválido {value!r}") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise SnapshotValidationError(f"{field}: debe incluir zona horaria")
    return result.astimezone(timezone.utc)


def _parse_json_object(value: Any) -> dict[str, Any]:
    if _is_null(value):
        return {}
    if isinstance(value, Mapping):
        result = dict(value)
    else:
        try:
            result = json.loads(str(value))
        except json.JSONDecodeError as exc:
            raise SnapshotValidationError("atributos_adicionales: JSON inválido") from exc
    if not isinstance(result, dict):
        raise SnapshotValidationError("atributos_adicionales: debe ser un objeto JSON")
    try:
        json.dumps(result, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise SnapshotValidationError(
            "atributos_adicionales: contiene valores no serializables o no finitos"
        ) from exc
    return result


def _parse_bytea(value: Any) -> bytes | None:
    if _is_null(value):
        return None
    if isinstance(value, bytes):
        return value
    text = str(value).strip()
    if text.startswith("\\x"):
        text = text[2:]
    try:
        return bytes.fromhex(text)
    except ValueError as exc:
        raise SnapshotValidationError("cdc_lsn: debe ser hexadecimal") from exc


def read_snapshot_file(source_file: str | Path) -> list[dict[str, Any]]:
    """Lee CSV, JSON array o JSON Lines preservando GTIN como texto."""
    path = Path(source_file).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"No existe el archivo de entrada: {path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    if suffix == ".json":
        with path.open("r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise SnapshotValidationError("El JSON debe contener un array de objetos")
        return payload
    if suffix in {".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8-sig") as handle:
            payload = [json.loads(line) for line in handle if line.strip()]
        if not all(isinstance(row, dict) for row in payload):
            raise SnapshotValidationError("Cada línea JSON debe contener un objeto")
        return payload
    raise SnapshotValidationError("Formato no soportado; usar .csv, .json, .jsonl o .ndjson")


def assert_sqlserver_procedure_name(procedure_name: str) -> str:
    identifier = r"(?:[A-Za-z_][A-Za-z0-9_]*|\[[A-Za-z_][A-Za-z0-9_]*\])"
    if not re.fullmatch(rf"{identifier}\.{identifier}", procedure_name):
        raise ValueError(
            "Nombre de stored procedure inválido; usar schema.procedure o [schema].[procedure]"
        )
    return procedure_name


def read_snapshot_sqlserver(
    procedure_name: str = DEFAULT_SQLSERVER_PROCEDURE,
    *,
    fetch_size: int = SQLSERVER_FETCH_SIZE,
) -> list[dict[str, Any]]:
    """Ejecuta el SP contractual usando la conexión SQL_SERVER existente."""
    procedure = assert_sqlserver_procedure_name(procedure_name)
    if fetch_size <= 0:
        raise ValueError("fetch_size debe ser mayor que cero")
    settings = {
        "SQL_SERVER": os.getenv("SQL_SERVER"),
        "SQL_USER": os.getenv("SQL_USER"),
        "SQL_PASSWORD": os.getenv("SQL_PASSWORD"),
        "SQL_DATABASE": os.getenv("SQL_DATABASE"),
    }
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("Faltan variables de conexión SQL Server: " + ", ".join(missing))

    engine = build_sql_server_engine(
        settings["SQL_SERVER"], settings["SQL_USER"],
        settings["SQL_PASSWORD"], settings["SQL_DATABASE"],
    )
    rows: list[dict[str, Any]] = []
    try:
        with closing(engine.raw_connection()) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(f"EXEC {procedure}")
                if cursor.description is None:
                    raise RuntimeError(f"{procedure} no devolvió un result set")
                columns = [str(column[0]).strip().lower() for column in cursor.description]
                if len(columns) != len(set(columns)):
                    raise RuntimeError(f"{procedure} devolvió columnas duplicadas")
                while True:
                    batch = cursor.fetchmany(fetch_size)
                    if not batch:
                        break
                    rows.extend(dict(zip(columns, values)) for values in batch)
    finally:
        engine.dispose()
    if not rows:
        raise SnapshotValidationError(f"{procedure} devolvió un snapshot vacío")
    return rows


def _decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return "0" if text in {"-0", ""} else text


def _checksum_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, dict):
        return {key: _checksum_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_checksum_value(item) for item in value]
    return value


def calculate_input_checksum(row: Mapping[str, Any]) -> str:
    payload = {field: _checksum_value(row.get(field)) for field in BUSINESS_FIELDS}
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_positive(row: Mapping[str, Any], fields: Iterable[str], errors: list[str]) -> None:
    for field in fields:
        value = row.get(field)
        if value is not None and value <= 0:
            errors.append(f"{field}: debe ser mayor que cero")


def _validate_physical_types(row: Mapping[str, Any], errors: list[str]) -> None:
    for field, (precision, scale) in NUMERIC_LIMITS.items():
        value = row.get(field)
        if value is None:
            continue
        if value.copy_abs() >= Decimal(10) ** (precision - scale):
            errors.append(f"{field}: supera la precisión numeric({precision},{scale})")
            continue
        quantum = Decimal(1).scaleb(-scale)
        if value != value.quantize(quantum):
            errors.append(f"{field}: supera la escala numeric({precision},{scale})")
    for field in ("c_articulo", "c_proveedor", "q_bultos_por_capa", "q_capas_por_pallet", "q_bultos_por_pallet"):
        value = row.get(field)
        if value is not None and not (-2147483648 <= value <= 2147483647):
            errors.append(f"{field}: fuera del rango integer de PostgreSQL")
    for field in ("q_max_niveles_apilado", "estado_sincronizacion"):
        value = row.get(field)
        if value is not None and not (-32768 <= value <= 32767):
            errors.append(f"{field}: fuera del rango smallint de PostgreSQL")


def _normalize_row(raw: Mapping[str, Any], row_number: int, effective_at: datetime, source_name: str) -> dict[str, Any]:
    known = set(BUSINESS_FIELDS) | {
        "fecha_extraccion", "cdc_lsn", "estado_sincronizacion", "input_checksum"
    }
    unknown = sorted(str(key) for key in raw if key not in known)
    if unknown:
        raise SnapshotValidationError(f"fila {row_number}: columnas desconocidas: {', '.join(unknown)}")

    row: dict[str, Any] = {field: raw.get(field, DEFAULTS.get(field)) for field in BUSINESS_FIELDS}
    row.update({
        "fecha_extraccion": raw.get("fecha_extraccion", effective_at),
        "cdc_lsn": raw.get("cdc_lsn"),
        "estado_sincronizacion": raw.get("estado_sincronizacion", 1),
    })
    for field in STRING_FIELDS:
        value = row.get(field)
        row[field] = None if _is_null(value) else str(value).strip()
    for field in BOOLEAN_FIELDS:
        row[field] = _parse_bool(row.get(field), field)
    for field in INTEGER_FIELDS:
        row[field] = _parse_int(row.get(field), field)
    for field in DECIMAL_FIELDS:
        row[field] = _parse_decimal(row.get(field), field)
    for field in DATETIME_FIELDS:
        row[field] = _parse_datetime(row.get(field), field)

    row["atributos_adicionales"] = _parse_json_object(raw.get("atributos_adicionales", {}))
    row["cdc_lsn"] = _parse_bytea(raw.get("cdc_lsn"))
    row["c_configuracion_logistica"] = (row["c_configuracion_logistica"] or "DEFAULT").upper()
    for field in (
        "c_unidad_base", "c_tipo_bulto", "c_metodo_volumen", "c_zona_temperatura",
        "c_orientacion", "c_calidad_embalaje", "c_calidad_peso", "c_calidad_volumen",
        "c_calidad_pallet",
    ):
        if row.get(field) is not None:
            row[field] = row[field].upper()
    if row["fuente_origen"] is None:
        row["fuente_origen"] = source_name
    if row["fecha_extraccion"] is None:
        row["fecha_extraccion"] = effective_at

    dimensions = (row["q_largo_bulto_cm"], row["q_ancho_bulto_cm"], row["q_alto_bulto_cm"])
    if all(value is not None for value in dimensions) and row["q_volumen_bulto_m3"] is None:
        with localcontext() as decimal_context:
            decimal_context.prec = 60
            row["q_volumen_bulto_m3"] = (
                dimensions[0] * dimensions[1] * dimensions[2] / Decimal("1000000")
            ).quantize(Decimal("0.000000001"), rounding=ROUND_HALF_UP)
        if row["c_metodo_volumen"] is None:
            row["c_metodo_volumen"] = "SOURCE_DIMENSIONS"
    if (
        row["q_bultos_por_capa"] is not None
        and row["q_capas_por_pallet"] is not None
        and row["q_bultos_por_pallet"] is None
    ):
        row["q_bultos_por_pallet"] = row["q_bultos_por_capa"] * row["q_capas_por_pallet"]

    quality_defaults = {
        "c_calidad_embalaje": "SOURCE" if row["q_unidades_por_bulto"] is not None else "MISSING",
        "c_calidad_peso": "SOURCE" if any(row[field] is not None for field in (
            "q_peso_neto_unitario_kg", "q_peso_bruto_unitario_kg", "q_peso_bruto_bulto_kg"
        )) else "MISSING",
        "c_calidad_volumen": (
            "ESTIMATED" if row["c_metodo_volumen"] == "ESTIMATED"
            else "SOURCE" if row["q_volumen_bulto_m3"] is not None else "MISSING"
        ),
        "c_calidad_pallet": "SOURCE" if row["q_bultos_por_pallet"] is not None else "MISSING",
    }
    for field, default in quality_defaults.items():
        if row[field] is None:
            row[field] = default

    errors: list[str] = []
    if row["c_articulo"] is None or row["c_articulo"] <= 0:
        errors.append("c_articulo: obligatorio y mayor que cero")
    if row["c_proveedor"] is not None and row["c_proveedor"] <= 0:
        errors.append("c_proveedor: debe ser mayor que cero")
    if not re.fullmatch(r"[A-Z][A-Z0-9_-]*", row["c_configuracion_logistica"] or ""):
        errors.append("c_configuracion_logistica: formato inválido")
    if len(row["c_configuracion_logistica"] or "") > 60:
        errors.append("c_configuracion_logistica: supera 60 caracteres")
    if row["c_unidad_base"] not in UNIT_VALUES:
        errors.append(f"c_unidad_base: valor inválido {row['c_unidad_base']!r}")
    if row["m_vende_por_peso"] is None:
        errors.append("m_vende_por_peso: obligatorio")
    if row["m_configuracion_default"] is None:
        errors.append("m_configuracion_default: obligatorio")
    if row["m_activo"] is None:
        errors.append("m_activo: obligatorio")
    if row["estado_sincronizacion"] is None:
        errors.append("estado_sincronizacion: obligatorio")
    if row["c_tipo_bulto"] is not None and row["c_tipo_bulto"] not in PACKAGE_VALUES:
        errors.append(f"c_tipo_bulto: valor inválido {row['c_tipo_bulto']!r}")
    if row["c_metodo_volumen"] is not None and row["c_metodo_volumen"] not in VOLUME_METHOD_VALUES:
        errors.append(f"c_metodo_volumen: valor inválido {row['c_metodo_volumen']!r}")
    if row["c_zona_temperatura"] is not None and row["c_zona_temperatura"] not in TEMPERATURE_VALUES:
        errors.append(f"c_zona_temperatura: valor inválido {row['c_zona_temperatura']!r}")
    if row["c_orientacion"] is not None and row["c_orientacion"] not in ORIENTATION_VALUES:
        errors.append(f"c_orientacion: valor inválido {row['c_orientacion']!r}")
    for field in ("c_calidad_embalaje", "c_calidad_peso", "c_calidad_volumen", "c_calidad_pallet"):
        if row[field] not in QUALITY_VALUES:
            errors.append(f"{field}: valor inválido {row[field]!r}")
    for field in ("c_gtin_unidad", "c_gtin_bulto"):
        value = row[field]
        if value is not None and (not value.isdigit() or len(value) not in GTIN_LENGTHS):
            errors.append(f"{field}: debe tener sólo dígitos y longitud 8, 12, 13 o 14")
    _validate_positive(row, DECIMAL_FIELDS - {"q_temperatura_min_c", "q_temperatura_max_c"}, errors)
    _validate_positive(row, {
        "q_bultos_por_capa", "q_capas_por_pallet", "q_bultos_por_pallet", "q_max_niveles_apilado"
    }, errors)
    _validate_physical_types(row, errors)
    if sum(value is not None for value in dimensions) not in {0, 3}:
        errors.append("dimensiones de bulto: deben informarse las tres o ninguna")
    if (row["q_volumen_bulto_m3"] is None) != (row["c_metodo_volumen"] is None):
        errors.append("volumen y método: deben informarse juntos")
    if (
        row["q_peso_neto_unitario_kg"] is not None
        and row["q_peso_bruto_unitario_kg"] is not None
        and row["q_peso_bruto_unitario_kg"] < row["q_peso_neto_unitario_kg"]
    ):
        errors.append("peso bruto unitario: no puede ser menor al neto")
    if (
        row["q_bultos_por_capa"] is not None and row["q_capas_por_pallet"] is not None
        and row["q_bultos_por_pallet"] != row["q_bultos_por_capa"] * row["q_capas_por_pallet"]
    ):
        errors.append("pallet: q_bultos_por_pallet no coincide con capas por bultos/capa")
    if row["m_apilable"] is not True and row["q_max_niveles_apilado"] is not None:
        errors.append("apilado: el DDL exige m_apilable=true cuando se informa el máximo")
    if (
        row["q_temperatura_min_c"] is not None and row["q_temperatura_max_c"] is not None
        and row["q_temperatura_min_c"] > row["q_temperatura_max_c"]
    ):
        errors.append("temperatura: mínima mayor que máxima")
    if row["fuente_origen"] is None or len(row["fuente_origen"]) > 60:
        errors.append("fuente_origen: obligatorio y de hasta 60 caracteres")
    if row["referencia_origen"] is not None and len(row["referencia_origen"]) > 160:
        errors.append("referencia_origen: supera 160 caracteres")
    if row["verificado_por"] is not None and len(row["verificado_por"]) > 100:
        errors.append("verificado_por: supera 100 caracteres")
    if row["c_tipo_pallet"] is not None and len(row["c_tipo_pallet"]) > 30:
        errors.append("c_tipo_pallet: supera 30 caracteres")
    if errors:
        raise SnapshotValidationError(f"fila {row_number}: " + "; ".join(errors))

    computed_checksum = calculate_input_checksum(row)
    supplied_checksum = raw.get("input_checksum")
    if not _is_null(supplied_checksum) and str(supplied_checksum).lower() != computed_checksum:
        raise SnapshotValidationError(
            f"fila {row_number}: input_checksum no coincide; esperado {computed_checksum}"
        )
    row["input_checksum"] = computed_checksum
    row["f_vigencia_desde"] = effective_at
    row["f_vigencia_hasta"] = None
    return row


def normalize_snapshot(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    effective_at: datetime | str | None = None,
    source_name: str = "MANUAL_PDD_LOGISTICS",
) -> list[dict[str, Any]]:
    if not raw_rows:
        raise SnapshotValidationError("El snapshot está vacío; no se aplican cierres masivos")
    effective = _parse_datetime(effective_at, "effective_at") if effective_at else datetime.now(timezone.utc)
    assert effective is not None
    normalized: list[dict[str, Any]] = []
    errors: list[str] = []
    for index, raw in enumerate(raw_rows, start=2):
        try:
            normalized.append(_normalize_row(raw, index, effective, source_name.strip()))
        except SnapshotValidationError as exc:
            errors.append(str(exc))
    keys: dict[tuple[Any, ...], int] = {}
    defaults: dict[int, int] = {}
    for index, row in enumerate(normalized, start=2):
        key = tuple(row[field] for field in NATURAL_KEY)
        if key in keys:
            errors.append(f"fila {index}: clave natural duplicada; primera aparición en fila {keys[key]}")
        keys[key] = index
        if row["m_activo"] and row["m_configuracion_default"]:
            article = row["c_articulo"]
            if article in defaults:
                errors.append(f"fila {index}: más de una configuración default activa para artículo {article}")
            defaults[article] = index
    if errors:
        preview = "\n".join(f"- {error}" for error in errors[:50])
        suffix = f"\n- ... {len(errors) - 50} errores adicionales" if len(errors) > 50 else ""
        raise SnapshotValidationError(f"Snapshot inválido ({len(errors)} errores):\n{preview}{suffix}")
    return normalized


def _qualified_key(left: str, right: str) -> str:
    return (
        f"{left}.c_articulo = {right}.c_articulo "
        f"AND {left}.c_proveedor IS NOT DISTINCT FROM {right}.c_proveedor "
        f"AND {left}.c_configuracion_logistica = {right}.c_configuracion_logistica"
    )


def _assert_target_contract(cur: Any) -> None:
    cur.execute(
        "SELECT current_database(), to_regclass(%s), to_regclass(%s)",
        (TARGET_TABLE, "src.v_base_articulos_logistica_actual"),
    )
    database, relation, current_view = cur.fetchone()
    if database != "diarco_data":
        raise RuntimeError(f"Base incorrecta: se esperaba diarco_data y se recibió {database}")
    if relation is None:
        raise RuntimeError(
            f"No existe {TARGET_TABLE}; el DDL debe ser aplicado manualmente por su responsable"
        )
    if current_view is None:
        raise RuntimeError("No existe src.v_base_articulos_logistica_actual; DDL incompleto")
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'src' AND table_name = 'base_articulos_logistica'"
    )
    actual = {row[0] for row in cur.fetchall()}
    required = set(LOAD_FIELDS) | {"articulo_logistica_id", "creado_en", "actualizado_en"}
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError(f"La tabla final no cumple el contrato; faltan: {', '.join(missing)}")
    cur.execute(
        "SELECT indexname FROM pg_indexes "
        "WHERE schemaname = 'src' AND tablename = 'base_articulos_logistica'"
    )
    indexes = {row[0] for row in cur.fetchall()}
    required_indexes = {
        "uq_base_articulos_logistica_config_actual",
        "uq_base_articulos_logistica_default_actual",
    }
    missing_indexes = sorted(required_indexes - indexes)
    if missing_indexes:
        raise RuntimeError(
            "La tabla final no tiene los índices SCD2 requeridos: " + ", ".join(missing_indexes)
        )


def _stage_rows(cur: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    columns = ", ".join(LOAD_FIELDS)
    cur.execute(
        f"CREATE TEMP TABLE {STAGING_TABLE} ON COMMIT DROP AS "
        f"SELECT {columns} FROM {TARGET_TABLE} WITH NO DATA"
    )
    values = []
    for row in rows:
        values.append(tuple(
            Json(row[field]) if field == "atributos_adicionales" else row[field]
            for field in LOAD_FIELDS
        ))
    execute_values(
        cur,
        f"INSERT INTO {STAGING_TABLE} ({columns}) VALUES %s",
        values,
        page_size=1000,
    )


def _preview_changes(cur: Any, full_snapshot: bool) -> dict[str, int]:
    key = _qualified_key("t", "s")
    cur.execute(
        f"""
        SELECT
            count(*) FILTER (WHERE t.articulo_logistica_id IS NULL),
            count(*) FILTER (
                WHERE t.articulo_logistica_id IS NOT NULL
                  AND t.input_checksum <> s.input_checksum
            ),
            count(*) FILTER (
                WHERE t.articulo_logistica_id IS NOT NULL
                  AND t.input_checksum = s.input_checksum
            )
        FROM {STAGING_TABLE} s
        LEFT JOIN {TARGET_TABLE} t ON {key} AND t.f_vigencia_hasta IS NULL
        """
    )
    new, changed, unchanged = cur.fetchone()
    missing = 0
    if full_snapshot:
        cur.execute(
            f"SELECT count(*) FROM {TARGET_TABLE} t WHERE t.f_vigencia_hasta IS NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {STAGING_TABLE} s WHERE {key})"
        )
        missing = cur.fetchone()[0]
    return {
        "new": int(new), "changed": int(changed), "unchanged": int(unchanged),
        "closed_missing": int(missing),
    }


def _validate_proposed_defaults(cur: Any, full_snapshot: bool) -> None:
    key = _qualified_key("t", "s")
    retained = "FALSE" if full_snapshot else f"NOT EXISTS (SELECT 1 FROM {STAGING_TABLE} s WHERE {key})"
    cur.execute(
        f"""
        WITH proposed_defaults AS (
            SELECT t.c_articulo
            FROM {TARGET_TABLE} t
            WHERE t.f_vigencia_hasta IS NULL
              AND t.m_activo AND t.m_configuracion_default
              AND {retained}
            UNION ALL
            SELECT s.c_articulo
            FROM {STAGING_TABLE} s
            WHERE s.m_activo AND s.m_configuracion_default
        )
        SELECT c_articulo
        FROM proposed_defaults
        GROUP BY c_articulo
        HAVING count(*) > 1
        ORDER BY c_articulo
        LIMIT 20
        """
    )
    collisions = [row[0] for row in cur.fetchall()]
    if collisions:
        raise SnapshotValidationError(
            "La carga produciría más de una configuración default vigente para artículos: "
            + ", ".join(map(str, collisions))
        )


def _validate_effective_time(cur: Any, effective_at: datetime, full_snapshot: bool) -> None:
    key = _qualified_key("t", "s")
    join_type = "LEFT JOIN" if full_snapshot else "JOIN"
    version_will_close = (
        "(s.c_articulo IS NULL OR t.input_checksum <> s.input_checksum)"
        if full_snapshot
        else "t.input_checksum <> s.input_checksum"
    )
    cur.execute(
        f"""
        SELECT t.c_articulo, t.c_proveedor, t.c_configuracion_logistica, t.f_vigencia_desde
        FROM {TARGET_TABLE} t
        {join_type} {STAGING_TABLE} s ON {key}
        WHERE t.f_vigencia_hasta IS NULL
          AND {version_will_close}
          AND t.f_vigencia_desde >= %s
        LIMIT 20
        """,
        (effective_at,),
    )
    conflicts = cur.fetchall()
    if conflicts:
        raise SnapshotValidationError(
            "effective_at debe ser posterior al inicio de las versiones que se cierran: "
            + ", ".join(f"{row[0]}/{row[1]}/{row[2]}" for row in conflicts)
        )


def _post_load_validation(cur: Any) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT
            count(*) FILTER (WHERE f_vigencia_hasta IS NULL) AS current_rows,
            count(*) FILTER (WHERE f_vigencia_hasta IS NULL AND m_activo) AS active_rows,
            count(*) FILTER (WHERE f_vigencia_hasta IS NULL AND m_activo AND m_configuracion_default)
                AS default_rows,
            count(*) FILTER (WHERE f_vigencia_hasta IS NOT NULL AND f_vigencia_hasta <= f_vigencia_desde)
                AS invalid_periods
        FROM {TARGET_TABLE}
        """
    )
    current, active, defaults, invalid_periods = cur.fetchone()
    cur.execute(
        f"""
        SELECT
            (SELECT count(*) FROM (
                SELECT c_articulo, coalesce(c_proveedor, -1), c_configuracion_logistica
                FROM {TARGET_TABLE}
                WHERE f_vigencia_hasta IS NULL
                GROUP BY c_articulo, coalesce(c_proveedor, -1), c_configuracion_logistica
                HAVING count(*) > 1
            ) duplicated_keys),
            (SELECT count(*) FROM (
                SELECT c_articulo
                FROM {TARGET_TABLE}
                WHERE f_vigencia_hasta IS NULL AND m_activo AND m_configuracion_default
                GROUP BY c_articulo
                HAVING count(*) > 1
            ) duplicated_defaults)
        """
    )
    duplicate_keys, duplicate_defaults = cur.fetchone()
    if invalid_periods or duplicate_keys or duplicate_defaults:
        raise RuntimeError(
            "Validación SCD2 fallida: "
            f"períodos_inválidos={invalid_periods}, "
            f"claves_actuales_duplicadas={duplicate_keys}, "
            f"defaults_actuales_duplicados={duplicate_defaults}"
        )
    return {
        "current_rows": int(current), "active_rows": int(active),
        "default_rows": int(defaults), "invalid_periods": 0,
        "duplicate_current_keys": 0, "duplicate_current_defaults": 0,
    }


def apply_scd2(
    conn: Any,
    rows: Sequence[Mapping[str, Any]],
    *,
    full_snapshot: bool,
    validate_only: bool,
) -> dict[str, Any]:
    """Aplica el snapshot dentro de una transacción y devuelve métricas auditables."""
    effective_at = rows[0]["f_vigencia_desde"]
    try:
        with conn.cursor() as cur:
            _assert_target_contract(cur)
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (TARGET_TABLE,))
            _stage_rows(cur, rows)
            _validate_proposed_defaults(cur, full_snapshot)
            _validate_effective_time(cur, effective_at, full_snapshot)
            result = _preview_changes(cur, full_snapshot)
            result["source_rows"] = len(rows)
            result["full_snapshot"] = full_snapshot
            result["validate_only"] = validate_only
            result["effective_at"] = effective_at.isoformat()
            if validate_only:
                result.update(_post_load_validation(cur))
                conn.rollback()
                result["status"] = "VALIDATED"
                return result

            key = _qualified_key("t", "s")
            cur.execute(
                f"""
                UPDATE {TARGET_TABLE} t
                SET f_vigencia_hasta = %s, actualizado_en = clock_timestamp()
                FROM {STAGING_TABLE} s
                WHERE t.f_vigencia_hasta IS NULL
                  AND {key}
                  AND t.input_checksum <> s.input_checksum
                """,
                (effective_at,),
            )
            if full_snapshot:
                cur.execute(
                    f"""
                    UPDATE {TARGET_TABLE} t
                    SET f_vigencia_hasta = %s, actualizado_en = clock_timestamp()
                    WHERE t.f_vigencia_hasta IS NULL
                      AND NOT EXISTS (SELECT 1 FROM {STAGING_TABLE} s WHERE {key})
                    """,
                    (effective_at,),
                )

            insert_columns = ", ".join(LOAD_FIELDS)
            select_columns = ", ".join(f"s.{field}" for field in LOAD_FIELDS)
            cur.execute(
                f"""
                INSERT INTO {TARGET_TABLE} ({insert_columns})
                SELECT {select_columns}
                FROM {STAGING_TABLE} s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {TARGET_TABLE} t
                    WHERE t.f_vigencia_hasta IS NULL AND {key}
                )
                """
            )
            inserted = max(cur.rowcount or 0, 0)
            result["inserted"] = inserted
            result.update(_post_load_validation(cur))
        conn.commit()
        result["status"] = "APPLIED"
        return result
    except Exception:
        conn.rollback()
        raise


def open_target_connection() -> Any:
    return open_pg_conn(
        os.getenv("PG_HOST"), os.getenv("PG_PORT"), os.getenv("PG_DB"),
        os.getenv("PG_USER"), os.getenv("PG_PASSWORD"),
    )


@task(name="validar_snapshot_base_articulos_logistica", persist_result=False)
def validate_snapshot_task(
    source_mode: str,
    source_file: str,
    stored_procedure: str,
    effective_at: str | None,
    source_name: str,
) -> list[dict[str, Any]]:
    logger = get_run_logger()
    mode = source_mode.strip().lower()
    if mode == "file":
        if not source_file.strip():
            raise ValueError("source_file es obligatorio cuando source_mode='file'")
        raw_rows = read_snapshot_file(source_file)
        source_description = source_file
    elif mode == "sqlserver_sp":
        raw_rows = read_snapshot_sqlserver(stored_procedure)
        source_description = assert_sqlserver_procedure_name(stored_procedure)
    else:
        raise ValueError("source_mode inválido; usar 'file' o 'sqlserver_sp'")
    rows = normalize_snapshot(
        raw_rows, effective_at=effective_at, source_name=source_name
    )
    logger.info(
        "Snapshot contractual válido | modo=%s | filas=%s | origen=%s",
        mode, len(rows), source_description,
    )
    return rows


@task(
    name="aplicar_scd2_base_articulos_logistica",
    retries=1,
    retry_delay_seconds=30,
    persist_result=False,
)
def apply_scd2_task(
    rows: Sequence[Mapping[str, Any]],
    full_snapshot: bool,
    validate_only: bool,
) -> dict[str, Any]:
    logger = get_run_logger()
    with closing(open_target_connection()) as conn:
        result = apply_scd2(conn, rows, full_snapshot=full_snapshot, validate_only=validate_only)
    logger.info("Resultado SCD2 | %s", result)
    return result


@flow(name="pdd_base_articulos_logistica_scd2", persist_result=False)
def base_articulos_logistica_scd2_flow(
    source_mode: str = DEFAULT_SOURCE_MODE,
    source_file: str = "",
    stored_procedure: str = DEFAULT_SQLSERVER_PROCEDURE,
    full_snapshot: bool = False,
    validate_only: bool = True,
    effective_at: str | None = None,
    source_name: str = "MANUAL_PDD_LOGISTICS",
) -> dict[str, Any]:
    """Valida y aplica manualmente una fuente explícita; nunca ejecuta el DDL."""
    mode = source_mode.strip().lower()
    if mode == "sqlserver_sp" and not full_snapshot:
        raise ValueError("sqlserver_sp devuelve el universo completo y exige full_snapshot=true")
    rows = validate_snapshot_task(
        mode, source_file, stored_procedure, effective_at, source_name
    )
    return apply_scd2_task(rows, full_snapshot, validate_only)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_file", nargs="?", default="")
    parser.add_argument("--source-mode", choices=("file", "sqlserver_sp"), default=DEFAULT_SOURCE_MODE)
    parser.add_argument("--stored-procedure", default=DEFAULT_SQLSERVER_PROCEDURE)
    parser.add_argument("--full-snapshot", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Aplica; por defecto sólo valida")
    parser.add_argument("--effective-at")
    parser.add_argument("--source-name", default="MANUAL_PDD_LOGISTICS")
    args = parser.parse_args()
    base_articulos_logistica_scd2_flow(
        source_mode=args.source_mode,
        source_file=args.source_file,
        stored_procedure=args.stored_procedure,
        full_snapshot=args.full_snapshot,
        validate_only=not args.apply,
        effective_at=args.effective_at,
        source_name=args.source_name,
    )
