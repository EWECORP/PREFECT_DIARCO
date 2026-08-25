"""Sincronizacion y contrato diario de fuentes PDD en ``diarco_data``.

``ETL_DIARCO`` es el propietario de estas fuentes. El backend PDD solamente
debe consumirlas cuando la ultima corrida de este flujo quede en estado READY.
El DDL de auditoria se aplica por migracion; el flujo nunca crea tablas.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from prefect import flow, get_run_logger, task
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL


LOCAL_TIMEZONE = ZoneInfo("America/Argentina/Buenos_Aires")
AUDIT_RUN_TABLE = "audit.pdd_source_sync_run"
AUDIT_DETAIL_TABLE = "audit.pdd_source_sync_detail"
LOCK_NAME = "pdd.etl_diarco.source_daily"


@dataclass(frozen=True)
class SourceCheck:
    source_code: str
    physical_relation: str
    is_required: bool
    status: str
    max_business_date: date | None
    as_of_ts: datetime | None
    row_count: int | None
    blocker_codes: tuple[str, ...] = ()
    warning_codes: tuple[str, ...] = ()
    detail: Mapping[str, Any] = field(default_factory=dict)

    def serializable(self) -> dict[str, Any]:
        value = asdict(self)
        value["max_business_date"] = (
            self.max_business_date.isoformat() if self.max_business_date else None
        )
        value["as_of_ts"] = self.as_of_ts.isoformat() if self.as_of_ts else None
        return value


@dataclass(frozen=True)
class SourceReadiness:
    business_date: date
    cutoff_date: date
    status: str
    common_closed_date: date | None
    recommended_business_date: date | None
    checks: tuple[SourceCheck, ...]

    @property
    def blocker_codes(self) -> tuple[str, ...]:
        return tuple(
            f"{check.source_code}:{code}"
            for check in self.checks
            for code in check.blocker_codes
        )

    @property
    def warning_codes(self) -> tuple[str, ...]:
        return tuple(
            f"{check.source_code}:{code}"
            for check in self.checks
            for code in check.warning_codes
        )

    def serializable(self) -> dict[str, Any]:
        return {
            "business_date": self.business_date.isoformat(),
            "cutoff_date": self.cutoff_date.isoformat(),
            "status": self.status,
            "common_closed_date": (
                self.common_closed_date.isoformat() if self.common_closed_date else None
            ),
            "recommended_business_date": (
                self.recommended_business_date.isoformat()
                if self.recommended_business_date
                else None
            ),
            "blocker_codes": list(self.blocker_codes),
            "warning_codes": list(self.warning_codes),
            "checks": [check.serializable() for check in self.checks],
        }


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _parse_business_date(value: date | str | None) -> date:
    if value is None:
        return datetime.now(LOCAL_TIMEZONE).date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _load_db_settings() -> dict[str, str]:
    default_path = "E:/ETL/ETL_DIARCO/.env"
    env_path = os.environ.get("ETL_ENV_PATH", default_path)
    file_values = dotenv_values(env_path) if os.path.exists(env_path) else {}

    settings: dict[str, str] = {}
    for key in ("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD"):
        value = os.environ.get(key) or file_values.get(key)
        if key == "PG_PORT" and not value:
            value = "5432"
        if not value:
            raise RuntimeError(
                f"Variable requerida no configurada: {key}; ETL_ENV_PATH={env_path}"
            )
        settings[key] = str(value)
    return settings


def build_pg_engine() -> Engine:
    settings = _load_db_settings()
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=settings["PG_USER"],
        password=settings["PG_PASSWORD"],
        host=settings["PG_HOST"],
        port=int(settings["PG_PORT"]),
        database=settings["PG_DB"],
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={
            "application_name": "etl_diarco:pdd_source_daily",
            "keepalives": 1,
            "keepalives_idle": 60,
            "keepalives_interval": 30,
            "keepalives_count": 5,
        },
    )


SOURCE_STATE_SQL = text(
    """
    WITH raw_diarco AS (
        SELECT max(f_venta)::date AS max_date, count(*)::bigint AS rows
        FROM src.t702_est_vtas_por_articulo
    ), raw_barrio AS (
        SELECT max(f_venta)::date AS max_date, count(*)::bigint AS rows
        FROM src.t702_est_vtas_por_articulo_dbarrio
    ), raw_sales AS (
        SELECT max(fecha)::date AS max_date, count(*)::bigint AS rows
        FROM src.base_ventas_extendida
    ), enriched_sales AS (
        SELECT max(fecha)::date AS max_date, count(*)::bigint AS rows
        FROM datamart.dm_bve_ventas_enriquecidas
    ), historical_stock AS (
        SELECT
            max(
                least(
                    (
                        date_trunc(
                            'month', make_date(c_anio::integer, c_mes::integer, 1)
                        ) + interval '1 month - 1 day'
                    )::date,
                    coalesce(
                        fecha_proceso::date - 1,
                        (
                            date_trunc(
                                'month', make_date(c_anio::integer, c_mes::integer, 1)
                            ) + interval '1 month - 1 day'
                        )::date
                    )
                )
            ) AS max_date,
            count(*)::bigint AS rows,
            max(fecha_proceso) AS as_of_ts
        FROM src.t710_estadis_stock
    ), branch_latest AS (
        SELECT max(fecha_stock)::date AS max_date
        FROM src.base_stock_sucursal
    ), branch_stock AS (
        SELECT
            l.max_date,
            count(s.*)::bigint AS rows,
            max(s.fecha_extraccion) AS as_of_ts,
            count(*) FILTER (WHERE s.stock IS NULL)::bigint AS null_stock
        FROM branch_latest AS l
        LEFT JOIN src.base_stock_sucursal AS s
          ON s.fecha_stock::date = l.max_date
        GROUP BY l.max_date
    ), assortment AS (
        SELECT
            count(*)::bigint AS rows,
            max(fecha_extraccion) AS as_of_ts,
            count(*) FILTER (
                WHERE c_sucu_empr = 41 AND active_for_purchase = 1
            )::bigint AS cd_purchase_articles
        FROM src.base_productos_vigentes
    ), logistics AS (
        SELECT
            count(*)::bigint AS rows,
            max(fecha_extraccion) AS as_of_ts,
            count(*) FILTER (WHERE c_calidad_peso = 'MISSING')::bigint AS missing_weight,
            count(*) FILTER (WHERE c_calidad_volumen = 'MISSING')::bigint AS missing_volume,
            count(*) FILTER (WHERE c_calidad_pallet = 'MISSING')::bigint AS missing_pallet,
            count(*) FILTER (WHERE c_calidad_embalaje = 'INVALID'
                                  OR c_calidad_peso = 'INVALID'
                                  OR c_calidad_volumen = 'INVALID'
                                  OR c_calidad_pallet = 'INVALID')::bigint AS invalid_quality
        FROM src.v_base_articulos_logistica_actual
    ), open_po AS (
        SELECT
            count(*)::bigint AS rows,
            max(fecha_extraccion) AS as_of_ts,
            count(*) FILTER (WHERE pendientes > 0)::bigint AS positive_lines,
            count(*) FILTER (WHERE pendientes < 0)::bigint AS negative_lines
        FROM src.mv_base_oc_pendientes
    ), audited_open_po_refresh AS (
        SELECT max((d.detail ->> 'refresh_completed_at')::timestamptz) AS refreshed_at
        FROM audit.pdd_source_sync_detail AS d
        JOIN audit.pdd_source_sync_run AS r
          ON r.source_sync_run_uuid = d.source_sync_run_uuid
        WHERE d.source_code = 'OPEN_PURCHASE_ORDERS'
          AND r.refresh_mode = 'FULL'
          AND r.status IN ('READY', 'BLOCKED')
          AND d.detail ? 'refresh_completed_at'
          AND d.detail ->> 'refresh_completed_at' IS NOT NULL
    )
    SELECT
        raw_diarco.max_date AS raw_diarco_date,
        raw_diarco.rows AS raw_diarco_rows,
        raw_barrio.max_date AS raw_barrio_date,
        raw_barrio.rows AS raw_barrio_rows,
        raw_sales.max_date AS raw_sales_date,
        raw_sales.rows AS raw_sales_rows,
        enriched_sales.max_date AS enriched_sales_date,
        enriched_sales.rows AS enriched_sales_rows,
        historical_stock.max_date AS historical_stock_date,
        historical_stock.rows AS historical_stock_rows,
        historical_stock.as_of_ts AS historical_stock_as_of_ts,
        branch_stock.max_date AS branch_stock_date,
        branch_stock.rows AS branch_stock_rows,
        branch_stock.as_of_ts AS branch_stock_as_of_ts,
        branch_stock.null_stock AS branch_stock_nulls,
        assortment.rows AS assortment_rows,
        assortment.as_of_ts AS assortment_as_of_ts,
        assortment.cd_purchase_articles AS assortment_cd_purchase_articles,
        logistics.rows AS logistics_rows,
        logistics.as_of_ts AS logistics_as_of_ts,
        logistics.missing_weight AS logistics_missing_weight,
        logistics.missing_volume AS logistics_missing_volume,
        logistics.missing_pallet AS logistics_missing_pallet,
        logistics.invalid_quality AS logistics_invalid_quality,
        open_po.rows AS open_po_rows,
        open_po.as_of_ts AS open_po_as_of_ts,
        open_po.positive_lines AS open_po_positive_lines,
        open_po.negative_lines AS open_po_negative_lines,
        audited_open_po_refresh.refreshed_at AS open_po_refresh_at,
        (SELECT count(*)::bigint FROM src.m_3_articulos) AS article_master_rows,
        (SELECT count(*)::bigint FROM src.m_1_categorias) AS category_master_rows,
        (SELECT count(*)::bigint FROM src.sucursales_excluidas) AS excluded_branch_rows
    FROM raw_diarco
    CROSS JOIN raw_barrio
    CROSS JOIN raw_sales
    CROSS JOIN enriched_sales
    CROSS JOIN historical_stock
    CROSS JOIN branch_stock
    CROSS JOIN assortment
    CROSS JOIN logistics
    CROSS JOIN open_po
    CROSS JOIN audited_open_po_refresh
    """
)


def read_source_state(engine: Engine) -> dict[str, Any]:
    with engine.connect() as connection:
        return dict(connection.execute(SOURCE_STATE_SQL).mappings().one())


def _check(
    source_code: str,
    relation: str,
    *,
    max_date: date | None,
    as_of_ts: datetime | None,
    row_count: int | None,
    blockers: list[str],
    warnings: list[str] | None = None,
    detail: Mapping[str, Any] | None = None,
    required: bool = True,
) -> SourceCheck:
    warning_values = warnings or []
    status = "BLOCKED" if blockers else ("WARN" if warning_values else "READY")
    return SourceCheck(
        source_code=source_code,
        physical_relation=relation,
        is_required=required,
        status=status,
        max_business_date=max_date,
        as_of_ts=as_of_ts,
        row_count=int(row_count) if row_count is not None else None,
        blocker_codes=tuple(blockers),
        warning_codes=tuple(warning_values),
        detail=detail or {},
    )


def evaluate_source_state(
    state: Mapping[str, Any],
    business_date: date,
    refresh_evidence: Mapping[str, datetime] | None = None,
) -> SourceReadiness:
    """Evalua el contrato que debe estar listo antes del maestro operativo PDD."""
    cutoff = business_date - timedelta(days=1)
    evidence = refresh_evidence or {}
    checks: list[SourceCheck] = []

    def date_blockers(value: date | None, expected: date) -> list[str]:
        if value is None:
            return ["SOURCE_EMPTY"]
        if value < expected:
            return ["SOURCE_DATE_BEHIND"]
        return []

    checks.append(
        _check(
            "RAW_SALES_DIARCO",
            "src.t702_est_vtas_por_articulo",
            max_date=state.get("raw_diarco_date"),
            as_of_ts=None,
            row_count=state.get("raw_diarco_rows"),
            blockers=date_blockers(state.get("raw_diarco_date"), cutoff),
            detail={"required_through": cutoff.isoformat()},
        )
    )
    checks.append(
        _check(
            "RAW_SALES_BARRIO",
            "src.t702_est_vtas_por_articulo_dbarrio",
            max_date=state.get("raw_barrio_date"),
            as_of_ts=None,
            row_count=state.get("raw_barrio_rows"),
            blockers=date_blockers(state.get("raw_barrio_date"), cutoff),
            detail={"required_through": cutoff.isoformat()},
        )
    )
    checks.append(
        _check(
            "EXTENDED_SALES",
            "src.base_ventas_extendida",
            max_date=state.get("raw_sales_date"),
            as_of_ts=None,
            row_count=state.get("raw_sales_rows"),
            blockers=date_blockers(state.get("raw_sales_date"), cutoff),
            detail={"required_through": cutoff.isoformat()},
        )
    )
    checks.append(
        _check(
            "ENRICHED_SALES",
            "datamart.dm_bve_ventas_enriquecidas",
            max_date=state.get("enriched_sales_date"),
            as_of_ts=None,
            row_count=state.get("enriched_sales_rows"),
            blockers=date_blockers(state.get("enriched_sales_date"), cutoff),
            detail={"required_through": cutoff.isoformat()},
        )
    )
    checks.append(
        _check(
            "HISTORICAL_STOCK",
            "src.t710_estadis_stock",
            max_date=state.get("historical_stock_date"),
            as_of_ts=state.get("historical_stock_as_of_ts"),
            row_count=state.get("historical_stock_rows"),
            blockers=date_blockers(state.get("historical_stock_date"), cutoff),
            detail={"required_through": cutoff.isoformat()},
        )
    )

    # SP_BASE_STOCK_EXTEND etiqueta deliberadamente la foto ejecutada en D con
    # fecha_stock=D-1. La vigencia exige ambas condiciones: contenido hasta el
    # cierre D-1 y evidencia de que la extracción se realizó durante D.
    branch_stock_date = state.get("branch_stock_date")
    branch_stock_as_of_ts = state.get("branch_stock_as_of_ts")
    branch_blockers = date_blockers(branch_stock_date, cutoff)
    if not state.get("branch_stock_rows"):
        branch_blockers.append("SOURCE_EMPTY")
    elif (
        branch_stock_as_of_ts is None
        or branch_stock_as_of_ts.date() < business_date
    ):
        branch_blockers.append("REFRESH_NOT_PROVEN_FOR_BUSINESS_DATE")
    if state.get("branch_stock_nulls"):
        branch_blockers.append("NULL_PHYSICAL_STOCK")
    checks.append(
        _check(
            "BRANCH_STOCK",
            "src.base_stock_sucursal",
            max_date=branch_stock_date,
            as_of_ts=branch_stock_as_of_ts,
            row_count=state.get("branch_stock_rows"),
            blockers=sorted(set(branch_blockers)),
            detail={
                "required_stock_through": cutoff.isoformat(),
                "required_extraction_through": business_date.isoformat(),
                "null_stock": int(state.get("branch_stock_nulls") or 0),
            },
        )
    )

    assortment_blockers = [] if state.get("assortment_rows") else ["SOURCE_EMPTY"]
    assortment_warnings = []
    assortment_freshness = evidence.get("ASSORTMENT") or state.get("assortment_as_of_ts")
    if assortment_freshness is None or assortment_freshness.date() < business_date:
        assortment_warnings.append("REFRESH_NOT_PROVEN_FOR_BUSINESS_DATE")
    checks.append(
        _check(
            "ASSORTMENT",
            "src.base_productos_vigentes",
            max_date=None,
            as_of_ts=state.get("assortment_as_of_ts"),
            row_count=state.get("assortment_rows"),
            blockers=assortment_blockers,
            warnings=assortment_warnings,
            detail={
                "cd_purchase_articles": int(
                    state.get("assortment_cd_purchase_articles") or 0
                ),
                "refresh_completed_at": (
                    evidence["ASSORTMENT"].isoformat()
                    if evidence.get("ASSORTMENT")
                    else None
                ),
            },
        )
    )

    logistics_blockers = [] if state.get("logistics_rows") else ["SOURCE_EMPTY"]
    if state.get("logistics_invalid_quality"):
        logistics_blockers.append("INVALID_LOGISTICS_QUALITY")
    logistics_warnings = []
    if state.get("logistics_missing_weight"):
        logistics_warnings.append("WEIGHT_INCOMPLETE")
    if state.get("logistics_missing_volume"):
        logistics_warnings.append("VOLUME_INCOMPLETE")
    if state.get("logistics_missing_pallet"):
        logistics_warnings.append("PALLET_INCOMPLETE")
    checks.append(
        _check(
            "PRODUCT_LOGISTICS",
            "src.v_base_articulos_logistica_actual",
            max_date=None,
            as_of_ts=state.get("logistics_as_of_ts"),
            row_count=state.get("logistics_rows"),
            blockers=logistics_blockers,
            warnings=logistics_warnings,
            detail={
                "missing_weight": int(state.get("logistics_missing_weight") or 0),
                "missing_volume": int(state.get("logistics_missing_volume") or 0),
                "missing_pallet": int(state.get("logistics_missing_pallet") or 0),
                "invalid_quality": int(state.get("logistics_invalid_quality") or 0),
                "refresh_completed_at": (
                    evidence["PRODUCT_LOGISTICS"].isoformat()
                    if evidence.get("PRODUCT_LOGISTICS")
                    else None
                ),
            },
        )
    )

    # fecha_extraccion pertenece a las filas de origen y no prueba que la vista
    # materializada haya sido refrescada. La evidencia válida es la ejecución
    # actual o una corrida FULL auditada anteriormente.
    open_po_freshness = evidence.get("OPEN_PURCHASE_ORDERS") or state.get(
        "open_po_refresh_at"
    )
    open_po_blockers = []
    if open_po_freshness is None:
        open_po_blockers.append("REFRESH_NOT_PROVEN")
    elif open_po_freshness.date() < business_date:
        open_po_blockers.append("REFRESH_DATE_BEHIND")
    checks.append(
        _check(
            "OPEN_PURCHASE_ORDERS",
            "src.mv_base_oc_pendientes",
            max_date=None,
            as_of_ts=state.get("open_po_as_of_ts"),
            row_count=state.get("open_po_rows"),
            blockers=open_po_blockers,
            warnings=(
                ["NEGATIVE_LINES_EXCLUDED"] if state.get("open_po_negative_lines") else []
            ),
            detail={
                "positive_lines": int(state.get("open_po_positive_lines") or 0),
                "negative_lines": int(state.get("open_po_negative_lines") or 0),
                "refresh_completed_at": (
                    evidence["OPEN_PURCHASE_ORDERS"].isoformat()
                    if evidence.get("OPEN_PURCHASE_ORDERS")
                    else None
                ),
            },
        )
    )

    for code, relation, row_key in (
        ("ARTICLE_MASTER", "src.m_3_articulos", "article_master_rows"),
        ("CATEGORY_MASTER", "src.m_1_categorias", "category_master_rows"),
        ("EXCLUDED_BRANCH_POLICY", "src.sucursales_excluidas", "excluded_branch_rows"),
    ):
        checks.append(
            _check(
                code,
                relation,
                max_date=None,
                as_of_ts=None,
                row_count=state.get(row_key),
                blockers=[] if state.get(row_key) else ["SOURCE_EMPTY"],
            )
        )

    closed_dates = [
        state.get("raw_diarco_date"),
        state.get("raw_barrio_date"),
        state.get("raw_sales_date"),
        state.get("enriched_sales_date"),
        state.get("historical_stock_date"),
    ]
    common_closed = min(closed_dates) if all(closed_dates) else None
    recommended = common_closed + timedelta(days=1) if common_closed else None
    blockers = [code for check in checks for code in check.blocker_codes]
    return SourceReadiness(
        business_date=business_date,
        cutoff_date=cutoff,
        status="BLOCKED" if blockers else "READY",
        common_closed_date=common_closed,
        recommended_business_date=recommended,
        checks=tuple(checks),
    )


def _assert_audit_contract(engine: Engine) -> None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    to_regclass(:run_table) IS NOT NULL AS run_exists,
                    to_regclass(:detail_table) IS NOT NULL AS detail_exists
                """
            ),
            {"run_table": AUDIT_RUN_TABLE, "detail_table": AUDIT_DETAIL_TABLE},
        ).mappings().one()
    if not row["run_exists"] or not row["detail_exists"]:
        raise RuntimeError(
            "Falta aplicar scripts/sql/pdd/002_create_pdd_source_sync_audit.sql"
        )


def _insert_run(
    engine: Engine,
    run_uuid: UUID,
    business_date: date,
    refresh_mode: str,
    refresh_options: Mapping[str, Any],
    created_by: str,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                INSERT INTO {AUDIT_RUN_TABLE} (
                    source_sync_run_uuid,business_date,cutoff_date,status,
                    refresh_mode,refresh_options,created_by
                ) VALUES (
                    CAST(:run_uuid AS uuid),:business_date,:cutoff_date,'RUNNING',
                    :refresh_mode,CAST(:refresh_options AS jsonb),:created_by
                )
                """
            ),
            {
                "run_uuid": run_uuid,
                "business_date": business_date,
                "cutoff_date": business_date - timedelta(days=1),
                "refresh_mode": refresh_mode,
                "refresh_options": _json(refresh_options),
                "created_by": created_by,
            },
        )


def _finish_run(
    engine: Engine,
    run_uuid: UUID,
    readiness: SourceReadiness | None,
    *,
    status: str,
    summary: Mapping[str, Any],
    error_message: str | None = None,
) -> None:
    checks = readiness.checks if readiness else ()
    with engine.begin() as connection:
        for check in checks:
            connection.execute(
                text(
                    f"""
                    INSERT INTO {AUDIT_DETAIL_TABLE} (
                        source_sync_run_uuid,source_code,physical_relation,is_required,
                        status,max_business_date,as_of_ts,row_count,blocker_codes,
                        warning_codes,detail
                    ) VALUES (
                        CAST(:run_uuid AS uuid),:source_code,:physical_relation,:is_required,
                        :status,:max_business_date,:as_of_ts,:row_count,
                        CAST(:blocker_codes AS text[]),CAST(:warning_codes AS text[]),
                        CAST(:detail AS jsonb)
                    )
                    """
                ),
                {
                    "run_uuid": run_uuid,
                    "source_code": check.source_code,
                    "physical_relation": check.physical_relation,
                    "is_required": check.is_required,
                    "status": check.status,
                    "max_business_date": check.max_business_date,
                    "as_of_ts": check.as_of_ts,
                    "row_count": check.row_count,
                    "blocker_codes": list(check.blocker_codes),
                    "warning_codes": list(check.warning_codes),
                    "detail": _json(check.detail),
                },
            )
        source_count = len(checks)
        ready_count = sum(check.status == "READY" for check in checks)
        warning_count = sum(check.status == "WARN" for check in checks)
        blocker_count = sum(check.status == "BLOCKED" for check in checks)
        connection.execute(
            text(
                f"""
                UPDATE {AUDIT_RUN_TABLE}
                SET status=:status,
                    finished_at=clock_timestamp(),
                    common_closed_date=:common_closed_date,
                    recommended_business_date=:recommended_business_date,
                    source_count=:source_count,
                    ready_count=:ready_count,
                    warning_count=:warning_count,
                    blocker_count=:blocker_count,
                    summary=CAST(:summary AS jsonb),
                    error_message=:error_message
                WHERE source_sync_run_uuid=CAST(:run_uuid AS uuid)
                """
            ),
            {
                "run_uuid": run_uuid,
                "status": status,
                "common_closed_date": (
                    readiness.common_closed_date if readiness else None
                ),
                "recommended_business_date": (
                    readiness.recommended_business_date if readiness else None
                ),
                "source_count": source_count,
                "ready_count": ready_count,
                "warning_count": warning_count,
                "blocker_count": blocker_count,
                "summary": _json(summary),
                "error_message": error_message[:8000] if error_message else None,
            },
        )


def _refresh_open_purchase_orders(engine: Engine) -> dict[str, Any]:
    with engine.begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_name))"),
            {"lock_name": "pdd.source.refresh.mv_base_oc_pendientes"},
        )
        connection.execute(text("REFRESH MATERIALIZED VIEW src.mv_base_oc_pendientes"))
        return dict(
            connection.execute(
                text(
                    """
                    SELECT clock_timestamp() AS refreshed_at,
                           max(fecha_extraccion) AS source_as_of_ts,
                           count(*)::bigint AS row_count,
                           count(*) FILTER (WHERE pendientes > 0)::bigint AS positive_lines,
                           count(*) FILTER (WHERE pendientes < 0)::bigint AS negative_lines
                    FROM src.mv_base_oc_pendientes
                    """
                )
            ).mappings().one()
        )


def _refresh_sales_pipeline(
    business_date: date,
    *,
    overlap_days: int,
    reconciliation_days: int,
) -> dict[str, Any]:
    """Sincroniza ventas y reconstruye sus derivados para las fechas reparadas."""
    from scripts.send.actualizar_bases_ventas import actualizar_bases_ventas
    from scripts.push.actualizar_base_ventas_extendida import (
        actualizar_base_ventas_extendida,
    )
    from scripts.pull.flujo_procesar_promos_bve import procesar_promos_bve

    sales_sync = actualizar_bases_ventas(
        overlap_days=overlap_days,
        reconcile_lookback_days=reconciliation_days,
    )

    cutoff_date = business_date - timedelta(days=1)
    repaired_dates = [
        date.fromisoformat(value) for value in sales_sync.get("repaired_dates", [])
    ]
    oldest_repaired = min(repaired_dates) if repaired_dates else None
    normal_window_start = cutoff_date - timedelta(days=13)
    if oldest_repaired is not None and oldest_repaired < normal_window_start:
        extended_sales = actualizar_base_ventas_extendida(
            window_days=14,
            analyze=True,
            fecha_desde=oldest_repaired.isoformat(),
            fecha_hasta=cutoff_date.isoformat(),
            modo_reproceso="replace_rango",
        )
    else:
        extended_sales = actualizar_base_ventas_extendida(
            window_days=14,
            analyze=True,
        )

    enriched_start = cutoff_date.replace(day=1)
    if oldest_repaired is not None:
        # El baseline es mensual. Si se reparo un dia historico hay que
        # reconstruir desde el primer dia de ese mes.
        enriched_start = min(
            enriched_start,
            oldest_repaired.replace(day=1),
        )
    enriched_sales = procesar_promos_bve(
        # fecha_hasta es exclusiva. El día 1 debe reconstruir el mes de
        # cutoff_date para no omitir el cierre del mes anterior.
        fecha_desde=enriched_start,
        fecha_hasta=business_date,
        actualizar_base_original=True,
    )
    return {
        "sales_sync": sales_sync,
        "extended_sales": extended_sales,
        "enriched_sales": enriched_sales,
    }


def execute_refreshes(
    engine: Engine,
    business_date: date,
    options: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, datetime]]:
    """Ejecuta subflows existentes en orden causal, mediante imports diferidos."""
    results: dict[str, Any] = {}
    evidence: dict[str, datetime] = {}
    now = lambda: datetime.now(timezone.utc)

    if options["refresh_tabular_sources"]:
        from scripts.send.actualizar_tablas_tabulares import actualizar_tablas_tabulares

        results["tabular_sources"] = actualizar_tablas_tabulares()

    if options["refresh_sales"]:
        reconciliation_enabled = bool(options["force_sales_reconciliation"]) or (
            business_date.weekday() == int(options["sales_reconciliation_weekday"])
        )
        reconciliation_days = (
            int(options["sales_reconciliation_days"])
            if reconciliation_enabled
            else 0
        )
        results.update(
            _refresh_sales_pipeline(
                business_date,
                overlap_days=int(options["sales_overlap_days"]),
                reconciliation_days=reconciliation_days,
            )
        )

    if options["refresh_branch_stock"]:
        from scripts.push.obtener_base_stock import capturar_base_stock

        results["branch_stock"] = capturar_base_stock()

    if options["refresh_assortment"]:
        from scripts.push.obtener_base_productos_vigentes import capturar_base_articulos

        results["assortment"] = capturar_base_articulos()
        evidence["ASSORTMENT"] = now()

    if options["refresh_logistics"]:
        from scripts.pdd.cargar_base_articulos_logistica import (
            DEFAULT_SQLSERVER_PROCEDURE,
            base_articulos_logistica_scd2_flow,
        )

        results["product_logistics"] = base_articulos_logistica_scd2_flow(
            source_mode="sqlserver_sp",
            source_file="",
            stored_procedure=DEFAULT_SQLSERVER_PROCEDURE,
            full_snapshot=True,
            validate_only=False,
            effective_at=None,
            source_name="PDD_SOURCE_DAILY_MASTER",
        )
        evidence["PRODUCT_LOGISTICS"] = now()

    if options["refresh_open_purchase_orders"]:
        results["open_purchase_orders"] = _refresh_open_purchase_orders(engine)
        evidence["OPEN_PURCHASE_ORDERS"] = results["open_purchase_orders"][
            "refreshed_at"
        ]

    return results, evidence


@task(name="PDD - Leer y validar contrato de fuentes", persist_result=False)
def evaluate_sources_task(
    business_date: date,
    refresh_evidence: Mapping[str, datetime],
) -> SourceReadiness:
    engine = build_pg_engine()
    try:
        return evaluate_source_state(
            read_source_state(engine), business_date, refresh_evidence
        )
    finally:
        engine.dispose()


@flow(name="PDD - Sincronizar fuentes diarco_data", persist_result=False)
def pdd_source_daily_flow(
    business_date: date | str | None = None,
    refresh_tabular_sources: bool = True,
    refresh_sales: bool = True,
    refresh_branch_stock: bool = True,
    refresh_assortment: bool = True,
    refresh_logistics: bool = True,
    refresh_open_purchase_orders: bool = True,
    sales_overlap_days: int = 3,
    sales_reconciliation_days: int = 0,
    sales_reconciliation_weekday: int = 6,
    force_sales_reconciliation: bool = False,
    fail_if_not_ready: bool = True,
    created_by: str = "pdd.source.daily",
) -> dict[str, Any]:
    """Actualiza fuentes PDD, audita el corte y falla si no queda consumible."""
    if not created_by.strip():
        raise ValueError("created_by es obligatorio")
    target_date = _parse_business_date(business_date)
    if target_date > datetime.now(LOCAL_TIMEZONE).date():
        raise ValueError(f"business_date no puede ser futura: {target_date}")
    if sales_overlap_days < 1 or sales_overlap_days > 31:
        raise ValueError("sales_overlap_days debe estar entre 1 y 31")
    if (
        sales_reconciliation_days != 0
        and sales_reconciliation_days < sales_overlap_days
    ):
        raise ValueError(
            "sales_reconciliation_days no puede ser menor que sales_overlap_days"
        )
    if sales_reconciliation_days > 366:
        raise ValueError("sales_reconciliation_days no puede superar 366")
    if sales_reconciliation_weekday not in range(7):
        raise ValueError("sales_reconciliation_weekday debe estar entre 0 y 6")

    options = {
        "refresh_tabular_sources": refresh_tabular_sources,
        "refresh_sales": refresh_sales,
        "refresh_branch_stock": refresh_branch_stock,
        "refresh_assortment": refresh_assortment,
        "refresh_logistics": refresh_logistics,
        "refresh_open_purchase_orders": refresh_open_purchase_orders,
        "sales_overlap_days": sales_overlap_days,
        "sales_reconciliation_days": sales_reconciliation_days,
        "sales_reconciliation_weekday": sales_reconciliation_weekday,
        "force_sales_reconciliation": force_sales_reconciliation,
    }
    refresh_mode = (
        "FULL"
        if any(
            value
            for key, value in options.items()
            if key.startswith("refresh_")
        )
        else "VALIDATE_ONLY"
    )
    run_uuid = uuid4()
    engine = build_pg_engine()
    logger = get_run_logger()
    lock_connection = None
    readiness: SourceReadiness | None = None
    step_results: dict[str, Any] = {}

    try:
        _assert_audit_contract(engine)
        lock_connection = engine.connect()
        acquired = lock_connection.execute(
            text("SELECT pg_try_advisory_lock(hashtext(:lock_name))"),
            {"lock_name": LOCK_NAME},
        ).scalar_one()
        lock_connection.commit()
        if not acquired:
            raise RuntimeError("Ya existe una sincronizacion diaria PDD en ejecucion")

        _insert_run(
            engine,
            run_uuid,
            target_date,
            refresh_mode,
            options,
            created_by.strip(),
        )
        logger.info(
            "Sincronizacion PDD iniciada | run=%s | business_date=%s | mode=%s",
            run_uuid,
            target_date,
            refresh_mode,
        )

        step_results, evidence = execute_refreshes(engine, target_date, options)
        readiness = evaluate_sources_task(target_date, evidence)
        summary = {
            "source_sync_run_uuid": str(run_uuid),
            "refresh_mode": refresh_mode,
            "refresh_steps": sorted(step_results),
            "sales_sync": step_results.get("sales_sync"),
            "readiness": readiness.serializable(),
        }
        _finish_run(
            engine,
            run_uuid,
            readiness,
            status=readiness.status,
            summary=summary,
        )
        logger.info(
            "Contrato PDD evaluado | status=%s | common_closed=%s | blockers=%s | warnings=%s",
            readiness.status,
            readiness.common_closed_date,
            readiness.blocker_codes,
            readiness.warning_codes,
        )
        if fail_if_not_ready and readiness.status != "READY":
            raise RuntimeError(
                "Fuentes PDD no listas: " + ", ".join(readiness.blocker_codes)
            )
        return summary
    except Exception as exc:
        if readiness is None:
            try:
                _finish_run(
                    engine,
                    run_uuid,
                    None,
                    status="FAILED",
                    summary={
                        "source_sync_run_uuid": str(run_uuid),
                        "refresh_mode": refresh_mode,
                        "refresh_steps": sorted(step_results),
                    },
                    error_message=str(exc),
                )
            except Exception:
                logger.exception("No se pudo persistir el fallo de sincronizacion PDD")
        raise
    finally:
        if lock_connection is not None:
            try:
                if not lock_connection.closed and not lock_connection.invalidated:
                    lock_connection.execute(
                        text("SELECT pg_advisory_unlock(hashtext(:lock_name))"),
                        {"lock_name": LOCK_NAME},
                    )
                    lock_connection.commit()
            except Exception:
                # El cierre de la sesion libera automaticamente este tipo de
                # lock. El intento de unlock nunca debe ocultar la excepcion
                # causal del pipeline.
                logger.warning(
                    "No se pudo liberar explicitamente el lock PDD; "
                    "la sesion cerrada lo libera automaticamente",
                    exc_info=True,
                )
            finally:
                lock_connection.close()
        engine.dispose()


@flow(name="PDD - Reconciliar ventas semanal", persist_result=False)
def pdd_sales_reconciliation_flow(
    business_date: date | str | None = None,
    sales_overlap_days: int = 3,
    sales_reconciliation_days: int = 45,
    created_by: str = "pdd.sales.reconciliation.weekly",
) -> dict[str, Any]:
    """Reconcilia ventas históricas sin producir una auditoría de fuentes."""
    if not created_by.strip():
        raise ValueError("created_by es obligatorio")
    target_date = _parse_business_date(business_date)
    if target_date > datetime.now(LOCAL_TIMEZONE).date():
        raise ValueError(f"business_date no puede ser futura: {target_date}")
    if sales_overlap_days < 1 or sales_overlap_days > 31:
        raise ValueError("sales_overlap_days debe estar entre 1 y 31")
    if sales_reconciliation_days < sales_overlap_days:
        raise ValueError(
            "sales_reconciliation_days no puede ser menor que sales_overlap_days"
        )
    if sales_reconciliation_days > 366:
        raise ValueError("sales_reconciliation_days no puede superar 366")

    engine = build_pg_engine()
    logger = get_run_logger()
    lock_connection = None
    try:
        lock_connection = engine.connect()
        acquired = lock_connection.execute(
            text("SELECT pg_try_advisory_lock(hashtext(:lock_name))"),
            {"lock_name": LOCK_NAME},
        ).scalar_one()
        lock_connection.commit()
        if not acquired:
            raise RuntimeError(
                "Ya existe una sincronizacion diaria o reconciliacion PDD en ejecucion"
            )

        logger.info(
            "Reconciliacion semanal PDD iniciada | business_date=%s | days=%s | created_by=%s",
            target_date,
            sales_reconciliation_days,
            created_by.strip(),
        )
        results = _refresh_sales_pipeline(
            target_date,
            overlap_days=sales_overlap_days,
            reconciliation_days=sales_reconciliation_days,
        )
        sales_sync = results["sales_sync"]
        summary = {
            "business_date": target_date.isoformat(),
            "sales_overlap_days": sales_overlap_days,
            "sales_reconciliation_days": sales_reconciliation_days,
            "created_by": created_by.strip(),
            **results,
        }
        logger.info(
            "Reconciliacion semanal PDD completada | business_date=%s | repaired_dates=%s",
            target_date,
            sales_sync.get("repaired_dates", []),
        )
        return summary
    finally:
        if lock_connection is not None:
            try:
                if not lock_connection.closed and not lock_connection.invalidated:
                    lock_connection.execute(
                        text("SELECT pg_advisory_unlock(hashtext(:lock_name))"),
                        {"lock_name": LOCK_NAME},
                    )
                    lock_connection.commit()
            except Exception:
                logger.warning(
                    "No se pudo liberar explicitamente el lock PDD semanal; "
                    "la sesion cerrada lo libera automaticamente",
                    exc_info=True,
                )
            finally:
                lock_connection.close()
        engine.dispose()
