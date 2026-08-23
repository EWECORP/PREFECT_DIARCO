"""Sincroniza ventas DIARCO y BARRIO desde SQL Server hacia diarco_data.

La T702 no tiene una clave técnica estable para hacer UPSERT fila por fila. Por
eso la unidad idempotente es el día de venta: se carga una tabla staging y el
reemplazo de las fechas seleccionadas se hace dentro de una única transacción.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Mapping, Sequence

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from sqlalchemy import URL, create_engine, text


load_dotenv(os.environ.get("ETL_ENV_PATH"))

logger = logging.getLogger("replicacion_ventas_logger")
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler(
        "logs/actualizar_base_ventas.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


@dataclass(frozen=True)
class SalesSource:
    code: str
    sql_table: str
    upstream_table: str
    pg_table: str
    staging_table: str
    stored_procedure: str
    sql_lock_resource: str


SALES_SOURCES = (
    SalesSource(
        code="DIARCO",
        sql_table="T702_EST_VTAS_POR_ARTICULO",
        upstream_table=(
            "[DCO-DBCORE-P02].[DiarcoEst].[dbo]."
            "[T702_EST_VTAS_POR_ARTICULO]"
        ),
        pg_table="t702_est_vtas_por_articulo",
        staging_table="_stg_t702_est_vtas_por_articulo",
        stored_procedure="repl.usp_replicar_T702_EST_VTAS_POR_ARTICULO",
        sql_lock_resource="repl.T702_EST_VTAS_POR_ARTICULO.refresh",
    ),
    SalesSource(
        code="BARRIO",
        sql_table="T702_EST_VTAS_POR_ARTICULO_DBARRIO",
        upstream_table=(
            "[DCO-DBCORE-P02].[DiarcoEst].[dbo]."
            "[T702_EST_VTAS_POR_ARTICULO_DBARRIO]"
        ),
        pg_table="t702_est_vtas_por_articulo_dbarrio",
        staging_table="_stg_t702_est_vtas_por_articulo_dbarrio",
        stored_procedure="repl.usp_replicar_T702_EST_VTAS_POR_ARTICULO_BARRIO",
        sql_lock_resource="repl.T702_EST_VTAS_POR_ARTICULO_DBARRIO.refresh",
    ),
)
SOURCE_BY_CODE = {source.code: source for source in SALES_SOURCES}
LOCK_NAME = "etl_diarco.sales_t702_atomic_refresh"
AGGREGATE_SCALE = Decimal("0.000001")
SQL_SALES_COLUMNS = (
    "F_VENTA",
    "C_ARTICULO",
    "C_FAMILIA",
    "C_SUCU_EMPR",
    "I_PRECIO_VENTA",
    "I_PRECIO_COSTO",
    "I_VENDIDO",
    "Q_UNIDADES_VENDIDAS",
    "I_PRECIO_COSTO_PP",
    "I_PARTE_ULTIMO_INGRESO",
    "I_COMPRA_ULTIMO_INGRESO",
    "I_IMP_INTERNOS",
)
BOOTSTRAP_DATES = {
    "DIARCO": date(2025, 5, 1),
    "BARRIO": date(2025, 6, 1),
}


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variable requerida no configurada: {name}")
    return value


def build_sql_engine():
    url = URL.create(
        "mssql+pyodbc",
        username=_required("SQL_USER"),
        password=_required("SQL_PASSWORD"),
        host=_required("SQL_SERVER"),
        database=os.getenv("SQL_REPL_DATABASE", "data-sync"),
        query={"driver": "ODBC Driver 17 for SQL Server"},
    )
    return create_engine(url, pool_pre_ping=True)


def open_pg_conn():
    return psycopg2.connect(
        dbname=_required("PG_DB"),
        user=_required("PG_USER"),
        password=_required("PG_PASSWORD"),
        host=_required("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        application_name="etl_diarco:actualizar_bases_ventas",
    )


def _as_date(value: Any) -> date:
    return value.date() if isinstance(value, datetime) else value


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0)).quantize(AGGREGATE_SCALE)


def normalize_aggregate_rows(
    rows: Sequence[Mapping[str, Any]],
) -> dict[date, dict[str, Any]]:
    return {
        _as_date(row["sales_date"]): {
            "rows": int(row["rows"] or 0),
            "units": _decimal(row["units"]),
            "amount": _decimal(row["amount"]),
        }
        for row in rows
    }


def find_mismatched_dates(
    source: Mapping[date, Mapping[str, Any]],
    target: Mapping[date, Mapping[str, Any]],
) -> list[date]:
    """Compara los mismos controles operativos, agregando también el importe."""
    return sorted(
        sales_date
        for sales_date in set(source) | set(target)
        if source.get(sales_date) != target.get(sales_date)
    )


def rolling_dates(max_date: date, overlap_days: int) -> list[date]:
    if overlap_days < 1:
        raise ValueError("overlap_days debe ser mayor o igual a 1")
    start = max_date - timedelta(days=overlap_days - 1)
    return [start + timedelta(days=index) for index in range(overlap_days)]


def inclusive_dates(start_date: date, end_date: date) -> list[date]:
    if start_date > end_date:
        raise ValueError("start_date no puede ser posterior a end_date")
    days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=index) for index in range(days)]


def replica_refresh_start(previous_max_date: date | None, source_code: str) -> date:
    """Replica el mismo limite ``> max(F_VENTA)-3`` de los procedimientos."""
    reference = previous_max_date or BOOTSTRAP_DATES[source_code]
    return reference - timedelta(days=2)


def _date_filter(dates: Sequence[date]) -> str:
    if not dates:
        raise ValueError("Debe existir al menos una fecha para replicar")
    selected_dates = sorted(set(dates))
    values = ",".join(f"'{value.isoformat()}'" for value in selected_dates)
    return (
        f"F_VENTA >= '{selected_dates[0].isoformat()}' "
        f"AND F_VENTA < DATEADD(day, 1, '{selected_dates[-1].isoformat()}') "
        f"AND CAST(F_VENTA AS date) IN ({values})"
    )


@task(name="Actualizar réplica SQL Server T702", retries=2, retry_delay_seconds=60)
def refresh_sql_replica(
    source_code: str,
    reconcile_lookback_days: int = 0,
) -> dict[str, Any]:
    source = SOURCE_BY_CODE[source_code]
    engine = build_sql_engine()
    started_at = datetime.now()
    try:
        with engine.begin() as connection:
            previous_max_date = connection.execute(
                text(f"SELECT MAX(F_VENTA) FROM repl.{source.sql_table}")
            ).scalar_one()
            connection.exec_driver_sql(f"EXEC {source.stored_procedure}")
            max_date = connection.execute(
                text(f"SELECT MAX(F_VENTA) FROM repl.{source.sql_table}")
            ).scalar_one()
        if max_date is None:
            raise RuntimeError(f"La réplica SQL Server {source.code} quedó vacía")
        max_date = _as_date(max_date)
        historical_mismatches: list[date] = []
        reconciliation_from = None
        if reconcile_lookback_days:
            reconciliation_from = max_date - timedelta(
                days=reconcile_lookback_days - 1
            )
            upstream_totals = _read_sql_relation_aggregates(
                engine,
                source.upstream_table,
                reconciliation_from,
                max_date,
            )
            replica_totals = _read_sql_relation_aggregates(
                engine,
                f"repl.{source.sql_table}",
                reconciliation_from,
                max_date,
            )
            historical_mismatches = find_mismatched_dates(
                upstream_totals,
                replica_totals,
            )
            if historical_mismatches:
                _repair_sql_replica_dates(
                    engine,
                    source,
                    historical_mismatches,
                    upstream_totals,
                )
    finally:
        engine.dispose()
    return {
        "source_code": source.code,
        "max_date": max_date.isoformat(),
        "previous_max_date": (
            _as_date(previous_max_date).isoformat() if previous_max_date else None
        ),
        "refresh_from_date": replica_refresh_start(
            _as_date(previous_max_date) if previous_max_date else None,
            source.code,
        ).isoformat(),
        "reconciliation_from": (
            reconciliation_from.isoformat() if reconciliation_from else None
        ),
        "historical_mismatch_dates": [
            value.isoformat() for value in historical_mismatches
        ],
        "duration_seconds": round((datetime.now() - started_at).total_seconds(), 2),
    }


def _read_sql_relation_aggregates(
    engine,
    relation: str,
    start_date: date,
    end_date: date,
) -> dict[date, dict[str, Any]]:
    with engine.connect() as connection:
        return _read_sql_relation_aggregates_on_connection(
            connection,
            relation,
            start_date,
            end_date,
        )


def _read_sql_relation_aggregates_on_connection(
    connection,
    relation: str,
    start_date: date,
    end_date: date,
) -> dict[date, dict[str, Any]]:
    rows = connection.execute(
        text(
            f"""
                SELECT
                    CAST(F_VENTA AS date) AS sales_date,
                    COUNT_BIG(*) AS rows,
                    COALESCE(SUM(CAST(Q_UNIDADES_VENDIDAS AS decimal(38,6))), 0)
                        AS units,
                    COALESCE(SUM(CAST(I_VENDIDO AS decimal(38,6))), 0) AS amount
                FROM {relation}
                WHERE F_VENTA >= :start_date
                  AND F_VENTA < DATEADD(day, 1, :end_date)
                GROUP BY CAST(F_VENTA AS date)
            """
        ),
        {"start_date": start_date, "end_date": end_date},
    ).mappings().all()
    return normalize_aggregate_rows(rows)


def _read_sql_aggregates(
    source: SalesSource, start_date: date, end_date: date
) -> dict[date, dict[str, Any]]:
    engine = build_sql_engine()
    try:
        return _read_sql_relation_aggregates(
            engine,
            f"repl.{source.sql_table}",
            start_date,
            end_date,
        )
    finally:
        engine.dispose()


def _repair_sql_replica_dates(
    engine,
    source: SalesSource,
    dates: Sequence[date],
    expected: Mapping[date, Mapping[str, Any]],
) -> dict[str, Any]:
    selected_dates = sorted(set(dates))
    date_values = ",".join(
        f"'{value.isoformat()}'" for value in selected_dates
    )
    columns = ",".join(f"[{column}]" for column in SQL_SALES_COLUMNS)
    predicate = (
        f"F_VENTA >= '{selected_dates[0].isoformat()}' "
        f"AND F_VENTA < DATEADD(day, 1, '{selected_dates[-1].isoformat()}') "
        f"AND CONVERT(date, F_VENTA) IN ({date_values})"
    )
    lock_statement = """
        DECLARE @lock_result int;
        EXEC @lock_result = sys.sp_getapplock
            @Resource = :lock_resource,
            @LockMode = 'Exclusive',
            @LockOwner = 'Transaction',
            @LockTimeout = 0;
        IF @lock_result < 0
            THROW 51000, 'Ya existe una sincronizacion T702 en ejecucion', 1;
    """
    with engine.begin() as connection:
        connection.execute(
            text(lock_statement),
            {"lock_resource": source.sql_lock_resource},
        )
        deleted_rows = connection.execute(
            text(
                f"DELETE FROM repl.{source.sql_table} "
                f"WHERE {predicate}"
            )
        ).rowcount
        inserted_rows = connection.execute(
            text(
                f"INSERT INTO repl.{source.sql_table} ({columns}) "
                f"SELECT {columns} FROM {source.upstream_table} "
                f"WHERE {predicate}"
            )
        ).rowcount
        actual = _read_sql_relation_aggregates_on_connection(
            connection,
            f"repl.{source.sql_table}",
            selected_dates[0],
            selected_dates[-1],
        )
        expected_selected = {
            key: value for key, value in expected.items() if key in selected_dates
        }
        actual = {
            key: value for key, value in actual.items() if key in selected_dates
        }
        mismatches = find_mismatched_dates(expected_selected, actual)
        if mismatches:
            raise RuntimeError(
                f"La reparación SQL Server {source.code} no pasó el control: "
                + ", ".join(value.isoformat() for value in mismatches)
            )
    return {
        "source_code": source.code,
        "repaired_dates": [value.isoformat() for value in selected_dates],
        "deleted_rows": deleted_rows,
        "inserted_rows": inserted_rows,
    }


def _read_pg_aggregates(
    source: SalesSource, start_date: date, end_date: date
) -> dict[date, dict[str, Any]]:
    with open_pg_conn() as connection, connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                SELECT
                    f_venta::date AS sales_date,
                    COUNT(*)::bigint AS rows,
                    COALESCE(SUM(q_unidades_vendidas), 0) AS units,
                    COALESCE(SUM(i_vendido), 0) AS amount
                FROM src.{}
                WHERE f_venta >= %s AND f_venta < %s
                GROUP BY f_venta::date
                """
            ).format(sql.Identifier(source.pg_table)),
            (start_date, end_date + timedelta(days=1)),
        )
        columns = [description.name for description in cursor.description]
        return normalize_aggregate_rows(
            [dict(zip(columns, row)) for row in cursor.fetchall()]
        )


def _prepare_staging(connection, source: SalesSource) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS src.{}").format(
                sql.Identifier(source.staging_table)
            )
        )
        cursor.execute(
            sql.SQL(
                "CREATE UNLOGGED TABLE src.{} "
                "(LIKE src.{} INCLUDING DEFAULTS INCLUDING GENERATED)"
            ).format(
                sql.Identifier(source.staging_table),
                sql.Identifier(source.pg_table),
            )
        )
    connection.commit()


def _target_columns(cursor, source: SalesSource) -> list[str]:
    cursor.execute(
        """
        SELECT a.attname
        FROM pg_attribute AS a
        WHERE a.attrelid = %s::regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND a.attgenerated = ''
        ORDER BY a.attnum
        """,
        (f"src.{source.pg_table}",),
    )
    columns = [row[0] for row in cursor.fetchall()]
    if not columns:
        raise RuntimeError(f"No se obtuvieron columnas para src.{source.pg_table}")
    return columns


def _copy_value(value: Any) -> Any:
    if value is None:
        return "__PDD_NULL__"
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        return "\\x" + value.hex()
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _sql_identifier(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _load_staging_direct(
    pg_connection,
    source: SalesSource,
    dates: Sequence[date],
    chunk_size: int = 50_000,
) -> dict[str, Any]:
    """Transfiere SQL Server -> staging PostgreSQL sin ZIP ni SFTP."""
    engine = build_sql_engine()
    raw_connection = None
    sql_cursor = None
    loaded_rows = 0
    try:
        raw_connection = engine.raw_connection()
        sql_cursor = raw_connection.cursor()
        sql_cursor.execute(f"SELECT TOP 0 * FROM repl.{source.sql_table}")
        source_columns = [
            description[0].lower() for description in sql_cursor.description
        ]
        with pg_connection.cursor() as pg_cursor:
            target_columns = _target_columns(pg_cursor, source)
        target_column_set = set(target_columns)
        selected_columns = [
            column for column in source_columns if column in target_column_set
        ]
        required_columns = {
            "f_venta",
            "c_articulo",
            "c_familia",
            "c_sucu_empr",
            "i_vendido",
            "q_unidades_vendidas",
        }
        missing_required = sorted(required_columns - set(selected_columns))
        if missing_required:
            raise RuntimeError(
                f"Columnas T702 requeridas ausentes en {source.code}: {missing_required}"
            )

        select_list = ",".join(_sql_identifier(column) for column in selected_columns)
        query = (
            f"SELECT {select_list} FROM repl.{source.sql_table} "
            f"WHERE {_date_filter(dates)}"
        )
        sql_cursor.execute(query)

        copy_columns = sql.SQL(",").join(map(sql.Identifier, selected_columns))
        copy_statement = sql.SQL(
            "COPY src.{} ({}) FROM STDIN WITH ("
            "FORMAT CSV, DELIMITER '|', NULL '__PDD_NULL__', QUOTE '\"', ESCAPE '\"')"
        ).format(sql.Identifier(source.staging_table), copy_columns)

        while True:
            rows = sql_cursor.fetchmany(chunk_size)
            if not rows:
                break
            buffer = io.StringIO()
            writer = csv.writer(
                buffer,
                delimiter="|",
                quotechar='"',
                escapechar='"',
                lineterminator="\n",
            )
            writer.writerows([_copy_value(value) for value in row] for row in rows)
            buffer.seek(0)
            with pg_connection.cursor() as pg_cursor:
                pg_cursor.copy_expert(copy_statement.as_string(pg_connection), buffer)
            loaded_rows += len(rows)
    finally:
        if sql_cursor is not None:
            sql_cursor.close()
        if raw_connection is not None:
            raw_connection.close()
        engine.dispose()

    return {
        "mode": "DIRECT_COPY",
        "chunk_size": chunk_size,
        "loaded_rows": loaded_rows,
    }


def _publish_staging_atomically(
    connection,
    source: SalesSource,
    dates: Sequence[date],
    expected: Mapping[date, Mapping[str, Any]],
) -> dict[str, Any]:
    selected_dates = sorted(set(dates))
    with connection.cursor() as cursor:
        columns = _target_columns(cursor, source)
        identifiers = sql.SQL(",").join(map(sql.Identifier, columns))
        cursor.execute(
            sql.SQL(
                "DELETE FROM src.{} "
                "WHERE f_venta >= %s AND f_venta < %s "
                "AND f_venta::date = ANY(%s)"
            ).format(sql.Identifier(source.pg_table)),
            (
                selected_dates[0],
                selected_dates[-1] + timedelta(days=1),
                selected_dates,
            ),
        )
        deleted_rows = cursor.rowcount
        cursor.execute(
            sql.SQL("INSERT INTO src.{} ({}) SELECT {} FROM src.{}").format(
                sql.Identifier(source.pg_table),
                identifiers,
                identifiers,
                sql.Identifier(source.staging_table),
            )
        )
        inserted_rows = cursor.rowcount

        cursor.execute(
            sql.SQL(
                """
                SELECT f_venta::date AS sales_date,
                       COUNT(*)::bigint AS rows,
                       COALESCE(SUM(q_unidades_vendidas), 0) AS units,
                       COALESCE(SUM(i_vendido), 0) AS amount
                FROM src.{}
                WHERE f_venta >= %s AND f_venta < %s
                  AND f_venta::date = ANY(%s)
                GROUP BY f_venta::date
                """
            ).format(sql.Identifier(source.pg_table)),
            (
                selected_dates[0],
                selected_dates[-1] + timedelta(days=1),
                selected_dates,
            ),
        )
        names = [description.name for description in cursor.description]
        actual = normalize_aggregate_rows(
            [dict(zip(names, row)) for row in cursor.fetchall()]
        )
        expected_selected = {
            key: value for key, value in expected.items() if key in selected_dates
        }
        mismatches = find_mismatched_dates(expected_selected, actual)
        if mismatches:
            raise RuntimeError(
                f"Control posterior inconsistente para {source.code}: "
                + ", ".join(value.isoformat() for value in mismatches)
            )
    connection.commit()
    return {"deleted_rows": deleted_rows, "inserted_rows": inserted_rows}


def _drop_staging(connection, source: SalesSource) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS src.{}").format(
                sql.Identifier(source.staging_table)
            )
        )
    connection.commit()


def _sync_source(
    source: SalesSource,
    max_date: date,
    replica_from_date: date,
    replica_to_date: date,
    overlap_days: int,
    reconcile_lookback_days: int,
    connection,
) -> dict[str, Any]:
    recent_dates = rolling_dates(max_date, overlap_days)
    replica_dates = inclusive_dates(replica_from_date, replica_to_date)
    mismatch_dates: list[date] = []
    reconciliation_from = None
    if reconcile_lookback_days:
        if reconcile_lookback_days < overlap_days:
            raise ValueError("reconcile_lookback_days no puede ser menor que overlap_days")
        reconciliation_from = max_date - timedelta(days=reconcile_lookback_days - 1)
        source_totals = _read_sql_aggregates(source, reconciliation_from, max_date)
        target_totals = _read_pg_aggregates(source, reconciliation_from, max_date)
        mismatch_dates = find_mismatched_dates(source_totals, target_totals)

    refresh_dates = sorted(
        set(replica_dates) | set(recent_dates) | set(mismatch_dates)
    )
    expected = _read_sql_aggregates(source, min(refresh_dates), max(refresh_dates))
    expected = {key: value for key, value in expected.items() if key in refresh_dates}

    _prepare_staging(connection, source)
    try:
        transfer = _load_staging_direct(
            connection,
            source,
            refresh_dates,
        )
        publication = _publish_staging_atomically(
            connection, source, refresh_dates, expected
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        _drop_staging(connection, source)

    return {
        "source_code": source.code,
        "max_date": max_date.isoformat(),
        "replica_from_date": replica_from_date.isoformat(),
        "replica_to_date": replica_to_date.isoformat(),
        "overlap_days": overlap_days,
        "reconciliation_from": (
            reconciliation_from.isoformat() if reconciliation_from else None
        ),
        "mismatch_dates": [value.isoformat() for value in mismatch_dates],
        "repaired_dates": [value.isoformat() for value in refresh_dates],
        "transfer": transfer,
        **publication,
    }


@flow(name="actualizar_bases_ventas", log_prints=True, persist_result=False)
def actualizar_bases_ventas(
    overlap_days: int = 3,
    reconcile_lookback_days: int = 0,
) -> dict[str, Any]:
    """Actualiza ambas fuentes; opcionalmente reconcilia y repara días antiguos."""
    if overlap_days < 1 or overlap_days > 31:
        raise ValueError("overlap_days debe estar entre 1 y 31")
    if reconcile_lookback_days < 0 or reconcile_lookback_days > 366:
        raise ValueError("reconcile_lookback_days debe estar entre 0 y 366")

    log = get_run_logger()
    connection = open_pg_conn()
    acquired = False
    results: dict[str, Any] = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s))",
                (LOCK_NAME,),
            )
            acquired = bool(cursor.fetchone()[0])
        if not acquired:
            raise RuntimeError("Ya existe una sincronización T702 en ejecución")

        replicas = {
            source.code: refresh_sql_replica.submit(
                source.code,
                reconcile_lookback_days,
            ).result()
            for source in SALES_SOURCES
        }
        for source in SALES_SOURCES:
            results[source.code] = _sync_source(
                source,
                date.fromisoformat(replicas[source.code]["max_date"]),
                date.fromisoformat(replicas[source.code]["refresh_from_date"]),
                max(
                    date.fromisoformat(replicas[source.code]["max_date"]),
                    date.fromisoformat(
                        replicas[source.code]["previous_max_date"]
                        or replicas[source.code]["max_date"]
                    ),
                ),
                overlap_days,
                reconcile_lookback_days,
                connection,
            )

        repaired_dates = sorted(
            {
                value
                for result in results.values()
                for value in result["repaired_dates"]
            }
        )
        summary = {
            "overlap_days": overlap_days,
            "reconcile_lookback_days": reconcile_lookback_days,
            "sql_replicas": replicas,
            "sources": results,
            "repaired_dates": repaired_dates,
            "oldest_repaired_date": repaired_dates[0] if repaired_dates else None,
            "newest_repaired_date": repaired_dates[-1] if repaired_dates else None,
        }
        log.info("Sincronización T702 completada | %s", summary)
        return summary
    finally:
        if acquired:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s))",
                        (LOCK_NAME,),
                    )
            finally:
                connection.close()
        else:
            connection.close()


if __name__ == "__main__":
    actualizar_bases_ventas()
