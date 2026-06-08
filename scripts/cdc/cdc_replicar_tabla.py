from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable

import psycopg2
import pyodbc
from dotenv import load_dotenv
from prefect import flow, get_run_logger
from psycopg2 import sql
from psycopg2.extras import execute_values


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ENV_PATH = os.environ.get("ETL_ENV_PATH", str(PROJECT_ROOT / ".env"))
load_dotenv(ENV_PATH)

@dataclass(frozen=True)
class TableConfig:
    config_name: str
    source_server: str
    source_database: str
    source_schema: str
    source_table: str
    capture_instance: str
    source_driver_env: str
    source_port_env: str | None
    source_user_env: str
    source_password_env: str
    target_schema: str
    target_table: str
    pk_columns: tuple[str, ...]
    enabled: bool
    mode: str
    poll_seconds: int
    batch_size: int
    notes: str | None

    @property
    def source_label(self) -> str:
        return (
            f"{self.source_server}."
            f"{self.source_database}."
            f"{self.source_schema}."
            f"{self.source_table}"
        )


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno requerida: {name}")
    return value


def normalize_lsn(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, bytes):
        return value
    raise TypeError(f"Tipo de LSN no soportado: {type(value).__name__}")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def open_pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        dbname=require_env("PG_DB"),
        user=require_env("PG_USER"),
        password=require_env("PG_PASSWORD"),
        host=require_env("PG_HOST"),
        port=require_env("PG_PORT"),
    )


def open_sqlserver_conn(config: TableConfig) -> pyodbc.Connection:
    driver = os.getenv(config.source_driver_env, "ODBC Driver 17 for SQL Server")
    port = os.getenv(config.source_port_env) if config.source_port_env else None
    user = require_env(config.source_user_env)
    password = require_env(config.source_password_env)
    server = config.source_server
    if port and "," not in server and "\\" not in server:
        server = f"{server},{port}"
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={config.source_database};"
        f"UID={user};"
        f"PWD={password}"
    )
    return pyodbc.connect(conn_str)


def parse_table_config(row: tuple[Any, ...]) -> TableConfig:
    return TableConfig(
        config_name=row[0],
        source_server=row[1],
        source_database=row[2],
        source_schema=row[3],
        source_table=row[4],
        capture_instance=row[5],
        source_driver_env=row[6],
        source_port_env=row[7],
        source_user_env=row[8],
        source_password_env=row[9],
        target_schema=row[10],
        target_table=row[11],
        pk_columns=tuple(column.lower() for column in row[12]),
        enabled=row[13],
        mode=row[14],
        poll_seconds=row[15],
        batch_size=row[16],
        notes=row[17],
    )


def format_lsn(lsn: bytes | None) -> str | None:
    lsn = normalize_lsn(lsn)
    if lsn is None:
        return None
    return "0x" + lsn.hex().upper()


def ensure_state_row(pg_conn: psycopg2.extensions.connection, config_name: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl.cdc_state (config_name, last_status, updated_at)
            VALUES (%s, 'never_run', now())
            ON CONFLICT (config_name) DO NOTHING
            """,
            (config_name,),
        )
    pg_conn.commit()


def get_table_config(pg_conn: psycopg2.extensions.connection, config_name: str) -> TableConfig:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                config_name,
                source_server,
                source_database,
                source_schema,
                source_table,
                capture_instance,
                source_driver_env,
                source_port_env,
                source_user_env,
                source_password_env,
                target_schema,
                target_table,
                pk_columns,
                enabled,
                mode,
                poll_seconds,
                batch_size,
                notes
            FROM etl.cdc_table_config
            WHERE config_name = %s
            """,
            (config_name,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"No existe configuracion CDC para: {config_name}")
    return parse_table_config(row)


def get_state(
    pg_conn: psycopg2.extensions.connection,
    config_name: str,
) -> dict[str, Any]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                last_start_lsn,
                last_end_lsn,
                last_status,
                last_rowcount,
                last_error
            FROM etl.cdc_state
            WHERE config_name = %s
            """,
            (config_name,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"No existe estado CDC para: {config_name}")
    return {
        "last_start_lsn": normalize_lsn(row[0]),
        "last_end_lsn": normalize_lsn(row[1]),
        "last_status": row[2],
        "last_rowcount": row[3],
        "last_error": row[4],
    }


def update_state(
    pg_conn: psycopg2.extensions.connection,
    config_name: str,
    *,
    last_start_lsn: bytes | None,
    last_end_lsn: bytes | None,
    last_status: str,
    last_rowcount: int,
    last_error: str | None,
    last_started_at: datetime,
    last_finished_at: datetime,
) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl.cdc_state
            SET last_start_lsn = %s,
                last_end_lsn = %s,
                last_status = %s,
                last_rowcount = %s,
                last_error = %s,
                last_started_at = %s,
                last_finished_at = %s,
                updated_at = now()
            WHERE config_name = %s
            """,
            (
                last_start_lsn,
                last_end_lsn,
                last_status,
                last_rowcount,
                last_error,
                last_started_at,
                last_finished_at,
                config_name,
            ),
        )


def insert_run_log(
    pg_conn: psycopg2.extensions.connection,
    config: TableConfig,
    *,
    from_lsn: bytes | None,
    to_lsn: bytes | None,
    rows_read: int,
    rows_upserted: int,
    rows_deleted: int,
    status: str,
    duration_ms: int,
    error_text: str | None,
) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl.cdc_run_log (
                config_name,
                source_table,
                from_lsn,
                to_lsn,
                rows_read,
                rows_upserted,
                rows_deleted,
                status,
                duration_ms,
                error_text
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                config.config_name,
                f"{config.source_schema}.{config.source_table}",
                from_lsn,
                to_lsn,
                rows_read,
                rows_upserted,
                rows_deleted,
                status,
                duration_ms,
                error_text,
            ),
        )


def get_min_lsn(sql_conn: pyodbc.Connection, capture_instance: str) -> bytes:
    cur = sql_conn.cursor()
    cur.execute("SELECT sys.fn_cdc_get_min_lsn(?)", capture_instance)
    row = cur.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"No se pudo obtener min LSN para {capture_instance}")
    return row[0]


def get_max_lsn(sql_conn: pyodbc.Connection) -> bytes:
    cur = sql_conn.cursor()
    cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
    row = cur.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("No se pudo obtener max LSN")
    return row[0]


def increment_lsn(sql_conn: pyodbc.Connection, lsn: bytes) -> bytes:
    cur = sql_conn.cursor()
    cur.execute("SELECT sys.fn_cdc_increment_lsn(?)", lsn)
    row = cur.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("No se pudo calcular el siguiente LSN")
    return row[0]


def get_target_columns(
    pg_conn: psycopg2.extensions.connection,
    schema_name: str,
    table_name: str,
) -> list[str]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError(f"No existe la tabla destino {schema_name}.{table_name}")
    return [row[0] for row in rows]


def fetch_cdc_rows(
    sql_conn: pyodbc.Connection,
    capture_instance: str,
    from_lsn: bytes,
    to_lsn: bytes,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    query = f"""
    SELECT *
    FROM cdc.fn_cdc_get_all_changes_{capture_instance}(?, ?, 'all')
    ORDER BY __$start_lsn, __$seqval
    """
    cur = sql_conn.cursor()
    cur.execute(query, from_lsn, to_lsn)
    columns = [column[0] for column in cur.description]
    rows = cur.fetchall()
    return columns, rows


def collapse_changes(
    columns: list[str],
    rows: Iterable[tuple[Any, ...]],
    config: TableConfig,
    target_columns: list[str],
) -> tuple[list[dict[str, Any]], list[tuple[Any, ...]], int]:
    extracted_at = datetime.now()
    normalized_columns = [column.lower() for column in columns]
    pending: dict[tuple[Any, ...], tuple[str, dict[str, Any] | tuple[Any, ...]]] = {}
    rowcount = 0

    for row in rows:
        rowcount += 1
        row_map = {
            normalized_columns[idx]: row[idx]
            for idx in range(len(normalized_columns))
        }
        operation = row_map["__$operation"]
        pk_value = tuple(row_map[column] for column in config.pk_columns)

        if operation == 1:
            pending[pk_value] = ("delete", pk_value)
            continue

        if operation not in (2, 4):
            continue

        payload: dict[str, Any] = {}
        for column in target_columns:
            if column == "fuente_origen":
                payload[column] = config.source_label
            elif column == "fecha_extraccion":
                payload[column] = extracted_at
            elif column == "cdc_lsn":
                payload[column] = normalize_lsn(row_map["__$start_lsn"])
            elif column == "estado_sincronizacion":
                payload[column] = 0
            else:
                payload[column] = row_map.get(column)

        pending[pk_value] = ("upsert", payload)

    upserts: list[dict[str, Any]] = []
    deletes: list[tuple[Any, ...]] = []

    for action, data in pending.values():
        if action == "upsert":
            upserts.append(data)  # type: ignore[arg-type]
        else:
            deletes.append(data)  # type: ignore[arg-type]

    return upserts, deletes, rowcount


def create_temp_like_target(
    cur: psycopg2.extensions.cursor,
    temp_table_name: str,
    target_schema: str,
    target_table: str,
) -> None:
    cur.execute(
        sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(temp_table_name))
    )
    cur.execute(
        sql.SQL(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP"
        ).format(
            sql.Identifier(temp_table_name),
            sql.Identifier(target_schema, target_table),
        )
    )


def apply_upserts(
    pg_conn: psycopg2.extensions.connection,
    config: TableConfig,
    target_columns: list[str],
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0

    temp_table = "tmp_cdc_upsert"
    insert_values = [
        tuple(row[column] for column in target_columns)
        for row in rows
    ]
    update_columns = [
        column for column in target_columns
        if column not in config.pk_columns
    ]

    with pg_conn.cursor() as cur:
        create_temp_like_target(cur, temp_table, config.target_schema, config.target_table)

        execute_values(
            cur,
            sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(temp_table),
                sql.SQL(", ").join(sql.Identifier(column) for column in target_columns),
            ).as_string(pg_conn),
            insert_values,
            page_size=max(100, config.batch_size),
        )

        if update_columns:
            conflict_action = sql.SQL("DO UPDATE SET {}").format(
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(column),
                        sql.Identifier(column),
                    )
                    for column in update_columns
                )
            )
        else:
            conflict_action = sql.SQL("DO NOTHING")

        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} ({})
                SELECT {} FROM {}
                ON CONFLICT ({})
                {}
                """
            ).format(
                sql.Identifier(config.target_schema, config.target_table),
                sql.SQL(", ").join(sql.Identifier(column) for column in target_columns),
                sql.SQL(", ").join(sql.Identifier(column) for column in target_columns),
                sql.Identifier(temp_table),
                sql.SQL(", ").join(sql.Identifier(column) for column in config.pk_columns),
                conflict_action,
            )
        )

    return len(rows)


def apply_deletes(
    pg_conn: psycopg2.extensions.connection,
    config: TableConfig,
    keys: list[tuple[Any, ...]],
) -> int:
    if not keys:
        return 0

    temp_table = "tmp_cdc_delete"
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(temp_table))
        )
        cur.execute(
            sql.SQL(
                "CREATE TEMP TABLE {} AS SELECT {} FROM {} WHERE 1 = 0"
            ).format(
                sql.Identifier(temp_table),
                sql.SQL(", ").join(sql.Identifier(column) for column in config.pk_columns),
                sql.Identifier(config.target_schema, config.target_table),
            )
        )

        execute_values(
            cur,
            sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(temp_table),
                sql.SQL(", ").join(sql.Identifier(column) for column in config.pk_columns),
            ).as_string(pg_conn),
            keys,
            page_size=max(100, config.batch_size),
        )

        cur.execute(
            sql.SQL(
                """
                DELETE FROM {} AS target
                USING {} AS src
                WHERE {}
                """
            ).format(
                sql.Identifier(config.target_schema, config.target_table),
                sql.Identifier(temp_table),
                sql.SQL(" AND ").join(
                    sql.SQL("target.{} = src.{}").format(
                        sql.Identifier(column),
                        sql.Identifier(column),
                    )
                    for column in config.pk_columns
                ),
            )
        )

    return len(keys)


@flow(name="cdc_replicar_tabla", persist_result=False)
def replicar_tabla_cdc(
    config_name: str = "pilot_t050_articulos",
    bootstrap_mode: str = "current_max_lsn",
) -> dict[str, Any]:
    logger = get_run_logger()
    started_at = utc_now()
    started_perf = perf_counter()

    pg_conn = open_pg_conn()
    ensure_state_row(pg_conn, config_name)
    config = get_table_config(pg_conn, config_name)
    if not config.enabled:
        raise RuntimeError(f"La configuracion {config_name} esta deshabilitada")

    target_columns = get_target_columns(pg_conn, config.target_schema, config.target_table)
    state = get_state(pg_conn, config_name)
    state_last_end_lsn = normalize_lsn(state["last_end_lsn"])
    state_last_start_lsn = normalize_lsn(state["last_start_lsn"])

    sql_conn = open_sqlserver_conn(config)
    try:
        min_lsn = get_min_lsn(sql_conn, config.capture_instance)
        max_lsn = get_max_lsn(sql_conn)

        if state_last_end_lsn is None and bootstrap_mode == "current_max_lsn":
            finished_at = utc_now()
            duration_ms = int((perf_counter() - started_perf) * 1000)
            update_state(
                pg_conn,
                config_name,
                last_start_lsn=max_lsn,
                last_end_lsn=max_lsn,
                last_status="bootstrapped",
                last_rowcount=0,
                last_error=None,
                last_started_at=started_at,
                last_finished_at=finished_at,
            )
            insert_run_log(
                pg_conn,
                config,
                from_lsn=max_lsn,
                to_lsn=max_lsn,
                rows_read=0,
                rows_upserted=0,
                rows_deleted=0,
                status="bootstrapped",
                duration_ms=duration_ms,
                error_text=None,
            )
            pg_conn.commit()
            logger.info(
                "Bootstrap inicializado para %s en %s",
                config.config_name,
                format_lsn(max_lsn),
            )
            return {"status": "bootstrapped", "lsn": format_lsn(max_lsn)}

        if state_last_end_lsn is None:
            from_lsn = min_lsn
        else:
            if state_last_end_lsn < min_lsn:
                raise RuntimeError(
                    "El ultimo LSN procesado quedo fuera de la ventana CDC. "
                    "Se requiere reseed de la tabla."
                )
            from_lsn = increment_lsn(sql_conn, state_last_end_lsn)

        if from_lsn > max_lsn:
            finished_at = utc_now()
            duration_ms = int((perf_counter() - started_perf) * 1000)
            update_state(
                pg_conn,
                config_name,
                last_start_lsn=from_lsn,
                last_end_lsn=max_lsn,
                last_status="idle",
                last_rowcount=0,
                last_error=None,
                last_started_at=started_at,
                last_finished_at=finished_at,
            )
            insert_run_log(
                pg_conn,
                config,
                from_lsn=from_lsn,
                to_lsn=max_lsn,
                rows_read=0,
                rows_upserted=0,
                rows_deleted=0,
                status="idle",
                duration_ms=duration_ms,
                error_text=None,
            )
            pg_conn.commit()
            logger.info("No hay cambios pendientes para %s", config.config_name)
            return {"status": "idle", "rows_read": 0, "rows_upserted": 0, "rows_deleted": 0}

        columns, change_rows = fetch_cdc_rows(sql_conn, config.capture_instance, from_lsn, max_lsn)
        upserts, deletes, rows_read = collapse_changes(columns, change_rows, config, target_columns)

        apply_deletes(pg_conn, config, deletes)
        apply_upserts(pg_conn, config, target_columns, upserts)

        finished_at = utc_now()
        duration_ms = int((perf_counter() - started_perf) * 1000)
        update_state(
            pg_conn,
            config_name,
            last_start_lsn=from_lsn,
            last_end_lsn=max_lsn,
            last_status="success",
            last_rowcount=rows_read,
            last_error=None,
            last_started_at=started_at,
            last_finished_at=finished_at,
        )
        insert_run_log(
            pg_conn,
            config,
            from_lsn=from_lsn,
            to_lsn=max_lsn,
            rows_read=rows_read,
            rows_upserted=len(upserts),
            rows_deleted=len(deletes),
            status="success",
            duration_ms=duration_ms,
            error_text=None,
        )
        pg_conn.commit()

        logger.info(
            "CDC aplicado | config=%s | from_lsn=%s | to_lsn=%s | rows_read=%s | upserts=%s | deletes=%s",
            config.config_name,
            format_lsn(from_lsn),
            format_lsn(max_lsn),
            rows_read,
            len(upserts),
            len(deletes),
        )
        return {
            "status": "success",
            "from_lsn": format_lsn(from_lsn),
            "to_lsn": format_lsn(max_lsn),
            "rows_read": rows_read,
            "rows_upserted": len(upserts),
            "rows_deleted": len(deletes),
        }

    except Exception as exc:
        pg_conn.rollback()
        finished_at = utc_now()
        duration_ms = int((perf_counter() - started_perf) * 1000)

        with open_pg_conn() as error_conn:
            ensure_state_row(error_conn, config_name)
            update_state(
                error_conn,
                config_name,
                last_start_lsn=state_last_start_lsn,
                last_end_lsn=state_last_end_lsn,
                last_status="failed",
                last_rowcount=0,
                last_error=str(exc),
                last_started_at=started_at,
                last_finished_at=finished_at,
            )
            insert_run_log(
                error_conn,
                config,
                from_lsn=state_last_end_lsn,
                to_lsn=None,
                rows_read=0,
                rows_upserted=0,
                rows_deleted=0,
                status="failed",
                duration_ms=duration_ms,
                error_text=str(exc),
            )
            error_conn.commit()
        logger.error("Error en CDC %s: %s", config_name, exc)
        raise
    finally:
        sql_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    config_name = sys.argv[1] if len(sys.argv) > 1 else "pilot_t050_articulos"
    bootstrap_mode = sys.argv[2] if len(sys.argv) > 2 else "current_max_lsn"
    replicar_tabla_cdc(config_name=config_name, bootstrap_mode=bootstrap_mode)
