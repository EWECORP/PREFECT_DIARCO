from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any
from urllib import error, request
from zoneinfo import ZoneInfo

import psycopg2
from dotenv import load_dotenv
from prefect import flow, get_run_logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ENV_PATH = os.environ.get("ETL_ENV_PATH", str(PROJECT_ROOT / ".env"))
load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class MonitorConfig:
    config_name: str
    enabled: bool
    source_label: str
    target_label: str
    poll_seconds: int
    last_status: str | None
    last_rowcount: int | None
    last_error: str | None
    last_started_at: datetime | None
    last_finished_at: datetime | None
    updated_at: datetime | None
    last_run_status: str | None
    last_run_created_at: datetime | None
    last_run_error: str | None


@dataclass
class HealthResult:
    config_name: str
    level: str
    issues: list[str] = field(default_factory=list)
    stale_minutes: float | None = None
    stale_threshold_minutes: float | None = None
    last_status: str | None = None
    monitor_window_active: bool = True


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno requerida: {name}")
    return value


def open_pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        dbname=require_env("PG_DB"),
        user=require_env("PG_USER"),
        password=require_env("PG_PASSWORD"),
        host=require_env("PG_HOST"),
        port=require_env("PG_PORT"),
    )


def load_monitor_configs(
    pg_conn: psycopg2.extensions.connection,
    *,
    include_disabled: bool,
    config_names: tuple[str, ...] | None,
) -> list[MonitorConfig]:
    where_clauses = ["cfg.mode = 'cdc'"]
    params: list[Any] = []

    if not include_disabled:
        where_clauses.append("cfg.enabled = true")

    if config_names:
        where_clauses.append("cfg.config_name = ANY(%s)")
        params.append(list(config_names))

    query = f"""
        SELECT
            cfg.config_name,
            cfg.enabled,
            cfg.source_server || '.' || cfg.source_database || '.' || cfg.source_schema || '.' || cfg.source_table AS source_label,
            cfg.target_schema || '.' || cfg.target_table AS target_label,
            cfg.poll_seconds,
            st.last_status,
            st.last_rowcount,
            st.last_error,
            st.last_started_at,
            st.last_finished_at,
            st.updated_at,
            lr.status AS last_run_status,
            lr.created_at AS last_run_created_at,
            lr.error_text AS last_run_error
        FROM etl.cdc_table_config cfg
        LEFT JOIN etl.cdc_state st
            ON st.config_name = cfg.config_name
        LEFT JOIN LATERAL (
            SELECT
                status,
                created_at,
                error_text
            FROM etl.cdc_run_log rl
            WHERE rl.config_name = cfg.config_name
            ORDER BY rl.run_id DESC
            LIMIT 1
        ) lr
            ON true
        WHERE {" AND ".join(where_clauses)}
        ORDER BY cfg.config_name
    """

    with pg_conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [
        MonitorConfig(
            config_name=row[0],
            enabled=row[1],
            source_label=row[2],
            target_label=row[3],
            poll_seconds=row[4],
            last_status=row[5],
            last_rowcount=row[6],
            last_error=row[7],
            last_started_at=row[8],
            last_finished_at=row[9],
            updated_at=row[10],
            last_run_status=row[11],
            last_run_created_at=row[12],
            last_run_error=row[13],
        )
        for row in rows
    ]


def load_recent_statuses(
    pg_conn: psycopg2.extensions.connection,
    *,
    lookback_runs: int,
    config_names: tuple[str, ...] | None,
) -> dict[str, list[str]]:
    params: list[Any]
    where_sql = ""
    if config_names:
        where_sql = "WHERE rl.config_name = ANY(%s)"
        params = [list(config_names), lookback_runs]
    else:
        params = [lookback_runs]

    query = f"""
        SELECT config_name, status
        FROM (
            SELECT
                rl.config_name,
                rl.status,
                row_number() OVER (
                    PARTITION BY rl.config_name
                    ORDER BY rl.run_id DESC
                ) AS rn
            FROM etl.cdc_run_log rl
            {where_sql}
        ) ranked
        WHERE rn <= %s
        ORDER BY config_name, rn
    """

    with pg_conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    grouped: dict[str, list[str]] = {}
    for config_name, status in rows:
        grouped.setdefault(config_name, []).append(status)
    return grouped


def consecutive_failures(statuses: list[str]) -> int:
    count = 0
    for status in statuses:
        if status == "failed":
            count += 1
            continue
        break
    return count


def truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def max_level(current: str, candidate: str) -> str:
    weights = {"ok": 0, "disabled": 0, "warning": 1, "critical": 2}
    return candidate if weights[candidate] > weights.get(current, 0) else current


def parse_hhmm(value: str) -> time:
    hour_text, minute_text = value.split(":", maxsplit=1)
    return time(hour=int(hour_text), minute=int(minute_text))


def is_within_window(current_time: time, start_time: time, end_time: time) -> bool:
    if start_time <= end_time:
        return start_time <= current_time < end_time
    return current_time >= start_time or current_time < end_time


def compute_health(
    config: MonitorConfig,
    *,
    recent_statuses: list[str],
    stale_factor: float,
    min_stale_minutes: int,
    failure_threshold: int,
    now_utc: datetime,
    monitor_timezone: str,
    active_window_start: str,
    active_window_end: str,
) -> HealthResult:
    threshold_minutes = max((config.poll_seconds * stale_factor) / 60.0, float(min_stale_minutes))
    last_reference = config.last_finished_at or config.last_started_at or config.updated_at
    stale_minutes = None
    if last_reference is not None:
        stale_minutes = (now_utc - last_reference).total_seconds() / 60.0
    current_local_time = now_utc.astimezone(ZoneInfo(monitor_timezone)).time()
    monitor_window_active = is_within_window(
        current_local_time,
        parse_hhmm(active_window_start),
        parse_hhmm(active_window_end),
    )

    result = HealthResult(
        config_name=config.config_name,
        level="ok",
        stale_minutes=stale_minutes,
        stale_threshold_minutes=threshold_minutes,
        last_status=config.last_status,
        monitor_window_active=monitor_window_active,
    )

    if not config.enabled:
        result.level = "disabled"
        return result

    if config.last_status is None or config.last_status == "never_run":
        result.level = "warning"
        result.issues.append("sin bootstrap o sin corrida registrada")
        return result

    failures = consecutive_failures(recent_statuses)
    if failures >= failure_threshold:
        result.level = "critical"
        result.issues.append(f"{failures} fallas consecutivas")

    if config.last_status == "failed":
        result.level = "critical"
        error_text = config.last_error or config.last_run_error or "sin detalle"
        result.issues.append(f"ultima corrida fallida: {truncate(error_text, 160)}")
    elif config.last_status == "bootstrapped":
        result.level = max_level(result.level, "warning")
        result.issues.append("tabla bootstrapped pero sin corrida success posterior")

    if stale_minutes is None:
        result.level = max_level(result.level, "warning")
        result.issues.append("sin marca temporal de ultima corrida")
    elif monitor_window_active and stale_minutes > threshold_minutes:
        result.level = "critical"
        result.issues.append(
            f"atraso de {stale_minutes:.1f} min > umbral {threshold_minutes:.1f} min"
        )

    return result


def get_discord_webhook() -> str | None:
    for key in (
        "CDC_MONITOR_DISCORD_WEBHOOK",
        "IOSDB_DISCORD_WEBHOOK",
        "DISCORD_WEBHOOK",
    ):
        value = os.getenv(key)
        if value:
            return value
    return None


def notify_discord(message: str) -> None:
    logger = get_run_logger()
    webhook = get_discord_webhook()
    if not webhook:
        logger.info("Webhook de Discord no configurado; se omite notificacion CDC.")
        return

    payload = json.dumps({"content": message}).encode("utf-8")
    req = request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10):
            logger.info("Notificacion CDC enviada a Discord.")
    except error.URLError as exc:
        logger.warning("No se pudo enviar la notificacion CDC a Discord: %s", exc)


def build_message(results: list[HealthResult], summary: dict[str, int]) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"CDC Monitor | {timestamp}",
        (
            f"ok={summary['ok']} | warning={summary['warning']} | "
            f"critical={summary['critical']} | disabled={summary['disabled']}"
        ),
    ]
    for result in results:
        if result.level in {"warning", "critical"}:
            issues = "; ".join(result.issues) if result.issues else "sin detalle"
            lines.append(f"[{result.level.upper()}] {result.config_name}: {issues}")
    return "\n".join(lines[:20])


@flow(name="cdc_monitor", log_prints=True)
def monitorear_cdc(
    config_names: str | None = None,
    include_disabled: bool = False,
    stale_factor: float = 3.0,
    min_stale_minutes: int = 15,
    failure_threshold: int = 2,
    lookback_runs: int = 5,
    notify: bool = False,
    notify_on_warning: bool = False,
    fail_on_critical: bool = True,
    monitor_timezone: str = "America/Argentina/Buenos_Aires",
    active_window_start: str = "08:00",
    active_window_end: str = "18:00",
) -> dict[str, Any]:
    logger = get_run_logger()
    parsed_names = tuple(name.strip() for name in (config_names or "").split(",") if name.strip()) or None

    with open_pg_conn() as pg_conn:
        configs = load_monitor_configs(
            pg_conn,
            include_disabled=include_disabled,
            config_names=parsed_names,
        )
        statuses = load_recent_statuses(
            pg_conn,
            lookback_runs=lookback_runs,
            config_names=parsed_names,
        )

    if not configs:
        raise RuntimeError("No hay configuraciones CDC para monitorear.")

    now_utc = datetime.now(timezone.utc)
    results = [
        compute_health(
            config,
            recent_statuses=statuses.get(config.config_name, []),
            stale_factor=stale_factor,
            min_stale_minutes=min_stale_minutes,
            failure_threshold=failure_threshold,
            now_utc=now_utc,
            monitor_timezone=monitor_timezone,
            active_window_start=active_window_start,
            active_window_end=active_window_end,
        )
        for config in configs
    ]

    summary = {"ok": 0, "warning": 0, "critical": 0, "disabled": 0}
    for result in results:
        summary[result.level] = summary.get(result.level, 0) + 1
        logger.info(
            "CDC health | config=%s | level=%s | status=%s | issues=%s",
            result.config_name,
            result.level,
            result.last_status,
            " | ".join(result.issues) if result.issues else "none",
        )

    should_notify = notify and (
        summary["critical"] > 0 or (notify_on_warning and summary["warning"] > 0)
    )
    if should_notify:
        notify_discord(build_message(results, summary))

    if fail_on_critical and summary["critical"] > 0:
        raise RuntimeError(build_message(results, summary))

    return {
        "summary": summary,
        "details": [
            {
                "config_name": result.config_name,
                "level": result.level,
                "issues": result.issues,
                "stale_minutes": result.stale_minutes,
                "stale_threshold_minutes": result.stale_threshold_minutes,
                "last_status": result.last_status,
                "monitor_window_active": result.monitor_window_active,
            }
            for result in results
        ],
    }


if __name__ == "__main__":
    monitorear_cdc(fail_on_critical=False, notify=False)
