from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


def _resolve_env_path() -> Path:
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent
    repo_root = project_root.parent

    candidates = [
        project_root / ".env",
        repo_root / ".env",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        f"No se encontro archivo .env en {project_root} ni en {repo_root}"
    )


def _get_env(*keys: str, default: str = "") -> str:
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return default


def _require_env(*keys: str) -> str:
    value = _get_env(*keys)
    if not value:
        joined = ", ".join(keys)
        raise RuntimeError(f"Falta variable de entorno requerida: {joined}")
    return value


def _get_int_env(*keys: str, default: int) -> int:
    raw_value = _get_env(*keys, default=str(default))
    try:
        return int(raw_value)
    except ValueError as exc:
        joined = ", ".join(keys)
        raise RuntimeError(f"Valor invalido para {joined}: {raw_value}") from exc


@dataclass(frozen=True)
class SQLServerSettings:
    server: str
    database: str
    user: str
    password: str
    driver: str

    @property
    def connection_string(self) -> str:
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "TrustServerCertificate=yes;"
        )


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class IOSApiSettings:
    login_url: str
    stock_url: str
    categories_url: str
    products_url: str
    username: str
    company: str
    password: str
    login_timeout_seconds: int
    request_timeout_seconds: int


@dataclass(frozen=True)
class RuntimeSettings:
    batch_size: int
    category_batch_size: int
    product_batch_size: int
    max_workers: int
    max_retries: int
    logs_dir: Path
    discord_webhook: str


@dataclass(frozen=True)
class IOSdbSettings:
    env_path: Path
    project_root: Path
    sqlserver: SQLServerSettings
    postgres: PostgresSettings
    api: IOSApiSettings
    runtime: RuntimeSettings


@lru_cache(maxsize=1)
def load_settings() -> IOSdbSettings:
    env_path = _resolve_env_path()
    load_dotenv(dotenv_path=env_path, override=True)

    project_root = Path(__file__).resolve().parents[1]

    sqlserver = SQLServerSettings(
        server=_require_env("IOSDB_SQL_SERVER", "DB_SERVER"),
        database=_require_env("IOSDB_SQL_DATABASE", "DB_NAME"),
        user=_require_env("IOSDB_SQL_USER", "DB_USER"),
        password=_require_env("IOSDB_SQL_PASSWORD", "DB_PASSWORD"),
        driver=_get_env(
            "IOSDB_SQL_DRIVER",
            "DB_DRIVER",
            default="ODBC Driver 17 for SQL Server",
        ),
    )

    postgres = PostgresSettings(
        host=_require_env("IOSDB_PG_HOST", "PG_HOST"),
        port=_get_int_env("IOSDB_PG_PORT", "PG_PORT", default=5432),
        dbname=_require_env("IOSDB_PG_DB", "PG_DB"),
        user=_require_env("IOSDB_PG_USER", "PG_USER"),
        password=_require_env("IOSDB_PG_PASSWORD", "PG_PASSWORD"),
    )

    api = IOSApiSettings(
        login_url=_get_env(
            "IOSDB_API_LOGIN_URL",
            default="https://prod-apps.connexa-cloud.com/authentication/connexa-api-ios-authentication-1.0.0/connexa/api/ios/v1/auth/signIn",
        ),
        stock_url=_get_env(
            "IOSDB_API_STOCK_URL",
            default="https://prod-apps.connexa-cloud.com/stock-sales/connexa-api-ios-stock-sales-1.0.0/connexa/api/ios/stock/v1/stocksales",
        ),
        categories_url=_get_env(
            "IOSDB_API_CATEGORIES_URL",
            default="https://prod-apps.connexa-cloud.com/stock-sales/connexa-api-ios-stock-sales-1.0.0/connexa/api/ios/stock/v1/stocksales/categories",
        ),
        products_url=_get_env(
            "IOSDB_API_PRODUCTS_URL",
            default="https://prod-apps.connexa-cloud.com/stock-sales/connexa-api-ios-stock-sales-1.0.0/connexa/api/ios/stock/v1/stocksales/products",
        ),
        username=_require_env("IOSDB_API_USERNAME", "API_USERNAME"),
        company=_require_env("IOSDB_API_COMPANY", "API_COMPANY"),
        password=_require_env("IOSDB_API_PASSWORD", "API_PASSWORD"),
        login_timeout_seconds=_get_int_env("IOSDB_LOGIN_TIMEOUT_SECONDS", default=30),
        request_timeout_seconds=_get_int_env("IOSDB_REQUEST_TIMEOUT_SECONDS", default=60),
    )

    runtime = RuntimeSettings(
        batch_size=_get_int_env("IOSDB_BATCH_SIZE", default=300),
        category_batch_size=_get_int_env("IOSDB_CATEGORY_BATCH_SIZE", default=1),
        product_batch_size=_get_int_env("IOSDB_PRODUCT_BATCH_SIZE", default=100),
        max_workers=_get_int_env("IOSDB_MAX_WORKERS", default=10),
        max_retries=_get_int_env("IOSDB_MAX_RETRIES", default=2),
        logs_dir=Path(_get_env("IOSDB_LOGS_DIR", default=str(project_root / "logs"))),
        discord_webhook=_get_env("IOSDB_DISCORD_WEBHOOK", "DISCORD_WEBHOOK"),
    )

    return IOSdbSettings(
        env_path=env_path,
        project_root=project_root,
        sqlserver=sqlserver,
        postgres=postgres,
        api=api,
        runtime=runtime,
    )
