from __future__ import annotations

import psycopg2

from IOSdb.config.settings import load_settings


def open_postgres_connection():
    pg_settings = load_settings().postgres
    return psycopg2.connect(
        host=pg_settings.host,
        port=pg_settings.port,
        dbname=pg_settings.dbname,
        user=pg_settings.user,
        password=pg_settings.password,
    )
