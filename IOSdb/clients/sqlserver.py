from __future__ import annotations

import pyodbc

from IOSdb.config.settings import load_settings


def open_sqlserver_connection() -> pyodbc.Connection:
    return pyodbc.connect(load_settings().sqlserver.connection_string)
