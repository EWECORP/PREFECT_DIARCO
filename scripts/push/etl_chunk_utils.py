import csv
import io
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Callable, Dict

import pandas as pd
import psycopg2 as pg2
from sqlalchemy import create_engine


DataFrameTransform = Callable[[pd.DataFrame, int, logging.Logger], pd.DataFrame]


def setup_script_logger(logger_name: str, log_filename: str) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    logs_dir = Path(__file__).resolve().parents[2] / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    primary_log_path = logs_dir / log_filename
    try:
        file_handler = logging.FileHandler(primary_log_path, encoding="utf-8")
    except PermissionError:
        fallback_name = (
            f"{primary_log_path.stem}_{datetime.now():%Y%m%d_%H%M%S}_{os.getpid()}"
            f"{primary_log_path.suffix}"
        )
        fallback_path = logs_dir / fallback_name
        file_handler = logging.FileHandler(fallback_path, encoding="utf-8")
        logger.warning(
            "Archivo de log bloqueado: %s. Se usa archivo alternativo: %s",
            primary_log_path,
            fallback_path,
        )

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def build_sql_server_engine(
    sql_server: str,
    sql_user: str,
    sql_password: str,
    sql_database: str,
):
    return create_engine(
        f"mssql+pyodbc://{sql_user}:{sql_password}@{sql_server}/{sql_database}"
        "?driver=ODBC+Driver+17+for+SQL+Server",
        connect_args={"autocommit": True},
    )


def open_pg_conn(
    pg_host: str,
    pg_port: str,
    pg_db: str,
    pg_user: str,
    pg_password: str,
):
    return pg2.connect(
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
        host=pg_host,
        port=pg_port,
    )


def create_table_statement(schema_dict: Dict[str, str], table_name: str) -> str:
    columns_sql = ", ".join([f'"{quote_ident(col)}" {col_type}' for col, col_type in schema_dict.items()])
    return f"CREATE TABLE {table_name} ({columns_sql})"


def align_dataframe_to_schema(df: pd.DataFrame, schema_dict: Dict[str, str]) -> pd.DataFrame:
    aligned = df.copy()
    aligned.columns = [str(col).lower() for col in aligned.columns]

    for column in schema_dict:
        if column not in aligned.columns:
            aligned[column] = pd.NA

    aligned = aligned[list(schema_dict.keys())]
    return aligned.astype(object).where(pd.notnull(aligned), None)


def copy_dataframe_to_postgres(cur, table_name: str, df: pd.DataFrame) -> None:
    buffer = io.StringIO()
    df.to_csv(
        buffer,
        index=False,
        header=False,
        na_rep="\\N",
        date_format="%Y-%m-%d %H:%M:%S.%f",
        quoting=csv.QUOTE_MINIMAL,
    )
    buffer.seek(0)

    columns_sql = ", ".join([f'"{quote_ident(col)}"' for col in df.columns])
    copy_sql = (
        f"COPY {table_name} ({columns_sql}) "
        "FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    )
    cur.copy_expert(copy_sql, buffer)


def replace_table_from_query_chunks(
    *,
    query: str,
    sql_engine,
    pg_conn_factory: Callable[[], pg2.extensions.connection],
    table_name: str,
    schema_dict: Dict[str, str],
    transform_chunk: DataFrameTransform,
    logger: logging.Logger,
    read_chunk_size: int = 25000,
) -> Dict[str, float]:
    started_at = perf_counter()
    total_rows = 0
    total_chunks = 0

    logger.info(
        "Iniciando extracción desde SQL Server | tabla_destino=%s | chunk_size=%s",
        table_name,
        read_chunk_size,
    )

    with pg_conn_factory() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                cur.execute(create_table_statement(schema_dict, table_name))
                logger.info("Tabla destino recreada: %s", table_name)

                for chunk_index, raw_chunk in enumerate(
                    pd.read_sql(query, sql_engine, chunksize=read_chunk_size),
                    start=1,
                ):
                    raw_rows = len(raw_chunk)
                    logger.info(
                        "Chunk %s recibido desde SQL Server con %s filas",
                        chunk_index,
                        raw_rows,
                    )

                    transformed = transform_chunk(raw_chunk.copy(), chunk_index, logger)
                    if transformed.empty:
                        logger.warning(
                            "Chunk %s descartado luego de la transformación; se continúa",
                            chunk_index,
                        )
                        continue

                    aligned = align_dataframe_to_schema(transformed, schema_dict)
                    copy_dataframe_to_postgres(cur, table_name, aligned)

                    chunk_rows = len(aligned)
                    total_rows += chunk_rows
                    total_chunks += 1
                    logger.info(
                        "Chunk %s copiado a PostgreSQL | filas=%s | acumulado=%s",
                        chunk_index,
                        chunk_rows,
                        total_rows,
                    )

            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("Se revirtió la transacción de carga sobre %s", table_name)
            raise

    elapsed = perf_counter() - started_at
    logger.info(
        "Carga finalizada | tabla_destino=%s | filas=%s | chunks=%s | duracion=%.2fs",
        table_name,
        total_rows,
        total_chunks,
        elapsed,
    )
    return {"rows": total_rows, "chunks": total_chunks, "seconds": elapsed}


def quote_ident(identifier: str) -> str:
    return identifier.replace('"', '""')


def coerce_int_column(df: pd.DataFrame, column: str, logger: logging.Logger) -> None:
    if column not in df.columns:
        logger.warning("Columna ausente en origen; se cargará como NULL: %s", column)
        return
    df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")


def coerce_float_column(df: pd.DataFrame, column: str, logger: logging.Logger) -> None:
    if column not in df.columns:
        logger.warning("Columna ausente en origen; se cargará como NULL: %s", column)
        return
    df[column] = pd.to_numeric(df[column], errors="coerce").astype("Float64")


def coerce_datetime_column(df: pd.DataFrame, column: str, logger: logging.Logger) -> None:
    if column not in df.columns:
        logger.warning("Columna ausente en origen; se cargará como NULL: %s", column)
        return
    df[column] = pd.to_datetime(df[column], errors="coerce")


def coerce_string_column(df: pd.DataFrame, column: str, logger: logging.Logger, strip: bool = False) -> None:
    if column not in df.columns:
        logger.warning("Columna ausente en origen; se cargará como NULL: %s", column)
        return
    df[column] = df[column].astype("string")
    if strip:
        df[column] = df[column].str.strip()
