# -*- coding: utf-8 -*-
"""
ETL_DIARCO / Prefect 3
Flujo manual para enviar una única tabla desde SQL Server hacia PostgreSQL.

Objetivo
--------
Permite ejecutar manualmente, desde la UI de Prefect o por API, una
sincronización puntual de UNA tabla específica, reutilizando la lógica ya
existente en ``flujo_maestro_replica_datos.flujo_maestro``.

Uso esperado en DIARCO
----------------------
- Se publica como deployment manual, sin schedule.
- El operador ingresa los parámetros en Prefect UI al momento de lanzar el run.
- Soporta truncado previo de la tabla destino y filtro SQL opcional.

Entrypoint recomendado
----------------------
flows/flujo_envio_manual_tabla_final.py:envio_manual_tabla_sql_a_postgres
"""

from __future__ import annotations

import os
import time
from typing import Any

import psycopg2 as pg2
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from psycopg2 import errors as pg_errors
from psycopg2 import sql

from flujo_maestro_replica_datos import flujo_maestro


# =========================================================
# CONFIG
# =========================================================
load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")


# =========================================================
# HELPERS PG
# =========================================================
def open_pg_conn() -> pg2.extensions.connection:
    return pg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )


def log_relation_locks(conn: pg2.extensions.connection, schema: str, table: str, logger) -> None:
    """
    Informa locks concedidos/en espera sobre una tabla destino para ayudar
    en diagnósticos cuando el TRUNCATE no puede ejecutarse.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH rel AS (
                SELECT c.oid
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND c.relname = %s
            )
            SELECT
                l.granted,
                l.mode,
                a.pid,
                a.usename,
                a.application_name,
                a.state,
                a.wait_event_type,
                a.wait_event,
                now() - a.query_start AS running_for,
                regexp_replace(coalesce(a.query, ''), '\\s+', ' ', 'g') AS query
            FROM pg_locks l
            JOIN rel r ON l.relation = r.oid
            LEFT JOIN pg_stat_activity a ON a.pid = l.pid
            ORDER BY l.granted DESC, running_for DESC NULLS LAST;
            """,
            (schema, table),
        )
        rows = cur.fetchall()

    if not rows:
        logger.info("ℹ️ No se encontraron locks sobre %s.%s.", schema, table)
        return

    logger.warning("🔎 Locks detectados sobre %s.%s:", schema, table)
    for granted, mode, pid, user, app, state, wet, we, running_for, query in rows:
        logger.warning(
            "  granted=%s mode=%s pid=%s user=%s app=%s state=%s wait=%s/%s running_for=%s query=%s",
            granted,
            mode,
            pid,
            user,
            app,
            state,
            wet,
            we,
            running_for,
            (query or "")[:300],
        )


@task(name="Truncar tabla destino", retries=0)
def vaciar_tabla(
    schema_pg: str,
    tabla_pg: str,
    lock_timeout_ms_wait: int = 3000,
    statement_timeout_ms: int = 10 * 60 * 1000,
    max_retries: int = 5,
) -> None:
    """
    Intenta obtener ACCESS EXCLUSIVE y ejecutar TRUNCATE sobre la tabla destino.

    Estrategia:
    - primeros 2 intentos con NOWAIT
    - siguientes intentos con lock_timeout corto
    - backoff exponencial simple
    """
    logger = get_run_logger()
    fq_table = f"{schema_pg}.{tabla_pg}"

    for intento in range(1, max_retries + 1):
        conn = open_pg_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = %s", (f"{statement_timeout_ms}ms",))

                    use_nowait = intento <= 2
                    if use_nowait:
                        cur.execute(
                            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE NOWAIT").format(
                                sql.Identifier(schema_pg, tabla_pg)
                            )
                        )
                    else:
                        cur.execute("SET LOCAL lock_timeout = %s", (f"{lock_timeout_ms_wait}ms",))
                        cur.execute(
                            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(
                                sql.Identifier(schema_pg, tabla_pg)
                            )
                        )

                    cur.execute(
                        sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(schema_pg, tabla_pg))
                    )

            logger.info(
                "✅ Tabla destino truncada correctamente: %s (intento=%s, modo=%s)",
                fq_table,
                intento,
                "NOWAIT" if use_nowait else f"WAIT<={lock_timeout_ms_wait}ms",
            )
            return

        except pg_errors.LockNotAvailable:
            logger.warning(
                "⏳ No se pudo obtener lock NOWAIT sobre %s (intento %s/%s).",
                fq_table,
                intento,
                max_retries,
            )
            try:
                log_relation_locks(conn, schema_pg, tabla_pg, logger)
            except Exception:
                pass
            time.sleep(min(2 ** (intento - 1), 60))

        except Exception as exc:
            logger.warning(
                "⏳ Falló el truncado de %s en intento %s/%s: %s",
                fq_table,
                intento,
                max_retries,
                exc,
            )
            try:
                log_relation_locks(conn, schema_pg, tabla_pg, logger)
            except Exception:
                pass
            time.sleep(min(2 ** (intento - 1), 60))

        finally:
            try:
                conn.close()
            except Exception:
                pass

    raise RuntimeError(
        f"No se pudo obtener ACCESS EXCLUSIVE para {fq_table} tras {max_retries} intentos."
    )


@task(name="Ejecutar réplica de tabla", retries=0)
def ejecutar_replicacion(esquema_sql: str, tabla_sql: str, filtro_sql: str = "1=1") -> None:
    """
    Invoca la lógica estándar de réplica ya existente.
    """
    logger = get_run_logger()
    logger.info(
        "🚀 Ejecutando flujo_maestro(esquema=%s, tabla=%s, filtro_sql=%s)",
        esquema_sql,
        tabla_sql,
        filtro_sql,
    )
    flujo_maestro(esquema=esquema_sql, tabla=tabla_sql, filtro_sql=filtro_sql)
    logger.info("✅ Réplica finalizada para %s.%s", esquema_sql, tabla_sql)


@flow(
    name="ETL_DIARCO - Envío manual de tabla SQL a PostgreSQL",
    flow_run_name="manual-{esquema_sql}-{tabla_sql}-to-{schema_pg}.{tabla_pg}",
    log_prints=True,
)
def envio_manual_tabla_sql_a_postgres(
    esquema_sql: str = "repl",
    tabla_sql: str = "T050_ARTICULOS",
    schema_pg: str = "src",
    tabla_pg: str = "t050_articulos",
    filtro_sql: str = "1=1",
    truncar_destino: bool = True,
    validar_nombres: bool = True,
) -> dict[str, Any]:
    """
    Flujo manual parametrizable por tabla.

    Parámetros
    ----------
    esquema_sql : str
        Esquema origen en SQL Server.
    tabla_sql : str
        Nombre de tabla origen en SQL Server.
    schema_pg : str
        Esquema destino en PostgreSQL.
    tabla_pg : str
        Nombre de tabla destino en PostgreSQL.
    filtro_sql : str
        Filtro SQL opcional a aplicar en la extracción.
    truncar_destino : bool
        Si True, ejecuta TRUNCATE previo en la tabla destino.
    validar_nombres : bool
        Si True, exige que tabla_pg coincida con tabla_sql en lower-case.
        Esto evita cruces accidentales entre origen y destino.
    """
    logger = get_run_logger()

    esquema_sql = esquema_sql.strip()
    tabla_sql = tabla_sql.strip()
    schema_pg = schema_pg.strip()
    tabla_pg = tabla_pg.strip()
    filtro_sql = (filtro_sql or "1=1").strip()

    logger.info(
        "📦 Inicio envío manual | origen=%s.%s | destino=%s.%s | truncar=%s | filtro=%s",
        esquema_sql,
        tabla_sql,
        schema_pg,
        tabla_pg,
        truncar_destino,
        filtro_sql,
    )

    if not all([PG_HOST, PG_DB, PG_USER, PG_PASSWORD]):
        raise RuntimeError(
            "Faltan variables de entorno de PostgreSQL (PG_HOST, PG_DB, PG_USER, PG_PASSWORD)."
        )

    if validar_nombres:
        esperado = tabla_sql.lower()
        if tabla_pg.lower() != esperado:
            raise ValueError(
                "Validación de seguridad rechazada: tabla_pg no coincide con tabla_sql en lower-case. "
                f"Esperado='{esperado}' recibido='{tabla_pg}'. "
                "Si se desea forzar otro nombre de destino, ejecutar con validar_nombres=False."
            )

    if truncar_destino:
        vaciar_tabla(schema_pg=schema_pg, tabla_pg=tabla_pg)
    else:
        logger.warning("⚠️ Se omite TRUNCATE de destino por parámetro.")

    ejecutar_replicacion(esquema_sql=esquema_sql, tabla_sql=tabla_sql, filtro_sql=filtro_sql)

    resultado = {
        "status": "OK",
        "origen": f"{esquema_sql}.{tabla_sql}",
        "destino": f"{schema_pg}.{tabla_pg}",
        "filtro_sql": filtro_sql,
        "truncar_destino": truncar_destino,
        "validar_nombres": validar_nombres,
    }
    logger.info("✅ Flujo finalizado correctamente: %s", resultado)
    return resultado


if __name__ == "__main__":
    envio_manual_tabla_sql_a_postgres()
