# actualizar_tablas_maestras.py
# OBJETIVO: Actualizar las tablas maestras de productos y sucursales en PostgreSQL desde SQL Server.
# AUTOR: [EWE]
# Las tablas ya existen. TRUNCA las tablas destino y las vuelve a llenar.

import os
import sys
import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
from prefect import flow, task, get_run_logger
from datetime import datetime
import time
from psycopg2 import sql
from psycopg2 import errors as pg_errors

from flujo_maestro_replica_datos import flujo_maestro, generar_nombre_archivo

# ====================== CONFIGURACIÓN Y LOGGING ======================
load_dotenv()

# Variables de entorno
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

SP_NAME = "[dbo].[SP_BASE_PRODUCTOS_DMZ]"
TABLE_DESTINO = "src.base_productos_vigentes"

# Logging
logger = logging.getLogger("replicacion_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
os.makedirs("logs", exist_ok=True)
file_handler = logging.FileHandler("logs/actualizar_tablas_maestras.log", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ====================== CONEXIONES ======================
sql_engine = create_engine(
    f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
)

def open_pg_conn():
    return pg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )

# ====================== FUNCIONES ======================

def log_blockers(conn, fq_table: str, logger):
    """
    Registra sesiones bloqueadas y bloqueadoras para fq_table (schema.table).
    """
    with conn.cursor() as cur:
        # ¿Quién está bloqueado y por quién?
        cur.execute("""
            SELECT
                a.pid                             AS blocked_pid,
                a.usename                         AS blocked_user,
                a.query                           AS blocked_query,
                now() - a.query_start             AS blocked_for,
                pg_blocking_pids(a.pid)           AS blocking_pids
            FROM pg_stat_activity a
            WHERE a.datname = current_database()
                AND a.wait_event_type = 'Lock';
        """)
        rows = cur.fetchall()
        if not rows:
            logger.info("ℹ️ No hay sesiones esperando locks actualmente.")
            return

        logger.warning("🔎 Sesiones bloqueadas detectadas:")
        for r in rows:
            logger.warning(f" - blocked_pid={r[0]} user={r[1]} since={r[3]} blockers={r[4]}")

        # Detalle de bloqueadores (si los hay)
        blocker_pids = []
        for r in rows:
            if r[4]:
                blocker_pids.extend(r[4])
        if blocker_pids:
            cur.execute("""
                SELECT pid, usename, state, wait_event_type, wait_event,
                        now() - query_start AS running_for, query
                FROM pg_stat_activity
                WHERE pid = ANY(%s)
                ORDER BY query_start;
            """, (blocker_pids,))
            det = cur.fetchall()
            logger.warning("🔎 Detalle de bloqueadores:")
            for d in det:
                logger.warning(
                    f"   pid={d[0]} user={d[1]} state={d[2]} wait={d[3]}/{d[4]} "
                    f"running_for={d[5]} query={d[6][:300]}"
                )


def log_relation_locks(conn, schema: str, table: str, logger):
    """
    Muestra locks CONCEDIDOS y en ESPERA sobre schema.table,
    junto con la consulta, estado y cuánto hace que corren.
    También detecta autovacuum en progreso sobre esa relación.
    """
    with conn.cursor() as cur:
        # Locks concedidos/espera sobre la relación
        cur.execute("""
            WITH rel AS (
                SELECT c.oid
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            )
            SELECT
                l.granted, l.mode,
                a.pid, a.usename, a.application_name, a.state,
                a.wait_event_type, a.wait_event,
                now() - a.query_start AS running_for,
                regexp_replace(coalesce(a.query, ''), '\\s+', ' ', 'g') AS query
            FROM pg_locks l
            JOIN rel r ON l.relation = r.oid
            LEFT JOIN pg_stat_activity a ON a.pid = l.pid
            ORDER BY l.granted DESC, running_for DESC NULLS LAST;
        """, (schema, table))
        rows = cur.fetchall()
        if rows:
            logger.warning(f"🔎 Locks sobre {schema}.{table}:")
            for (granted, mode, pid, user, app, state, wet, we, running_for, query) in rows:
                logger.warning(
                    f"  granted={granted} mode={mode} pid={pid} user={user} app={app} "
                    f"state={state} wait={wet}/{we} running_for={running_for} "
                    f"query={query[:300]}"
                )
        else:
            logger.info(f"ℹ️ No se encontraron locks en {schema}.{table} (pg_locks).")

        # ¿Autovacuum en progreso sobre la misma relación?
        cur.execute("""
            SELECT v.pid, v.phase,
                    v.heap_blks_total, v.heap_blks_scanned, v.heap_blks_vacuumed,
                    now() - a.query_start AS running_for
            FROM pg_stat_progress_vacuum v
            JOIN pg_stat_activity a ON a.pid = v.pid
            WHERE v.relid = %s::regclass;
        """, (f"{schema}.{table}",))
        vac = cur.fetchall()
        if vac:
            logger.warning(f"🧹 autovacuum en progreso sobre {schema}.{table}:")
            for (pid, phase, total, scanned, vacuumed, running_for) in vac:
                logger.warning(
                    f"  pid={pid} phase={phase} running_for={running_for} "
                    f"heap_blks {scanned}/{vacuumed}/{total}"
                )



@task
def vaciar_tabla(tabla_pg: str,
                lock_timeout_ms_nowait: int = 0,       # no aplica, sólo por simetría
                lock_timeout_ms_wait: int = 3000,      # 3s en intentos con espera
                statement_timeout_ms: int = 10*60*1000,
                max_retries: int = 5) -> None:
    logger = get_run_logger()
    schema = "src"
    fq_table = f"{schema}.{tabla_pg}"

    for intento in range(1, max_retries + 1):
        conn = open_pg_conn()  # autocommit=False por defecto
        try:
            with conn:  # transacción corta
                with conn.cursor() as cur:
                    # Siempre límite a la ejecución
                    cur.execute("SET LOCAL statement_timeout = %s", (f"{statement_timeout_ms}ms",))

                    use_nowait = (intento <= 2)  # primeros intentos: NOWAIT
                    if use_nowait:
                        # NOWAIT: falla inmediato si hay cualquier lock concedido
                        cur.execute(
                            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE NOWAIT")
                                .format(sql.Identifier(schema, tabla_pg))
                        )
                    else:
                        # Espera corta: lock_timeout 3s (ajustable)
                        cur.execute("SET LOCAL lock_timeout = %s", (f"{lock_timeout_ms_wait}ms",))
                        cur.execute(
                            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE")
                                .format(sql.Identifier(schema, tabla_pg))
                        )

                    # Con el lock tomado, TRUNCATE
                    cur.execute(
                        sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(schema, tabla_pg))
                    )

            logger.info(f"✅ Tabla '{fq_table}' vaciada correctamente (intento {intento}, "
                        f"{'NOWAIT' if use_nowait else 'WAIT<=3s'}).")
            return

        except pg_errors.LockNotAvailable:
            logger.warning(f"⏳ Lock NOWAIT rechazado sobre {fq_table} (intento {intento}/{max_retries}).")
            try:
                log_relation_locks(conn, schema, tabla_pg, logger)
            except Exception:
                pass
            time.sleep(min(2 ** (intento - 1), 60))

        except Exception as e:
            # Incluye "canceling statement due to lock timeout" de los intentos con espera corta
            logger.warning(f"⏳ Intento {intento}/{max_retries} sobre {fq_table} falló: {e}")
            try:
                log_relation_locks(conn, schema, tabla_pg, logger)
            except Exception:
                pass
            time.sleep(min(2 ** (intento - 1), 60))

        finally:
            try:
                conn.close()
            except Exception:
                pass

    raise RuntimeError(f"No se pudo obtener ACCESS EXCLUSIVE para {fq_table} tras {max_retries} intentos.")


@flow(name="actualizar_tablas_maestras")
def actualizar_tablas_maestras():
    logger = get_run_logger()
    tablas = [
        ("repl","t050_articulos", "T050_ARTICULOS"),
        ("repl", "t020_proveedor", "T020_PROVEEDOR"),
        ("repl", "t052_articulos_proveedor", "T052_ARTICULOS_PROVEEDOR"),
        ("repl", "t060_stock", "T060_STOCK"),
        ("repl", "m_3_articulos", "M_3_ARTICULOS"),
        ("repl", "t100_empresa_suc", "T100_EMPRESA_SUC"),
        ("repl", "t114_rubros", "T114_RUBROS"),
        ("repl", "t117_compradores", "T117_COMPRADORES"),
        ("repl", "t020_proveedor_dias_entrega_cabe","T020_PROVEEDOR_DIAS_ENTREGA_CABE"),
        ("repl", "t020_proveedor_dias_entrega_deta","T020_PROVEEDOR_DIAS_ENTREGA_DETA"),

        ("dbo", "m_1_categorias", "M_1_CATEGORIAS"),
        ("dbo", "m_10_proveedores", "M_10_PROVEEDORES"),
        ("dbo", "m_2_movimientos", "M_2_MOVIMIENTOS"),
        ("dbo", "m_3_1_familia", "M_3_1_FAMILIA"),
        ("dbo", "m_3_2_familia_articulo", "M_3_2_FAMILIA_ARTICULO"),
        ("dbo", "m_9_compradores", "M_9_COMPRADORES"),
        ("dbo", "m_91_sucursales", "M_91_SUCURSALES"),
        ("dbo", "m_92_depositos", "M_92_DEPOSITOS"),
        ("dbo", "m_93_sustitutos", "M_93_SUSTITUTOS"),
        ("dbo", "m_94_alternativos", "M_94_ALTERNATIVOS"),
        ("dbo", "m_95_sensibles", "M_95_SENSIBLES"),
        ("dbo", "m_96_stock_seguridad", "M_96_STOCK_SEGURIDAD"),
        
        ("repl", "t080_oc_pendientes", "T080_OC_PENDIENTES"),  
        ("repl", "t080_oc_cabe", "T080_OC_CABE"),  
        ("repl", "t081_oc_deta", "T081_OC_DETA"),  
        ("repl", "t051_articulos_sucursal", "T051_ARTICULOS_SUCURSAL"),        
        ("repl", "t710_estadis_reposicion", "T710_ESTADIS_REPOSICION"),
        ("repl", "t874_precarga_connexa_hist", "T874_PRECARGA_CONNEXA_HIST"),
        
        ("repl", "mv_uso_mensual_comprador", "MV_USO_MENSUAL_COMPRADOR"),
        ("repl", "mv_uso_mensual_proveedor", "MV_USO_MENSUAL_PROVEEDOR"),
        ("repl", "mv_uso_semanal_comprador", "MV_USO_SEMANAL_COMPRADOR"),
        ("repl", "mv_uso_semanal_proveedor", "MV_USO_SEMANAL_PROVEEDOR")
    ]

    for origen, tabla_pg, tabla_sql in tablas:

        try:
            logger.info(f"🔄 Procesando tabla {tabla_pg}...")
            vaciar_tabla(tabla_pg) # Forzar a que termine antes de seguir
            # vaciar_tabla.submit(tabla_pg)
            flujo_maestro(esquema=origen, tabla=tabla_sql, filtro_sql="1=1")  # no usar run_deployment aquí si no está controlado
            logger.info(f"✅ Tabla {tabla_pg} actualizada con éxito.")
        except Exception as e:
            logger.error(f"❌ Error procesando {tabla_pg}: {e}")

    print("✅ Actualización de Tablas Maestras finalizada.")

if __name__ == "__main__":
    actualizar_tablas_maestras()
