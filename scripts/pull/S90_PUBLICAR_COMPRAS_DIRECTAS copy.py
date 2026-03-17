# -*- coding: utf-8 -*-
"""
S90_PUBLICAR_COMPRAS_DIRECTAS.py
--------------------------------
Publica compras directas desde diarco_data (PostgreSQL)
hacia SGM (SQL Server), tabla destino:
    dbo.T080_OC_PRECARGA_KIKKER

Incluye:
- Lectura dinámica de anchos de columnas del destino (INFORMATION_SCHEMA)
- Truncado automático en columnas VARCHAR
- Publicación idempotente (PK completa)
- Marcado de registros como publicados en PostgreSQL
- Mapeo explícito de columnas PG -> SQL (solo las necesarias)
  incluyendo la equivalencia:
      c_compra_connexa  -> C_COMPRA_KIKKER

Revisión: 2025-11-14
Autor: EWE / Zeetrex
"""

import os
import sys
import logging
import traceback
import io
from typing import Dict, List, Set, Tuple

import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values  # (no se usa, pero se deja por consistencia)
import pyodbc
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, dotenv_values
from datetime import datetime


# --------------------------------------------------------------------
# Configuración básica de logging
# --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

load_dotenv()

# Salida estándar en UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

print(f"[INFO] Python ejecutado: {sys.executable}")

# ------------------------------
# .env y paths
# ------------------------------
ENV_PATH = os.environ.get("ETL_ENV_PATH", r"E:\ETL\ETL_DIARCO\.env")
if not os.path.exists(ENV_PATH):
    print(f"[ERROR] No existe el archivo .env en: {ENV_PATH}")
    print(f"[DEBUG] Directorio actual: {os.getcwd()}")
    sys.exit(1)

secrets = dotenv_values(ENV_PATH)

# --------------------------------------------------------------------
# Mapeos de columnas
# --------------------------------------------------------------------
# Mapeo explícito PG -> SQL: SOLO las columnas que se deben publicar
COLUMN_MAP_PG_TO_SQL: Dict[str, str] = {
    "c_proveedor": "C_PROVEEDOR",
    "c_articulo": "C_ARTICULO",
    "c_sucu_empr": "C_SUCU_EMPR",
    "q_bultos_kilos_diarco": "Q_BULTOS_KILOS_DIARCO",
    "f_alta_sist": "F_ALTA_SIST",
    "c_usuario_genero_oc": "C_USUARIO_GENERO_OC",
    "c_terminal_genero_oc": "C_TERMINAL_GENERO_OC",
    "f_genero_oc": "F_GENERO_OC",
    "c_usuario_bloqueo": "C_USUARIO_BLOQUEO",
    "m_procesado": "M_PROCESADO",
    "f_procesado": "F_PROCESADO",
    "u_prefijo_oc": "U_PREFIJO_OC",
    "u_sufijo_oc": "U_SUFIJO_OC",
    # Equivalencia histórica:
    #   PG: c_compra_connexa  ->  SQL: C_COMPRA_KIKKER
    "c_compra_connexa": "C_COMPRA_KIKKER",
    "c_usuario_modif": "C_USUARIO_MODIF",
    "c_comprador": "C_COMPRADOR",
}

# Alias para truncado: PG -> columna de referencia en destino
# (para casos en que el nombre en PG difiere del nombre real en SQL)
WIDTH_ALIAS: Dict[str, str] = {
    "c_compra_connexa": "c_compra_kikker",
}

# PK lógica en SQL (nombres de columnas destino)
PK_COLS_SQL: List[str] = [
    "C_COMPRA_KIKKER",
    "C_PROVEEDOR",
    "C_ARTICULO",
    "C_SUCU_EMPR",
]

# PK equivalente en PG (por si alguna vez se usa fallback)
PK_COLS_PG: List[str] = [
    "c_compra_connexa",
    "c_proveedor",
    "c_articulo",
    "c_sucu_empr",
]

# --------------------------------------------------------------------
# Conexiones
# --------------------------------------------------------------------
def pg_connect():
    try:
        conn = pg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        logging.error(f"❌ Error conectando a PostgreSQL: {e}")
        return None


def sqlsrv_connect():
    try:
        conn = pyodbc.connect(
            f"DRIVER={os.getenv('SQLP_DRIVER')};"
            f"SERVER={os.getenv('SQLP_SERVER')},{os.getenv('SQLP_PORT')};"
            f"DATABASE={os.getenv('SQLP_DATABASE')};"
            f"UID={os.getenv('SQLP_USER')};"
            f"PWD={os.getenv('SQLP_PASSWORD')};"
            "TrustServerCertificate=yes;"
        )
        return conn
    except Exception as e:
        logging.error(f"❌ Error conectando a SQL Server: {e}")
        return None

# --------------------------------------------------------------------
# Leer metadatos del destino
# --------------------------------------------------------------------
def leer_anchos_destino(cursor_sql, schema: str, table: str) -> Dict[str, Dict[str, object]]:
    """
    Devuelve un dict:
        { nombre_columna_lower: {"dtype": ..., "length": ...}, ... }
    basado en INFORMATION_SCHEMA.COLUMNS
    """
    query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    cursor_sql.execute(query, (schema, table))

    widths: Dict[str, Dict[str, object]] = {}
    for col, dtype, length in cursor_sql.fetchall():
        widths[col.lower()] = {"dtype": dtype.lower(), "length": length}
    logging.info(f"📐 Columnas destino ({schema}.{table}): {len(widths)}")
    return widths


# --------------------------------------------------------------------
# Truncado automático de texto
# --------------------------------------------------------------------
def truncar_df(df: pd.DataFrame, widths: Dict[str, Dict[str, object]]) -> pd.DataFrame:
    """
    Trunca columnas texto de df según los anchos del destino.
    Usa WIDTH_ALIAS para columnas cuyo nombre en PG difiere del de SQL.
    """
    df2 = df.copy()
    for col in df2.columns:
        col_lower = col.lower()
        meta_key = WIDTH_ALIAS.get(col_lower, col_lower)

        meta = widths.get(meta_key)
        if not meta:
            continue

        dtype = meta["dtype"]
        length = meta["length"]

        if dtype in ("varchar", "nvarchar", "char", "nchar") and length and length > 0:
            serie = df2[col]
            mask = serie.notna()
            df2.loc[mask, col] = serie[mask].astype(str).str.slice(0, length)
    return df2


# --------------------------------------------------------------------
# Obtener compras directas a publicar
# --------------------------------------------------------------------
def obtener_compras_directas(conn_pg) -> pd.DataFrame:
    """
    Recupera las compras directas pendientes de publicar.
    Incluye campos adicionales (q_consolidada, stock, ajuste) que
    NO se publican pero pueden ser útiles para diagnóstico/log.
    """
    sql = """
        SELECT 
            c_proveedor, 
            c_articulo, 
            c_sucu_empr, 
            q_bultos_kilos_diarco,
            f_alta_sist, 
            c_usuario_genero_oc, 
            c_terminal_genero_oc,
            f_genero_oc, 
            c_usuario_bloqueo, 
            m_procesado, 
            f_procesado,
            u_prefijo_oc, 
            u_sufijo_oc, 
            c_compra_connexa, 
            c_usuario_modif,
            c_comprador, 
            m_publicado, 
            id, 
            q_consolidada, 
            stock, 
            ajuste
        FROM public.t080_oc_precarga_connexa
        WHERE m_publicado = false
    """
    df = pd.read_sql(sql, conn_pg)
    logging.info(f"📦 Compras directas pendientes en PG: {len(df)}")
    return df


# --------------------------------------------------------------------
# Preparar DF para SQL Server usando COLUMN_MAP_PG_TO_SQL
# --------------------------------------------------------------------
def preparar_df_para_sql(
    df: pd.DataFrame,
    widths: Dict[str, Dict[str, object]],
    column_map_pg_to_sql: Dict[str, str],
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Construye un DataFrame listo para SQL Server:
    - Solo columnas mapeadas en COLUMN_MAP_PG_TO_SQL
    - Renombra las columnas al nombre de destino (SQL)
    - Respeta el orden del mapeo
    """
    src_cols_present: List[str] = []
    dest_cols: List[str] = []

    for src_col, dest_col in column_map_pg_to_sql.items():
        if src_col in df.columns:
            # Verifican que exista también en el destino (widths) por seguridad
            if dest_col.lower() in widths:
                src_cols_present.append(src_col)
                dest_cols.append(dest_col)
            else:
                logging.warning(
                    f"⚠ Columna destino '{dest_col}' (from '{src_col}') "
                    f"no existe en metadatos de SQL; se omite."
                )
        else:
            logging.warning(
                f"⚠ Columna origen '{src_col}' no está en el DataFrame de PG; se omite."
            )

    if not src_cols_present:
        raise RuntimeError(
            "No hay columnas comunes entre el DataFrame de PG y el mapeo COLUMN_MAP_PG_TO_SQL."
        )

    df_sql = df[src_cols_present].copy()
    # Renombrar columnas al nombre de destino (SQL)
    rename_dict = {src: dest for src, dest in zip(src_cols_present, dest_cols)}
    df_sql.rename(columns=rename_dict, inplace=True)

    logging.info(
        f"🧩 Columnas que se enviarán a SQL Server (PG -> SQL): "
        f"{[(s, rename_dict[s]) for s in src_cols_present]}"
    )

    return df_sql, dest_cols


# --------------------------------------------------------------------
# Publicar en SQL Server con patrón UPDATE + INSERT
# --------------------------------------------------------------------
def normalizar_valor_sql(col: str, valor_raw):
    """
    Normaliza el valor según el DDL de T080_OC_PRECARGA_KIKKER
    y según la lógica histórica del rows_iter original.

    - Evita mandar NULL en columnas NOT NULL (usa "", "N" o 0).
    - Convierte tipos a los esperados (int para decimales sin escala, str para char).
    """
    import pandas as pd

    # PKs numéricas: NO deberían venir nulas
    if col in ("C_PROVEEDOR", "C_ARTICULO", "C_SUCU_EMPR", "C_COMPRADOR",
               "U_PREFIJO_OC", "U_SUFIJO_OC"):
        if pd.isna(valor_raw):
            # Para PKs sería grave tener NULL; preferimos explotar
            if col in ("C_PROVEEDOR", "C_ARTICULO", "C_SUCU_EMPR"):
                raise ValueError(f"Valor nulo en columna PK numérica {col}")
            # Para otras numéricas NOT NULL seguimos criterio histórico: default 0
            return 0
        # Viene como float o string, lo llevamos a int
        return int(valor_raw)

    # Cantidad (decimal(13,3))
    if col == "Q_BULTOS_KILOS_DIARCO":
        if pd.isna(valor_raw):
            # En principio no debería venir vacío; por seguridad 0
            return 0
        # Dejarlo como float/decimal; el driver castea a decimal(13,3)
        return float(valor_raw)

    # CHAR(10)/CHAR(15)/CHAR(20) NOT NULL
    if col in ("C_USUARIO_GENERO_OC", "C_TERMINAL_GENERO_OC",
               "C_USUARIO_BLOQUEO", "C_COMPRA_KIKKER", "C_USUARIO_MODIF"):
        # Igual que el script original: str(valor or "")
        return str(valor_raw or "")

    # M_PROCESADO CHAR(1) NOT NULL
    if col == "M_PROCESADO":
        # Script original: str(m_procesado or "N")
        return str(valor_raw or "N")

    # DATETIME NOT NULL (asumimos que vienen informados desde PG)
    if col in ("F_ALTA_SIST", "F_GENERO_OC", "F_PROCESADO"):
        if pd.isna(valor_raw):
            # Si llegase a venir vacío, dejamos None
            # (asumiendo que en la práctica no ocurre o hay default en la tabla)
            return datetime(1900, 1, 1, 0, 0, 0, 0)

        # pandas.Timestamp -> datetime
        return valor_raw.to_pydatetime() if hasattr(valor_raw, "to_pydatetime") else valor_raw

    # Fallback genérico: respetar valor, pero convertir NaN a None
    if pd.isna(valor_raw):
        return None
    return valor_raw


def publicar_compras_directas(
    df_pg: pd.DataFrame,
    df_sql: pd.DataFrame,
    cols_sql: List[str],
    cursor_sql,
    schema: str,
    table: str,
) -> Set:
    """
    df_pg  = DF original (incluye 'id' para marcar publicadas)
    df_sql = DF con columnas renombradas a nombres de destino (SQL)
    cols_sql = lista de columnas en destino en el orden de inserción
    """
    import pandas as pd

    if df_sql.empty:
        logging.info("⚠ No hay compras directas para publicar en SQL Server.")
        return set()

    # Validan que las columnas PK existan en df_sql
    for pk in PK_COLS_SQL:
        if pk not in df_sql.columns:
            raise RuntimeError(
                f"La columna PK destino '{pk}' no está presente entre las columnas a enviar: {list(df_sql.columns)}"
            )

    # Columnas a actualizar (todas menos la PK)
    non_pk_cols = [c for c in cols_sql if c not in PK_COLS_SQL]

    if not non_pk_cols:
        raise RuntimeError(
            "No hay columnas no-PK para actualizar en el UPSERT; revisar configuración cols_sql / PK_COLS_SQL."
        )

    set_clause = ", ".join([f"{c} = ?" for c in non_pk_cols])
    where_clause = " AND ".join([f"{c} = ?" for c in PK_COLS_SQL])

    insert_cols = ", ".join(cols_sql)
    insert_placeholders = ", ".join(["?" for _ in cols_sql])

    tsql = f"""
        UPDATE {schema}.{table}
        SET {set_clause}
        WHERE {where_clause};

        IF @@ROWCOUNT = 0
        BEGIN
            INSERT INTO {schema}.{table} ({insert_cols})
            VALUES ({insert_placeholders});
        END;
    """

    logging.info("📝 SQL UPSERT (UPDATE+INSERT) preparado para Compras Directas.")

    published_ids: Set = set()

    for idx, row_sql in df_sql.iterrows():
        # Fila correspondiente en el DF original (para obtener 'id' y/o PK origen)
        row_pg = df_pg.loc[idx]

        # 1) valores SET (no PK) normalizados
        vals_set = [normalizar_valor_sql(c, row_sql[c]) for c in non_pk_cols]
        # 2) valores WHERE (PK destino) normalizados
        vals_where = [normalizar_valor_sql(c, row_sql[c]) for c in PK_COLS_SQL]
        # 3) valores INSERT (todas las columnas destino) normalizados
        vals_insert = [normalizar_valor_sql(c, row_sql[c]) for c in cols_sql]

        # Armamos lista final de parámetros
        params = vals_set + vals_where + vals_insert

        try:
            cursor_sql.execute(tsql, params)

            # Registrar ID PG si existe
            if "id" in df_pg.columns:
                published_ids.add(int(row_pg["id"]))
            else:
                # Fallback por PK de negocio (lado PG)
                try:
                    pk_tuple = tuple(row_pg[pg_col] for pg_col in PK_COLS_PG)
                    published_ids.add(pk_tuple)
                except Exception:
                    logging.warning(
                        f"⚠ No se pudo construir PK de negocio PG para fila idx={idx}."
                    )

        except Exception as e:
            pk_log = ", ".join(
                [f"{pk}={row_sql[pk]!r}" for pk in PK_COLS_SQL]
            )
            logging.error(
                f"❌ Error publicando fila idx={idx}, PK destino=({pk_log}): {e}"
            )
            raise

    logging.info(f"✅ Filas publicadas/actualizadas en SQL Server: {len(published_ids)}")
    return published_ids



# --------------------------------------------------------------------
# Marcar como publicadas en PostgreSQL
# --------------------------------------------------------------------
def marcar_publicadas(conn_pg, published_ids: Set):
    if not published_ids:
        logging.info("⚠ No hay IDs para marcar como publicados en PostgreSQL.")
        return

    if all(isinstance(x, int) for x in published_ids):
        # Caso normal: id entero
        sql = """
            UPDATE public.t080_oc_precarga_connexa
               SET m_publicado = TRUE,
                   f_procesado = NOW()
             WHERE id = ANY(%s)
        """
        ids_list = list(published_ids)
        with conn_pg.cursor() as cur:
            cur.execute(sql, (ids_list,))
        logging.info(f"✅ Filas marcadas como publicadas en PG por ID: {len(ids_list)}")
    else:
        # Fallback por PK de negocio (si no existiera 'id'): no lo aplicamos por seguridad
        logging.info(
            "⚠ published_ids no son enteros. Se esperaba marcar por ID. "
            "Por seguridad NO se marca nada."
        )


# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------
def main():
    logging.info("🚀 Iniciando PUBLICACIÓN COMPRAS DIRECTAS...")

    conn_pg = None
    conn_sql = None

    try:
        conn_pg = pg_connect()
        if conn_pg is None:
            raise RuntimeError("No se pudo abrir conexión a PostgreSQL.")

        conn_sql = sqlsrv_connect()
        if conn_sql is None:
            raise RuntimeError("No se pudo abrir conexión a SQL Server.")

        cursor_sql = conn_sql.cursor()

        # Metadatos del destino
        schema = "dbo"
        table = "T080_OC_PRECARGA_KIKKER"
        widths = leer_anchos_destino(cursor_sql, schema, table)

        # Datos origen (PG)
        df_pg = obtener_compras_directas(conn_pg)

        if df_pg.empty:
            logging.info("⏹ No hay compras directas pendientes. Fin.")
            conn_pg.commit()
            return

        # Truncado de texto según destino (incluye alias para c_compra_connexa)
        df_pg_trunc = truncar_df(df_pg, widths)

        # Filtrar SOLO columnas mapeadas y renombrar a nombres de destino
        df_sql, cols_sql = preparar_df_para_sql(
            df_pg_trunc,
            widths,
            COLUMN_MAP_PG_TO_SQL,
        )

        # Publicar en SQL Server
        published_ids = publicar_compras_directas(
            df_pg=df_pg_trunc,
            df_sql=df_sql,
            cols_sql=cols_sql,
            cursor_sql=cursor_sql,
            schema=schema,
            table=table,
        )

        # Commit en SQL Server
        conn_sql.commit()

        # Marcar como publicadas en PG
        marcar_publicadas(conn_pg, published_ids)
        conn_pg.commit()

        logging.info("🏁 Publicación de compras directas finalizada OK.")

    except Exception as e:
        logging.error(f"❌ Error general en S90_PUBLICAR_COMPRAS_DIRECTAS.py: {e}")
        traceback.print_exc()
        if conn_pg:
            conn_pg.rollback()
        if conn_sql:
            conn_sql.rollback()
        # Propagar error para que Prefect vea exit code != 0
        raise

    finally:
        if conn_pg:
            conn_pg.close()
        if conn_sql:
            conn_sql.close()


if __name__ == "__main__":
    main()
