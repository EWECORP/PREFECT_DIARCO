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

Ajustes 2026-03-12:
- Se corrige el tratamiento de id en PostgreSQL: ahora puede ser UUID
- Se corrige el marcado de publicados en PG para soportar UUID y enteros
- Se encapsula la normalización de ID PG
- Se mejora el tipado de published_ids

Revisión original: 2025-11-14
Autor: EWE / Zeetrex
"""

import os
import sys
import logging
import traceback
import io
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

import pandas as pd
import psycopg2 as pg2
import pyodbc
from dotenv import load_dotenv, dotenv_values


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

PublishedId = Union[str, int]

M_PROCESADO_STAGING = "X"
M_PROCESADO_PENDIENTE = "N"


def es_compra_directa(valor) -> bool:
    """Identifica compras directas por prefijo D en c_compra_connexa."""
    if pd.isna(valor):
        return False
    return str(valor).strip().upper().startswith("D")


# --------------------------------------------------------------------
# Helpers de IDs PG
# --------------------------------------------------------------------
def es_uuid_valido(valor) -> bool:
    """Devuelve True si el valor puede interpretarse como UUID."""
    if valor is None:
        return False
    try:
        UUID(str(valor).strip())
        return True
    except Exception:
        return False


def es_int_valido(valor) -> bool:
    """Devuelve True si el valor puede interpretarse como entero."""
    if valor is None:
        return False
    try:
        int(str(valor).strip())
        return True
    except Exception:
        return False


def normalizar_id_pg(valor) -> str:
    """
    Normaliza el ID de PostgreSQL como texto.
    Acepta UUID, int o string.
    """
    if pd.isna(valor):
        raise ValueError("id nulo en PostgreSQL")

    valor_str = str(valor).strip()
    if not valor_str:
        raise ValueError("id vacío en PostgreSQL")

    return valor_str


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

        if dtype in ("varchar", "nvarchar", "char", "nchar") and length and length > 0: # type: ignore
            serie = df2[col]
            mask = serie.notna()
            df2.loc[mask, col] = serie[mask].astype(str).str.slice(0, length) # type: ignore
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
          AND UPPER(COALESCE(c_compra_connexa, '')) LIKE 'D%%'
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
    # PKs numéricas / numéricas obligatorias
    if col in (
        "C_PROVEEDOR",
        "C_ARTICULO",
        "C_SUCU_EMPR",
        "C_COMPRADOR",
        "U_PREFIJO_OC",
        "U_SUFIJO_OC",
    ):
        if pd.isna(valor_raw):
            if col in ("C_PROVEEDOR", "C_ARTICULO", "C_SUCU_EMPR"):
                raise ValueError(f"Valor nulo en columna PK numérica {col}")
            return 0
        return int(valor_raw)

    # Cantidad (decimal(13,3))
    if col == "Q_BULTOS_KILOS_DIARCO":
        if pd.isna(valor_raw):
            return 0
        return float(valor_raw)

    # CHAR/VARCHAR NOT NULL
    if col in (
        "C_USUARIO_GENERO_OC",
        "C_TERMINAL_GENERO_OC",
        "C_USUARIO_BLOQUEO",
        "C_COMPRA_KIKKER",
        "C_USUARIO_MODIF",
    ):
        return str(valor_raw or "")

    # M_PROCESADO CHAR(1) NOT NULL
    if col == "M_PROCESADO":
        return str(valor_raw or "N")

    # DATETIME
    if col in ("F_ALTA_SIST", "F_GENERO_OC", "F_PROCESADO"):
        if pd.isna(valor_raw):
            return datetime(1900, 1, 1, 0, 0, 0, 0)
        return valor_raw.to_pydatetime() if hasattr(valor_raw, "to_pydatetime") else valor_raw

    # Fallback
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
) -> Set[PublishedId]:
    """
    df_pg  = DF original (incluye 'id' para marcar publicadas)
    df_sql = DF con columnas renombradas a nombres de destino (SQL)
    cols_sql = lista de columnas en destino en el orden de inserción
    """
    if df_sql.empty:
        logging.info("⚠ No hay compras directas para publicar en SQL Server.")
        return set()

    for pk in PK_COLS_SQL:
        if pk not in df_sql.columns:
            raise RuntimeError(
                f"La columna PK destino '{pk}' no está presente entre las columnas a enviar: {list(df_sql.columns)}"
            )

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

    published_ids: Set[PublishedId] = set()

    for idx, row_sql in df_sql.iterrows():
        row_pg = df_pg.loc[idx] # pyright: ignore[reportArgumentType, reportCallIssue]

        vals_set = [normalizar_valor_sql(c, row_sql[c]) for c in non_pk_cols]
        vals_where = [normalizar_valor_sql(c, row_sql[c]) for c in PK_COLS_SQL]
        vals_insert = [normalizar_valor_sql(c, row_sql[c]) for c in cols_sql]

        params = vals_set + vals_where + vals_insert

        try:
            cursor_sql.execute(tsql, params)

            if "id" in df_pg.columns:
                published_ids.add(normalizar_id_pg(row_pg["id"]))
            else:
                try:
                    pk_tuple = tuple(row_pg[pg_col] for pg_col in PK_COLS_PG)
                    published_ids.add(str(pk_tuple))
                except Exception:
                    logging.warning(
                        f"⚠ No se pudo construir PK de negocio PG para fila idx={idx}."
                    )

        except Exception as e:
            pk_log = ", ".join([f"{pk}={row_sql[pk]!r}" for pk in PK_COLS_SQL])
            logging.error(
                f"❌ Error publicando fila idx={idx}, PK destino=({pk_log}): {e}"
            )
            raise

    logging.info(f"✅ Filas publicadas/actualizadas en SQL Server: {len(published_ids)}")
    return published_ids


def obtener_ids_verificados_en_sql_server(
    df_pg: pd.DataFrame,
    df_sql: pd.DataFrame,
    cursor_sql,
    schema: str,
    table: str,
    estado: Optional[str] = None,
) -> Set[PublishedId]:
    """
    Verifica contra SQL Server cuáles filas del lote existen realmente
    luego del commit y devuelve sólo los IDs de PG respaldados por destino.
    """
    if df_sql.empty:
        return set()

    compras = tuple(sorted(df_sql["C_COMPRA_KIKKER"].dropna().astype(str).unique().tolist()))
    proveedores = tuple(sorted(df_sql["C_PROVEEDOR"].dropna().astype(int).unique().tolist()))
    sucursales = tuple(sorted(df_sql["C_SUCU_EMPR"].dropna().astype(int).unique().tolist()))

    if not compras or not proveedores or not sucursales:
        logging.warning("⚠ No hay claves suficientes para verificar publicaciones en SQL Server.")
        return set()

    compras_sql = "(" + ",".join(["?"] * len(compras)) + ")"
    proveedores_sql = "(" + ",".join(["?"] * len(proveedores)) + ")"
    sucursales_sql = "(" + ",".join(["?"] * len(sucursales)) + ")"

    filtro_estado = "" if estado is None else "AND M_PROCESADO = ?"
    sql = f"""
        SELECT C_COMPRA_KIKKER, C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR
        FROM {schema}.{table}
        WHERE C_COMPRA_KIKKER IN {compras_sql}
          AND C_PROVEEDOR IN {proveedores_sql}
          AND C_SUCU_EMPR IN {sucursales_sql}
          {filtro_estado}
    """

    params = list(compras) + list(proveedores) + list(sucursales)
    if estado is not None:
        params.append(estado)
    cursor_sql.execute(sql, params)
    existentes = {
        (str(r[0]).strip(), int(r[1]), int(r[2]), int(r[3]))
        for r in cursor_sql.fetchall()
    }

    verified_ids: Set[PublishedId] = set()
    missing_keys: List[Tuple[str, int, int, int]] = []

    for idx, row_sql in df_sql.iterrows():
        pk = (
            str(row_sql["C_COMPRA_KIKKER"]).strip(),
            int(row_sql["C_PROVEEDOR"]),
            int(row_sql["C_ARTICULO"]),
            int(row_sql["C_SUCU_EMPR"]),
        )
        if pk in existentes:
            row_pg = df_pg.loc[idx] # pyright: ignore[reportArgumentType, reportCallIssue]
            if "id" in df_pg.columns:
                verified_ids.add(normalizar_id_pg(row_pg["id"]))
        else:
            missing_keys.append(pk)

    if missing_keys:
        logging.warning(
            f"⚠ Filas no verificadas en SQL Server luego del commit: {len(missing_keys)}. "
            f"Ejemplo: {missing_keys[:5]}"
        )

    logging.info(f"🔎 Filas verificadas en SQL Server: {len(verified_ids)}")
    return verified_ids


def cargar_claves_lote_sqlserver(cursor_sql, df_sql: pd.DataFrame) -> int:
    """
    Carga las claves completas únicas del lote en una tabla temporal.
    Se usa para verificar y liberar exactamente este lote de compras directas.
    """
    cursor_sql.execute("""
        IF OBJECT_ID('tempdb..#COMPRAS_DIRECTAS_KEYS') IS NOT NULL
            DROP TABLE #COMPRAS_DIRECTAS_KEYS;
    """)
    cursor_sql.execute("""
        CREATE TABLE #COMPRAS_DIRECTAS_KEYS (
            C_COMPRA_KIKKER VARCHAR(255) NOT NULL,
            C_PROVEEDOR INT NOT NULL,
            C_ARTICULO INT NOT NULL,
            C_SUCU_EMPR INT NOT NULL,
            PRIMARY KEY (C_COMPRA_KIKKER, C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR)
        );
    """)

    keys = {
        (
            str(row["C_COMPRA_KIKKER"]).strip(),
            int(row["C_PROVEEDOR"]),
            int(row["C_ARTICULO"]),
            int(row["C_SUCU_EMPR"]),
        )
        for _, row in df_sql.iterrows()
    }

    for key in keys:
        cursor_sql.execute(
            """
            INSERT INTO #COMPRAS_DIRECTAS_KEYS (
                C_COMPRA_KIKKER, C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR
            ) VALUES (?, ?, ?, ?)
            """,
            key,
        )
    return len(keys)


def contar_claves_lote_en_destino(
    cursor_sql,
    schema: str,
    table: str,
    estado: Optional[str] = None,
) -> int:
    filtro_estado = "" if estado is None else "AND target.M_PROCESADO = ?"
    sql = f"""
        SELECT COUNT(1)
        FROM #COMPRAS_DIRECTAS_KEYS AS keys_lote
        WHERE EXISTS (
            SELECT 1
            FROM {schema}.{table} AS target
            WHERE target.C_COMPRA_KIKKER = keys_lote.C_COMPRA_KIKKER
              AND target.C_PROVEEDOR = keys_lote.C_PROVEEDOR
              AND target.C_ARTICULO = keys_lote.C_ARTICULO
              AND target.C_SUCU_EMPR = keys_lote.C_SUCU_EMPR
              {filtro_estado}
        )
    """
    if estado is None:
        cursor_sql.execute(sql)
    else:
        cursor_sql.execute(sql, (estado,))
    return int(cursor_sql.fetchone()[0])


def liberar_lote_sqlserver(cursor_sql, schema: str, table: str, df_sql: pd.DataFrame) -> int:
    esperadas = cargar_claves_lote_sqlserver(cursor_sql, df_sql)
    if esperadas == 0:
        return 0

    encontradas = contar_claves_lote_en_destino(cursor_sql, schema, table)
    if encontradas != esperadas:
        raise RuntimeError(
            f"Lote incompleto en SQL Server: esperadas={esperadas}, encontradas={encontradas}. "
            "Se mantiene M_PROCESADO='X' y no se actualiza PostgreSQL."
        )

    cursor_sql.execute(
        f"""
        UPDATE target
           SET M_PROCESADO = ?
        FROM {schema}.{table} AS target
        INNER JOIN #COMPRAS_DIRECTAS_KEYS AS keys_lote
            ON target.C_COMPRA_KIKKER = keys_lote.C_COMPRA_KIKKER
           AND target.C_PROVEEDOR = keys_lote.C_PROVEEDOR
           AND target.C_ARTICULO = keys_lote.C_ARTICULO
           AND target.C_SUCU_EMPR = keys_lote.C_SUCU_EMPR
        WHERE target.M_PROCESADO = ?
        """,
        (M_PROCESADO_PENDIENTE, M_PROCESADO_STAGING),
    )

    listas = contar_claves_lote_en_destino(
        cursor_sql,
        schema,
        table,
        M_PROCESADO_PENDIENTE,
    )
    if listas != esperadas:
        raise RuntimeError(
            f"Lote no liberado completamente: esperadas={esperadas}, listas={listas}. "
            "Se revierte la liberación y no se actualiza PostgreSQL."
        )
    return listas


# --------------------------------------------------------------------
# Marcar como publicadas en PostgreSQL
# --------------------------------------------------------------------

def marcar_publicadas(conn_pg, published_ids: Set[PublishedId]):
    if not published_ids:
        logging.info("⚠ No hay IDs para marcar como publicados en PostgreSQL.")
        return

    ids_list = [str(x).strip() for x in published_ids]

    sql = """
        UPDATE public.t080_oc_precarga_connexa
           SET m_publicado = TRUE,
               f_procesado = NOW()
         WHERE id::text = ANY(%s::text[])
    """

    with conn_pg.cursor() as cur:
        cur.execute(sql, (ids_list,))

    logging.info(f"✅ Filas marcadas como publicadas en PG por ID texto: {len(ids_list)}")

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

        # Defensa adicional: si por algún motivo el SELECT cambia, igual
        # no dejamos pasar compras no directas en este publicador.
        mask_directas = df_pg["c_compra_connexa"].apply(es_compra_directa)
        df_pg = df_pg.loc[mask_directas].copy()
        logging.info(f"🧭 Filas directas válidas en lote: {len(df_pg)}")
        if df_pg.empty:
            logging.info("⏹ No hay compras directas válidas para publicar. Fin.")
            conn_pg.commit()
            return

        # Truncado de texto según destino
        df_pg_trunc = truncar_df(df_pg, widths)

        # Filtrar SOLO columnas mapeadas y renombrar a nombres de destino
        df_sql, cols_sql = preparar_df_para_sql(
            df_pg_trunc,
            widths,
            COLUMN_MAP_PG_TO_SQL,
        )
        if "M_PROCESADO" not in df_sql.columns:
            raise RuntimeError("No se puede garantizar integridad: falta M_PROCESADO en el lote SQL.")
        df_sql["M_PROCESADO"] = M_PROCESADO_STAGING

        # Publicar en SQL Server en staging: el consumidor sólo toma M_PROCESADO='N'.
        attempted_ids = publicar_compras_directas(
            df_pg=df_pg_trunc,
            df_sql=df_sql,
            cols_sql=cols_sql,
            cursor_sql=cursor_sql,
            schema=schema,
            table=table,
        )

        # Commit en SQL Server con M_PROCESADO='X'.
        conn_sql.commit()

        released = liberar_lote_sqlserver(
            cursor_sql=cursor_sql,
            schema=schema,
            table=table,
            df_sql=df_sql,
        )
        conn_sql.commit()
        logging.info(f"✅ Lote liberado a M_PROCESADO='N' en SQL Server: {released} claves.")

        # Verificar contra SQL Server ya liberado antes de marcar publicadas en PG
        verified_ids = obtener_ids_verificados_en_sql_server(
            df_pg=df_pg_trunc,
            df_sql=df_sql,
            cursor_sql=cursor_sql,
            schema=schema,
            table=table,
            estado=M_PROCESADO_PENDIENTE,
        )

        if len(verified_ids) != len(attempted_ids):
            logging.warning(
                f"⚠ Diferencia entre filas intentadas y verificadas. "
                f"Intentadas={len(attempted_ids)} | Verificadas={len(verified_ids)}"
            )

        # Marcar como publicadas en PG solo las verificadas
        marcar_publicadas(conn_pg, verified_ids)
        conn_pg.commit()

        logging.info("🏁 Publicación de compras directas finalizada OK.")

    except Exception as e:
        logging.error(f"❌ Error general en S90_PUBLICAR_COMPRAS_DIRECTAS.py: {e}")
        traceback.print_exc()

        if conn_pg:
            conn_pg.rollback()
        if conn_sql:
            conn_sql.rollback()

        raise

    finally:
        if conn_pg:
            conn_pg.close()
        if conn_sql:
            conn_sql.close()


if __name__ == "__main__":
    main()
