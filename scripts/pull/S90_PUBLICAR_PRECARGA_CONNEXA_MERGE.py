# -*- coding: utf-8 -*-
# S90_PUBLICAR_PRECARGA_CONNEXA.py  (revisión: PK completa + truncado dinámico por esquema SQL Server)
"""
Proceso PULL que publica OCs desde diarco_data (PostgreSQL) hacia SGM (SQL Server).
- Evita duplicados por PK completa: (c_compra_connexa, c_proveedor, c_articulo, c_sucu_empr)
- Trunca las columnas de texto según el DDL real de destino (INFORMATION_SCHEMA) para evitar HY000 truncation.
- Marca como publicadas en PG sólo las compras efectivamente insertadas
  (o también las ya existentes si activan idempotencia=True).
"""
import os
import sys
import io
import time
import logging
import traceback
import warnings
from typing import Tuple, Set, Dict, Optional

import pandas as pd
import pyodbc
import psycopg2 as pg2
from sqlalchemy import create_engine, text
from dotenv import dotenv_values

# --- Filtros de warnings (usar CLASES, no cadenas) ---
warnings.filterwarnings("ignore", category=UserWarning, module=r"pandas(\.|$)")
warnings.filterwarnings("ignore", category=FutureWarning, module=r"pandas(\.|$)")

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

def _normpath(*parts: str) -> str:
    return os.path.normpath(os.path.join(*parts))

def _sanitize_windows_drive(p: str) -> str:
    # Corrige rutas tipo "E:ETL\..." -> "E:\ETL\..."
    if os.name == "nt" and len(p) >= 2 and p[1] == ":" and (len(p) == 2 or p[2] not in ("\\", "/")):
        return p[:2] + "\\" + p[2:]
    return p

BASE_DIR     = _sanitize_windows_drive(secrets.get("BASE_DIR", r"E:\ETL\ETL_DIARCO")) # type: ignore
FOLDER_DATOS = secrets.get("FOLDER_DATOS", "data")
FOLDER_LOGS  = secrets.get("FOLDER_LOG",  "logs")

folder      = _normpath(BASE_DIR, FOLDER_DATOS) # type: ignore
folder_logs = _normpath(BASE_DIR, FOLDER_LOGS) # type: ignore
os.makedirs(folder_logs, exist_ok=True)

log_file = _normpath(folder_logs, "publicacion_oc_precarga.log")
print(f"[INFO] Cargando configuración desde: {ENV_PATH}")
print(f"[INFO] Carpeta de datos: {folder}")
print(f"[INFO] Logs: {log_file}")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ------------------------------
# Conexiones
# ------------------------------
def open_sqlserver_connection():
    conn_str = (
        f"DRIVER={secrets['SQLP_DRIVER']};"
        f"SERVER={secrets['SQLP_SERVER']};"
        f"PORT={secrets['SQLP_PORT']};"
        f"DATABASE={secrets['SQLP_DATABASE']};"
        f"UID={secrets['SQLP_USER']};PWD={secrets['SQLP_PASSWORD']}"
    )
    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        logging.error(f"[ERROR] Conexión SQL Server: {e}")
        return None

def open_pg_sqlalchemy_engine():
    uri = (
        f"postgresql+psycopg2://{secrets['PG_USER']}:{secrets['PG_PASSWORD']}"
        f"@{secrets['PG_HOST']}:{secrets['PG_PORT']}/{secrets['PG_DB']}"
    )
    return create_engine(uri, pool_pre_ping=True)

def open_pg_psycopg2():
    conn_str = (
        f"dbname={secrets['PG_DB']} user={secrets['PG_USER']} "
        f"password={secrets['PG_PASSWORD']} host={secrets['PG_HOST']} port={secrets['PG_PORT']}"
    )
    for _ in range(5):
        try:
            return pg2.connect(conn_str)
        except Exception as e:
            logging.warning(f"[WARN] PG retry por conexión: {e}")
            time.sleep(5)
    return None

# ------------------------------
# Normalización / Validaciones
# ------------------------------
def _ensure_text_col(df: pd.DataFrame, col: str) -> None:
    """Crea la columna si no existe y la deja con dtype 'string' (admite NA)."""
    if col not in df.columns:
        df[col] = pd.Series([pd.NA] * len(df), dtype="string")
    else:
        df[col] = df[col].astype("string")

def limpiar_campos_oc(df: pd.DataFrame) -> pd.DataFrame:
    # Normalizar columnas de texto
    for c in ["c_usuario_genero_oc", "c_terminal_genero_oc", "c_usuario_bloqueo",
              "m_procesado", "c_compra_connexa", "c_usuario_modif"]:
        _ensure_text_col(df, c)

    # Textos y longitudes preliminares (se hará truncado final según DDL más adelante)
    df["c_usuario_genero_oc"]  = df["c_usuario_genero_oc"].fillna("").str.strip()
    df["c_terminal_genero_oc"] = df["c_terminal_genero_oc"].fillna("").str.strip()
    df["c_usuario_bloqueo"]    = df["c_usuario_bloqueo"].fillna("").str.strip()
    df["m_procesado"]          = df["m_procesado"].fillna("N").str.strip()
    df["c_compra_connexa"]     = df["c_compra_connexa"].fillna("").str.strip()
    df["c_usuario_modif"]      = df["c_usuario_modif"].fillna("").str.strip()

    # Claves / numéricos exactos
    for col in ["c_proveedor","c_articulo","c_sucu_empr","u_prefijo_oc","u_sufijo_oc","c_comprador"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Cantidades
    if "q_bultos_kilos_diarco" in df.columns:
        df["q_bultos_kilos_diarco"] = pd.to_numeric(df["q_bultos_kilos_diarco"], errors="coerce").fillna(0)
        df["q_bultos_kilos_diarco"] = df["q_bultos_kilos_diarco"].clip(lower=0).astype(int)

    # Fechas
    for dcol in ["f_alta_sist","f_genero_oc","f_procesado"]:
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
    if "f_genero_oc" in df.columns:
        df["f_genero_oc"] = df["f_genero_oc"].fillna(pd.Timestamp("1900-01-01 00:00:00"))
    if "f_procesado" in df.columns:
        df["f_procesado"] = df["f_procesado"].fillna(pd.Timestamp("1900-01-01 00:00:00"))

    # Deduplicación por PK completa (incluye compra)
    pk_cols = [c for c in ["c_compra_connexa","c_proveedor","c_articulo","c_sucu_empr"] if c in df.columns]
    if pk_cols:
        df = df.drop_duplicates(subset=pk_cols, keep="last").reset_index(drop=True)

    return df

def forzar_enteros(df: pd.DataFrame) -> pd.DataFrame:
    int_cols = [
        "c_proveedor","c_articulo","c_sucu_empr",
        "u_prefijo_oc","u_sufijo_oc","c_comprador",
        "q_bultos_kilos_diarco","c_proveedor_primario"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")
    if "m_publicado" in df.columns:
        df["m_publicado"] = df["m_publicado"].fillna(False).astype(bool)
    for dcol in ["f_alta_sist","f_genero_oc","f_procesado"]:
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
    return df

def validar_longitudes(df: pd.DataFrame):
    campos = [
        "c_usuario_genero_oc","c_terminal_genero_oc","c_usuario_bloqueo",
        "m_procesado","c_compra_connexa","c_usuario_modif"
    ]
    print("\n[INFO] Validando longitudes máximas de texto:")
    for col in campos:
        if col in df.columns:
            print(f"{col}: max_len={df[col].astype('string').str.len().max()}")

# ------------------------------
# Helpers para IN (...)
# ------------------------------
def _fmt_in_numeric(values: Tuple[int, ...]) -> str:
    return f"({values[0]})" if len(values) == 1 else str(values)

def _fmt_in_string(values: Tuple[str, ...]) -> str:
    """Devuelve ('a','b','c') con escapado de comillas simples, sin f-strings en la expresión."""
    if len(values) == 1:
        v = values[0] if values[0] is not None else ""
        v = v.replace("'", "''")
        return "('" + v + "')"
    escaped = []
    for v in values:
        vv = v if v is not None else ""
        vv = vv.replace("'", "''")
        escaped.append("'" + vv + "'")
    return "(" + ",".join(escaped) + ")"

# ------------------------------
# Truncado dinámico según DDL de SQL Server
# ------------------------------
def leer_anchos_destino(cursor_ss, schema: str, table: str) -> Dict[str, Optional[int]]:
    """
    Retorna {columna: CHARACTER_MAXIMUM_LENGTH or None} para columnas textuales.
    Para tipos no textuales, devuelve None (no se truncan).
    """
    q = """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    cursor_ss.execute(q, (schema, table))
    out: Dict[str, Optional[int]] = {}
    for name, dtype, charlen in cursor_ss.fetchall():
        if dtype in ("char","varchar","nchar","nvarchar"):
            out[name.upper()] = int(charlen) if charlen is not None else None
        else:
            out[name.upper()] = None
    return out

def truncar_según_schema(df: pd.DataFrame, lens: Dict[str, Optional[int]]) -> pd.DataFrame:
    """
    Aplica slicing por longitud real del destino a las columnas de texto
    que intervenimos en el INSERT.
    """
    # Mapa origen->destino
    mapping = {
        "c_usuario_genero_oc":  "C_USUARIO_GENERO_OC",
        "c_terminal_genero_oc": "C_TERMINAL_GENERO_OC",
        "c_usuario_bloqueo":    "C_USUARIO_BLOQUEO",
        "m_procesado":          "M_PROCESADO",
        "c_compra_connexa":     "C_COMPRA_KIKKER",
        "c_usuario_modif":      "C_USUARIO_MODIF",
    }
    for src_col, dst_col in mapping.items():
        if src_col in df.columns and dst_col in lens and lens[dst_col] is not None:
            maxlen = lens[dst_col]  # p.ej., 40
            # Asegurar dtype string y recortar
            df[src_col] = df[src_col].astype("string").fillna("").str.slice(0, maxlen)
    return df

# ------------------------------
# Lectura de pendientes
# ------------------------------
def open_and_read_pending_from_pg() -> pd.DataFrame:
    eng_pg = open_pg_sqlalchemy_engine()
    with eng_pg.connect() as cx:
        df = pd.read_sql(
            text("""
                SELECT *
                FROM public.t080_oc_precarga_connexa
                WHERE m_publicado = false
            """),
            cx,
        )
    return df

# ------------------------------
# Publicación principal
# ------------------------------
def publicar_oc_precarga(idempotente_marcar_existentes: bool = False):
    logging.info("[START] Publicación OC Precarga (PK completa + truncado dinámico)")
    conn_ss = None
    cursor_ss = None

    try:
        # 1) Leer pendientes desde PG
        df_oc = open_and_read_pending_from_pg()
        if df_oc.empty:
            logging.info("[INFO] No hay pendientes (m_publicado = false).")
            print("✔ No hay registros pendientes para publicar.")
            return

        df_oc = forzar_enteros(df_oc)
        total_pend = len(df_oc)
        logging.info(f"[INFO] Pendientes leídos: {total_pend}")

        df_oc = limpiar_campos_oc(df_oc)
        validar_longitudes(df_oc)
        print(df_oc.head(5))

        # 2) Conexión SQL Server y lectura de anchos reales
        conn_ss = open_sqlserver_connection()
        if conn_ss is None:
            raise ConnectionError("No se pudo conectar a SQL Server.")
        cursor_ss = conn_ss.cursor()
        cursor_ss.fast_executemany = True

        # Leer anchos del destino
        schema = "dbo"
        table  = "T080_OC_PRECARGA_KIKKER"
        lens = leer_anchos_destino(cursor_ss, schema, table)

        # Truncar según DDL real (evita HY000 truncation)
        df_oc = truncar_según_schema(df_oc, lens)

        # 2.a) Anti-join contra destino usando PK completa
        proveedores = tuple(sorted(df_oc["c_proveedor"].astype(int).unique().tolist()))
        sucursales  = tuple(sorted(df_oc["c_sucu_empr"].astype(int).unique().tolist()))
        compras     = tuple(sorted(df_oc["c_compra_connexa"].astype(str).unique().tolist()))

        if not proveedores: proveedores = tuple([-1])
        if not sucursales:  sucursales  = tuple([-1])
        if not compras:     compras     = tuple(["__VOID__"])

        qry_exist = f"""
            SELECT C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR, C_COMPRA_KIKKER
            FROM {schema}.{table}
            WHERE C_PROVEEDOR     IN {_fmt_in_numeric(proveedores)}
              AND C_SUCU_EMPR     IN {_fmt_in_numeric(sucursales)}
              AND C_COMPRA_KIKKER IN {_fmt_in_string(compras)}
        """
        cursor_ss.execute(qry_exist)
        existentes: Set[Tuple[int,int,int,str]] = {
            (int(r[0]), int(r[1]), int(r[2]), str(r[3])) for r in cursor_ss.fetchall()
        }

        def _k4(row) -> Tuple[int,int,int,str]:
            return (
                int(row["c_proveedor"]),
                int(row["c_articulo"]),
                int(row["c_sucu_empr"]),
                str(row["c_compra_connexa"])
            )

        mask_insert = ~df_oc.apply(_k4, axis=1).isin(existentes)
        df_insert = df_oc[mask_insert].copy()
        df_omit   = df_oc[~mask_insert].copy()

        n_insert = len(df_insert)
        n_omit   = len(df_omit)
        logging.info(f"[INFO] Lote={total_pend} | A insertar={n_insert} | Omitidas(existían)={n_omit}")

        # 2.b) Inserción efectiva
        inserted_compra_ids: Set[str] = set()
        if n_insert > 0:
            insert_stmt = f"""
                INSERT INTO {schema}.{table} (
                    [C_PROVEEDOR], [C_ARTICULO], [C_SUCU_EMPR], [Q_BULTOS_KILOS_DIARCO],
                    [F_ALTA_SIST], [C_USUARIO_GENERO_OC], [C_TERMINAL_GENERO_OC], [F_GENERO_OC],
                    [C_USUARIO_BLOQUEO], [M_PROCESADO], [F_PROCESADO], [U_PREFIJO_OC],
                    [U_SUFIJO_OC], [C_COMPRA_KIKKER], [C_USUARIO_MODIF], [C_COMPRADOR]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            def rows_iter():
                for row in df_insert.itertuples(index=False):
                    yield (
                        int(getattr(row,"c_proveedor")),
                        int(getattr(row,"c_articulo")),
                        int(getattr(row,"c_sucu_empr")),
                        int(getattr(row,"q_bultos_kilos_diarco")),
                        getattr(row,"f_alta_sist").to_pydatetime() if pd.notna(getattr(row,"f_alta_sist")) else None,
                        str(getattr(row,"c_usuario_genero_oc") or "")[:10],
                        str(getattr(row,"c_terminal_genero_oc") or "")[:15],
                        getattr(row,"f_genero_oc").to_pydatetime() if pd.notna(getattr(row,"f_genero_oc")) else None,
                        str(getattr(row,"c_usuario_bloqueo") or "")[:10],
                        str(getattr(row,"m_procesado") or "N")[:1],
                        getattr(row,"f_procesado").to_pydatetime() if pd.notna(getattr(row,"f_procesado")) else None,
                        int(getattr(row,"u_prefijo_oc")) if pd.notna(getattr(row,"u_prefijo_oc")) else 0,
                        int(getattr(row,"u_sufijo_oc"))  if pd.notna(getattr(row,"u_sufijo_oc"))  else 0,
                        str(getattr(row,"c_compra_connexa") or "")[:20],  # Ajustar a su DDL
                        str(getattr(row,"c_usuario_modif") or "")[:20],
                        int(getattr(row,"c_comprador")) if pd.notna(getattr(row,"c_comprador")) else 0,
                    )

            batch = list(rows_iter())
            
            # - Versión Anterior sin fast_executemany:
            # cursor_ss.executemany(insert_stmt, batch)
            # conn_ss.commit()
            
            merge_stmt = f"""
            MERGE INTO {schema}.{table} AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
                C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR, Q_BULTOS_KILOS_DIARCO,
                F_ALTA_SIST, C_USUARIO_GENERO_OC, C_TERMINAL_GENERO_OC, F_GENERO_OC,
                C_USUARIO_BLOQUEO, M_PROCESADO, F_PROCESADO, U_PREFIJO_OC,
                U_SUFIJO_OC, C_COMPRA_KIKKER, C_USUARIO_MODIF, C_COMPRADOR
            )
            ON target.C_PROVEEDOR = source.C_PROVEEDOR
            AND target.C_ARTICULO = source.C_ARTICULO
            AND target.C_SUCU_EMPR = source.C_SUCU_EMPR
            AND target.C_COMPRA_KIKKER = source.C_COMPRA_KIKKER
            WHEN MATCHED THEN
                UPDATE SET
                    target.Q_BULTOS_KILOS_DIARCO = source.Q_BULTOS_KILOS_DIARCO,
                    target.F_ALTA_SIST = source.F_ALTA_SIST,
                    target.C_USUARIO_GENERO_OC = source.C_USUARIO_GENERO_OC,
                    target.C_TERMINAL_GENERO_OC = source.C_TERMINAL_GENERO_OC,
                    target.F_GENERO_OC = source.F_GENERO_OC,
                    target.C_USUARIO_BLOQUEO = source.C_USUARIO_BLOQUEO,
                    target.M_PROCESADO = source.M_PROCESADO,
                    target.F_PROCESADO = source.F_PROCESADO,
                    target.U_PREFIJO_OC = source.U_PREFIJO_OC,
                    target.U_SUFIJO_OC = source.U_SUFIJO_OC,
                    target.C_USUARIO_MODIF = source.C_USUARIO_MODIF,
                    target.C_COMPRADOR = source.C_COMPRADOR
            WHEN NOT MATCHED THEN
                INSERT (
                    C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR, Q_BULTOS_KILOS_DIARCO,
                    F_ALTA_SIST, C_USUARIO_GENERO_OC, C_TERMINAL_GENERO_OC, F_GENERO_OC,
                    C_USUARIO_BLOQUEO, M_PROCESADO, F_PROCESADO, U_PREFIJO_OC,
                    U_SUFIJO_OC, C_COMPRA_KIKKER, C_USUARIO_MODIF, C_COMPRADOR
                )
                VALUES (
                    source.C_PROVEEDOR, source.C_ARTICULO, source.C_SUCU_EMPR, source.Q_BULTOS_KILOS_DIARCO,
                    source.F_ALTA_SIST, source.C_USUARIO_GENERO_OC, source.C_TERMINAL_GENERO_OC, source.F_GENERO_OC,
                    source.C_USUARIO_BLOQUEO, source.M_PROCESADO, source.F_PROCESADO, source.U_PREFIJO_OC,
                    source.U_SUFIJO_OC, source.C_COMPRA_KIKKER, source.C_USUARIO_MODIF, source.C_COMPRADOR
                );
            """

            for row in df_insert.itertuples(index=False):
                cursor_ss.execute(merge_stmt, (
                    int(getattr(row,"c_proveedor")),
                    int(getattr(row,"c_articulo")),
                    int(getattr(row,"c_sucu_empr")),
                    int(getattr(row,"q_bultos_kilos_diarco")),
                    getattr(row,"f_alta_sist").to_pydatetime() if pd.notna(getattr(row,"f_alta_sist")) else None,
                    str(getattr(row,"c_usuario_genero_oc") or "")[:10],
                    str(getattr(row,"c_terminal_genero_oc") or "")[:15],
                    getattr(row,"f_genero_oc").to_pydatetime() if pd.notna(getattr(row,"f_genero_oc")) else None,
                    str(getattr(row,"c_usuario_bloqueo") or "")[:10],
                    str(getattr(row,"m_procesado") or "N")[:1],
                    getattr(row,"f_procesado").to_pydatetime() if pd.notna(getattr(row,"f_procesado")) else None,
                    int(getattr(row,"u_prefijo_oc")) if pd.notna(getattr(row,"u_prefijo_oc")) else 0,
                    int(getattr(row,"u_sufijo_oc"))  if pd.notna(getattr(row,"u_sufijo_oc"))  else 0,
                    str(getattr(row,"c_compra_connexa") or "")[:20],  # Ajustar a su DDL
                    str(getattr(row,"c_usuario_modif") or "")[:20],
                    int(getattr(row,"c_comprador")) if pd.notna(getattr(row,"c_comprador")) else 0,
                ))

            conn_ss.commit()
            print(f"✔ Inserciones/actualizaciones MERGE ejecutadas correctamente ({len(df_insert)} filas)")

            print(f"✔ Insertadas en SQL Server: {len(batch)} filas")
            logging.info(f"[INFO] Inserción SQL Server OK: {len(batch)}")

            inserted_compra_ids = set(df_insert["c_compra_connexa"].dropna().astype(str))

        # Idempotencia opcional: marcar también las “omitidas” por existir (misma PK completa)
        if idempotente_marcar_existentes and n_omit > 0:
            inserted_compra_ids |= set(df_omit["c_compra_connexa"].dropna().astype(str))

        # 3) Marcar como publicadas SOLO las compras insertadas (y/o existentes si idempotente=True)
        if inserted_compra_ids:
            conn_pg = open_pg_psycopg2()
            if conn_pg is None:
                raise ConnectionError("No se pudo reabrir PG para UPDATE.")
            with conn_pg:
                with conn_pg.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.t080_oc_precarga_connexa
                           SET m_publicado = true
                         WHERE m_publicado = false
                           AND c_compra_connexa = ANY(%s)
                        """,
                        (list(inserted_compra_ids),)
                    )
                    updated = cur.rowcount
            print(f"✔ {updated} registros actualizados con m_publicado = true")
            logging.info(f"[INFO] Publicadas en PG: {updated} (compras={len(inserted_compra_ids)})")
        else:
            print("ℹ No se marcaron publicaciones en PG (no hubo inserciones efectivas ni idempotencia activada).")
            logging.info("[INFO] Sin UPDATE PG (0 compras a marcar).")

    except Exception as e:
        logging.error("[ERROR] Fallo en publicación de OC Precarga")
        logging.error(traceback.format_exc())
        print("[ERROR] Error en la ejecución:", e)

    finally:
        try:
            if cursor_ss:
                cursor_ss.close()
        except Exception:
            pass
        try:
            if conn_ss:
                conn_ss.close()
        except Exception:
            pass

if __name__ == "__main__":
    # Activar True si desean marcar como publicadas también las filas que ya existen en SGM
    # con la MISMA compra (reintentos).
    publicar_oc_precarga(idempotente_marcar_existentes=False)
    print(f"[INFO] Proceso finalizado. Ver log en: {log_file}")
    logging.info("[END] Proceso de publicación finalizado.")
