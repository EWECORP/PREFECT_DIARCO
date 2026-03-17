# scripts/pull/publicar_transferencias_sgm.py
#
# Publica transferencias desde CONNEXA (PostgreSQL) hacia staging en SQL Server DMZ:
#   [data-sync].[repl].[TRANSF_CONNEXA_IN]
#
# Regla implementada:
# - Solo se publican detalles con stock disponible suficiente, bajo lógica
#   "todo o nada", descontando saldo en memoria por (sucursal origen, artículo).
# - Si no alcanza el saldo completo para una línea, esa línea NO se publica.
# - Las líneas no publicadas permanecen en Connexa bajo cabecera PRECARGA_CONNEXA.
# - La cabecera se actualiza a 80 (SINCRONIZANDO) solo cuando TODOS sus detalles
#   ya fueron publicados (previamente o en esta corrida).
#
# Reglas confirmadas:
# - qty_requested en Connexa YA está en BULTOS.
# - units_per_package = unidades por bulto (q_factor).
# - q_requerida = q_bultos * q_factor.
# - origin_cd puede venir como "41CD" / "82CD" y debe grabarse como 41 / 82.
#
# Notas:
# - Este script NO ejecuta el SP publicador SGM; solo carga staging + marca cabeceras en 80.
# - Para evitar duplicados por reintento, se filtran connexa_detail_uuid ya presentes en staging.

import os
import sys
import urllib.parse
from datetime import datetime
from typing import List
import uuid

import pandas as pd
from dotenv import dotenv_values, load_dotenv
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine


# =========================
# 1) CONFIGURACIÓN Y ENTORNO
# =========================
ENV_PATH = os.environ.get("ETL_ENV_PATH", r"E:\ETL\ETL_DIARCO\.env")
if not os.path.exists(ENV_PATH):
    print(f"[ERROR] No existe el archivo .env en: {ENV_PATH}")
    print(f"[DEBUG] Directorio actual: {os.getcwd()}")
    sys.exit(1)

secrets = dotenv_values(ENV_PATH)
load_dotenv(ENV_PATH)

print(
    f"[INFO] PGP_DB:{secrets.get('PGP_DB')} - PGP_HOST:{secrets.get('PGP_HOST')} - PGP_USER:{secrets.get('PGP_USER')}"
)


# =========================
# 2) CONEXIONES A BASES DE DATOS
# =========================
def get_pg_engine() -> Engine:
    """Devuelve un engine de SQLAlchemy para PostgreSQL (Connexa)."""
    host = os.getenv("PGP_HOST")
    port = os.getenv("PGP_PORT", "5432")
    db = os.getenv("PGP_DB")
    user = os.getenv("PGP_USER")
    pwd = os.getenv("PGP_PASSWORD")

    print(f"[INFO] Conectando a PostgreSQL: host={host}, port={port}, db={db}, user={user}")

    if not all([host, db, user, pwd, port]):
        raise RuntimeError("Faltan variables de entorno PGP_* para conectarse a PostgreSQL")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def get_sqlserver_engine() -> Engine:
    """
    Devuelve un engine para SQL Server (data-sync) usando pyodbc.
    Se utiliza 'Connect Timeout' de 30 segundos.
    """
    host = os.getenv("SQL_SERVER")
    port = os.getenv("SQL_PORT", "1433")
    db = os.getenv("SQL_DATABASE", "data-sync")
    user = os.getenv("SQL_USER")
    pwd = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    if not all([host, db, user, pwd]):
        raise RuntimeError("Faltan variables de entorno SQL_SERVER, SQL_DATABASE, SQL_USER, o SQL_PASSWORD")

    timeout_seconds = 30

    params = urllib.parse.quote_plus(
        f"DRIVER={driver};"
        f"SERVER={host},{port};"
        f"DATABASE={db};"
        f"UID={user};PWD={pwd};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        f"Connect Timeout={timeout_seconds};"
    )
    url = f"mssql+pyodbc:///?odbc_connect={params}"

    return create_engine(url, pool_pre_ping=True, fast_executemany=True)


# =========================
# 3) LECTURA Y NORMALIZACIÓN
# =========================
def obtener_transferencias_precarga(pg_engine: Engine) -> pd.DataFrame:
    """
    Obtiene todas las transferencias en estado PRECARGA_CONNEXA (detalle),
    incluyendo UUIDs de cabecera/detalle.
    """
    sql = """
    SELECT
        d.id                          AS connexa_detail_uuid,
        h.id                          AS connexa_header_uuid,
        h.origin_cd,
        h.destination_store_code,
        h.connexa_purchase_code,
        h.requested_at,
        h.created_by,
        h.created_at,
        h.status_id,
        s.code                        AS status_code,
        d.item_code,
        d.item_description,
        d.qty_requested,
        d.qty_planned,
        d.qty_shipped,
        d.qty_received,
        d.uom_id,
        d.units_per_package,
        d.packages_per_layer,
        d.layers_per_pallet
    FROM supply_planning.spl_distribution_transfer_detail d
    JOIN supply_planning.spl_distribution_transfer h
      ON d.distribution_transfer_id = h.id
    JOIN supply_planning.spl_distribution_transfer_status s
      ON h.status_id = s.id
    WHERE s.code = 'PRECARGA_CONNEXA';
    """
    return pd.read_sql(sql, pg_engine, parse_dates=["requested_at", "created_at"])


def normalizar_transferencias(df_src: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega columnas normalizadas para trabajar:
      - item_code_num
      - dest_store_num
      - origin_cd_num
      - connexa_header_uuid
      - connexa_detail_uuid
      - qty_requested_num
    """
    if df_src.empty:
        return df_src.copy()

    df = df_src.copy()

    df["item_code_num"] = pd.to_numeric(df["item_code"], errors="coerce").fillna(0).astype(int)
    df["dest_store_num"] = pd.to_numeric(df["destination_store_code"], errors="coerce").fillna(0).astype(int)
    df["qty_requested_num"] = pd.to_numeric(df["qty_requested"], errors="coerce").fillna(0.0).round(3)

    df["connexa_header_uuid"] = df["connexa_header_uuid"].astype(str).str.strip().str.lower()
    df["connexa_detail_uuid"] = df["connexa_detail_uuid"].astype(str).str.strip().str.lower()

    origin_str = df["origin_cd"].astype(str)
    df["origin_cd_num"] = (
        origin_str.str.extract(r"^(\d+)", expand=False)
        .fillna("0")
        .astype(int)
    )

    return df


# =========================
# 4) STOCK DISPONIBLE
# =========================
def obtener_stock_disponible(pg_engine: Engine, df_norm: pd.DataFrame) -> pd.DataFrame:
    """
    Obtiene q_bultos_disponible por (codigo_articulo, codigo_sucursal origen)
    desde src.base_stock_sucursal.
    """
    cols = ["item_code_num", "origin_cd_num"]
    if df_norm.empty or not set(cols).issubset(df_norm.columns):
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible"])

    articulos = sorted([int(x) for x in df_norm["item_code_num"].dropna().unique().tolist() if int(x) > 0])
    sucursales = sorted([int(x) for x in df_norm["origin_cd_num"].dropna().unique().tolist() if int(x) > 0])

    if not articulos or not sucursales:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible"])

    sql = """
        SELECT
            codigo_articulo::bigint AS item_code_num,
            codigo_sucursal::bigint AS origin_cd_num,
            MAX(
                FLOOR(
                    (COALESCE(stock, 0) + COALESCE(transfer_pendiente, 0))
                    / NULLIF(COALESCE(factor_venta, 0), 0)
                )
            )::bigint AS q_bultos_disponible
        FROM src.base_stock_sucursal
        WHERE codigo_sucursal = ANY(CAST(:lista_sucursales AS bigint[]))
          AND codigo_articulo = ANY(CAST(:lista_articulos AS bigint[]))
          AND COALESCE(factor_venta, 0) > 0
        GROUP BY codigo_articulo, codigo_sucursal
        HAVING MAX(
            FLOOR(
                (COALESCE(stock, 0) + COALESCE(transfer_pendiente, 0))
                / NULLIF(COALESCE(factor_venta, 0), 0)
            )
        ) > 0
    """

    with pg_engine.connect() as conn:
        df_stock = pd.read_sql(
            text(sql),
            conn,
            params={
                "lista_sucursales": sucursales,
                "lista_articulos": articulos,
            }, # type: ignore
        )

    if df_stock.empty:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "q_bultos_disponible"])

    df_stock["item_code_num"] = pd.to_numeric(df_stock["item_code_num"], errors="coerce").fillna(0).astype(int)
    df_stock["origin_cd_num"] = pd.to_numeric(df_stock["origin_cd_num"], errors="coerce").fillna(0).astype(int)
    df_stock["q_bultos_disponible"] = pd.to_numeric(df_stock["q_bultos_disponible"], errors="coerce").fillna(0).astype(float)

    return df_stock


def enriquecer_con_stock(df_norm: pd.DataFrame, df_stock: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega q_bultos_disponible inicial a cada detalle.
    """
    if df_norm.empty:
        df = df_norm.copy()
        df["q_bultos_disponible"] = pd.Series(dtype="float")
        return df

    df = df_norm.merge(
        df_stock,
        how="left",
        on=["item_code_num", "origin_cd_num"],
    )

    df["q_bultos_disponible"] = pd.to_numeric(df["q_bultos_disponible"], errors="coerce").fillna(0.0)
    return df


# =========================
# 5) DETALLES YA PUBLICADOS
# =========================
def obtener_detalles_ya_publicados(sql_engine: Engine, detail_uuids: List[str]) -> set[str]:
    """
    Devuelve el conjunto de connexa_detail_uuid que ya existen en repl.TRANSF_CONNEXA_IN.
    """
    ids = sorted({str(x).strip().lower() for x in detail_uuids if str(x).strip()})
    if not ids:
        return set()

    sql = text("""
        SELECT LOWER(LTRIM(RTRIM(connexa_detail_uuid))) AS connexa_detail_uuid
        FROM repl.TRANSF_CONNEXA_IN
        WHERE connexa_detail_uuid IN :ids
    """).bindparams(bindparam("ids", expanding=True))

    with sql_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"ids": ids}) # type: ignore

    if df.empty:
        return set()

    return set(df["connexa_detail_uuid"].astype(str).str.strip().str.lower().tolist())


def marcar_detalles_ya_publicados(df: pd.DataFrame, ya_publicados: set[str]) -> pd.DataFrame:
    out = df.copy()
    out["ya_publicado"] = out["connexa_detail_uuid"].isin(ya_publicados)
    return out


# =========================
# 6) ASIGNACIÓN EN MEMORIA (TODO O NADA)
# =========================
def asignar_stock_en_memoria_todo_o_nada(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asigna stock disponible en memoria por (origin_cd_num, item_code_num)
    bajo regla TODO O NADA.

    Orden de prioridad dentro de cada grupo:
      1. requested_at asc
      2. created_at asc
      3. connexa_detail_uuid asc

    Reglas:
      - Si la línea ya está publicada, no consume stock del saldo calculado
        en esta corrida. Se considera resuelta.
      - Si no está publicada:
          * si saldo >= qty_requested_num => asignar completa y descontar
          * si saldo < qty_requested_num  => no asignar
    """
    if df.empty:
        out = df.copy()
        out["saldo_inicial_grupo"] = pd.Series(dtype="float")
        out["saldo_antes"] = pd.Series(dtype="float")
        out["saldo_despues"] = pd.Series(dtype="float")
        out["q_bultos_asignado"] = pd.Series(dtype="float")
        out["publicable"] = pd.Series(dtype="bool")
        out["motivo_no_publicado"] = pd.Series(dtype="object")
        return out

    work = df.copy()

    work["requested_at_ord"] = pd.to_datetime(work["requested_at"], errors="coerce")
    work["created_at_ord"] = pd.to_datetime(work["created_at"], errors="coerce")
    work["qty_requested_num"] = pd.to_numeric(work["qty_requested_num"], errors="coerce").fillna(0.0).round(3)
    work["q_bultos_disponible"] = pd.to_numeric(work["q_bultos_disponible"], errors="coerce").fillna(0.0)

    work = work.sort_values(
        by=["origin_cd_num", "item_code_num", "requested_at_ord", "created_at_ord", "connexa_detail_uuid"],
        ascending=[True, True, True, True, True],
        kind="mergesort"
    ).copy()

    resultados = []

    for (origin_cd_num, item_code_num), grp in work.groupby(["origin_cd_num", "item_code_num"], sort=False):
        grp = grp.copy()

        saldo = float(grp["q_bultos_disponible"].iloc[0]) if len(grp) else 0.0
        saldo_inicial = saldo

        for _, row in grp.iterrows():
            row = row.copy()
            qty = float(row["qty_requested_num"]) if pd.notna(row["qty_requested_num"]) else 0.0

            row["saldo_inicial_grupo"] = round(saldo_inicial, 3)
            row["saldo_antes"] = round(saldo, 3)

            if bool(row.get("ya_publicado", False)):
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = True
                row["motivo_no_publicado"] = ""
                row["saldo_despues"] = round(saldo, 3)
                resultados.append(row)
                continue

            if qty <= 0:
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = False
                row["motivo_no_publicado"] = "QTY_REQUESTED_INVALIDA"
                row["saldo_despues"] = round(saldo, 3)
                resultados.append(row)
                continue

            if saldo >= qty:
                saldo -= qty
                row["q_bultos_asignado"] = round(qty, 3)
                row["publicable"] = True
                row["motivo_no_publicado"] = ""
                row["saldo_despues"] = round(saldo, 3)
            else:
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = False
                row["motivo_no_publicado"] = "SIN_STOCK_SUFICIENTE"
                row["saldo_despues"] = round(saldo, 3)

            resultados.append(row)

    df_res = pd.DataFrame(resultados)

    # Publicable para esta corrida = publicable y no estaba ya publicado
    df_res["publicable_ahora"] = df_res["publicable"] & (~df_res["ya_publicado"])

    return df_res


# =========================
# 7) TRANSFORMACIÓN A STAGING
# =========================
def transformar_a_staging(df_src: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame filtrado al formato esperado por:
      [data-sync].[repl].[TRANSF_CONNEXA_IN]

    Toma q_bultos_asignado como cantidad efectiva a publicar.
    """
    base_cols = [
        "c_articulo",
        "c_sucu_dest",
        "c_sucu_orig",
        "q_requerida",
        "q_bultos",
        "q_factor",
        "f_alta",
        "m_alta_prioridad",
        "vchUsuario",
        "vchTerminal",
        "forzarTransf",
        "estado",
        "mensaje_error",
        "connexa_header_uuid",
        "connexa_detail_uuid",
    ]

    if df_src.empty:
        return pd.DataFrame(columns=base_cols)

    df = df_src.copy()

    df["units_per_package"] = pd.to_numeric(df["units_per_package"], errors="coerce").fillna(1.0)
    df.loc[df["units_per_package"] <= 0, "units_per_package"] = 1.0

    df["q_factor"] = df["units_per_package"].round(0).astype(int)
    df["q_bultos"] = pd.to_numeric(df["q_bultos_asignado"], errors="coerce").fillna(0.0).round(3)
    df["q_requerida"] = (df["q_bultos"] * df["q_factor"]).round(3)

    now = datetime.now()
    df["f_alta"] = df["requested_at"].fillna(df["created_at"]).fillna(now)

    df["m_alta_prioridad"] = "N"
    df["vchUsuario"] = "CONNEXA"
    df["vchTerminal"] = "API"
    df["forzarTransf"] = "N"
    df["estado"] = "PENDIENTE"
    df["mensaje_error"] = ""

    df_stg = pd.DataFrame(
        {
            "c_articulo": df["item_code_num"],
            "c_sucu_dest": df["dest_store_num"],
            "c_sucu_orig": df["origin_cd_num"],
            "q_requerida": df["q_requerida"],
            "q_bultos": df["q_bultos"],
            "q_factor": df["q_factor"],
            "f_alta": pd.to_datetime(df["f_alta"]),
            "m_alta_prioridad": df["m_alta_prioridad"],
            "vchUsuario": df["vchUsuario"],
            "vchTerminal": df["vchTerminal"],
            "forzarTransf": df["forzarTransf"],
            "estado": df["estado"],
            "mensaje_error": df["mensaje_error"],
            "connexa_header_uuid": df["connexa_header_uuid"],
            "connexa_detail_uuid": df["connexa_detail_uuid"],
        }
    )

    df_stg = df_stg[
        (df_stg["c_articulo"] > 0)
        & (df_stg["c_sucu_dest"] > 0)
        & (df_stg["c_sucu_orig"] > 0)
        & (df_stg["q_bultos"] > 0)
        & (df_stg["q_factor"] > 0)
    ].copy()

    return df_stg


# =========================
# 8) INSERCIÓN Y UPDATE
# =========================
def insertar_en_staging_sqlserver(df_stg: pd.DataFrame, sql_engine: Engine) -> int:
    """Inserta filas en [data-sync].[repl].[TRANSF_CONNEXA_IN] usando to_sql."""
    if df_stg.empty:
        return 0

    try:
        df_stg.to_sql(
            name="TRANSF_CONNEXA_IN",
            con=sql_engine,
            schema="repl",
            if_exists="append",
            index=False,
        )
        return len(df_stg)
    except Exception as e:
        print(f"[ERROR] Error al insertar en SQL Server: {e}")
        raise


def _to_uuid_list(values: List[str]) -> List[uuid.UUID]:
    uuids: List[uuid.UUID] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip().lower()
        if not s:
            continue
        try:
            uuids.append(uuid.UUID(s))
        except Exception:
            pass
    return uuids


def actualizar_estado_cabeceras(pg_engine: Engine, header_uuids: List[str], nuevo_estado_id: int = 80) -> int:
    if not header_uuids:
        return 0

    header_uuid_objs = _to_uuid_list(header_uuids)
    if not header_uuid_objs:
        print("[WARN] No quedaron UUIDs válidos para actualizar cabeceras en Connexa.")
        return 0

    sql = """
        UPDATE supply_planning.spl_distribution_transfer
           SET status_id = :nuevo_estado,
               updated_at = NOW()
         WHERE id = ANY(CAST(:lista_ids AS uuid[]))
    """

    with pg_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {"nuevo_estado": nuevo_estado_id, "lista_ids": header_uuid_objs},
        )

    return result.rowcount


# =========================
# 9) CABECERAS COMPLETAS
# =========================
def obtener_headers_completamente_publicables(df_all: pd.DataFrame) -> List[str]:
    """
    Devuelve cabeceras para las que todos los detalles quedaron resueltos:
      - ya_publicado = True
      - o publicable = True
    Si queda aunque sea un detalle no publicable, la cabecera NO pasa a 80.
    """
    if df_all.empty:
        return []

    df = df_all.copy()
    df["quedara_publicado"] = df["ya_publicado"] | df["publicable"]

    resumen = (
        df.groupby("connexa_header_uuid", as_index=False)
          .agg(
              total_detalles=("connexa_detail_uuid", "count"),
              total_publicables=("quedara_publicado", "sum"),
          )
    )

    headers = resumen.loc[
        resumen["total_detalles"] == resumen["total_publicables"],
        "connexa_header_uuid"
    ].astype(str).str.lower().tolist()

    return sorted(set(headers))


# =========================
# 10) MAIN
# =========================
def main() -> int:
    try:
        pg_engine = get_pg_engine()
        sql_engine = get_sqlserver_engine()

        print("\n[INFO] Leyendo transferencias en estado PRECARGA_CONNEXA desde Connexa...")
        df_src = obtener_transferencias_precarga(pg_engine)
        print(f"[INFO] Registros origen (detalle): {len(df_src)}")

        if df_src.empty:
            print("[INFO] No hay transferencias en PRECARGA_CONNEXA para publicar.")
            return 0

        print("\n[INFO] Normalizando datos de origen...")
        df_norm = normalizar_transferencias(df_src)

        print("\n[INFO] Consultando detalles ya publicados en staging DMZ...")
        ya_publicados = obtener_detalles_ya_publicados(
            sql_engine,
            df_norm["connexa_detail_uuid"].astype(str).tolist()
        )
        print(f"[INFO] Detalles ya publicados previamente: {len(ya_publicados)}")

        print("\n[INFO] Consultando stock disponible por artículo + sucursal origen...")
        df_stock = obtener_stock_disponible(pg_engine, df_norm)
        print(f"[INFO] Combinaciones artículo/sucursal con stock disponible: {len(df_stock)}")

        print("\n[INFO] Enriqueciendo detalles con stock disponible...")
        df_work = enriquecer_con_stock(df_norm, df_stock)
        df_work = marcar_detalles_ya_publicados(df_work, ya_publicados)

        print("\n[INFO] Asignando stock en memoria (variante A - todo o nada)...")
        df_asignado = asignar_stock_en_memoria_todo_o_nada(df_work)

        df_a_insertar = df_asignado[df_asignado["publicable_ahora"]].copy()

        print(f"[INFO] Detalles publicables ahora: {len(df_a_insertar)}")

        print("\n[INFO] Transformando datos a formato TRANSF_CONNEXA_IN...")
        df_stg = transformar_a_staging(df_a_insertar)
        print(f"[INFO] Registros válidos a insertar en staging: {len(df_stg)}")

        if not df_stg.empty:
            print("\n[INFO] Insertando en SQL Server [data-sync].[repl].[TRANSF_CONNEXA_IN]...")
            n = insertar_en_staging_sqlserver(df_stg, sql_engine)
            print(f"[INFO] Filas insertadas en TRANSF_CONNEXA_IN: {n}")
        else:
            n = 0
            print("[INFO] No hay filas nuevas para insertar en staging.")

        header_uuids_actualizables = obtener_headers_completamente_publicables(df_asignado)
        print(f"[INFO] Cabeceras completamente publicables / publicadas: {len(header_uuids_actualizables)}")

        if header_uuids_actualizables:
            print("\n[INFO] Actualizando estado de cabeceras a SINCRONIZANDO (80)...")
            filas_actualizadas = actualizar_estado_cabeceras(
                pg_engine,
                header_uuids_actualizables,
                nuevo_estado_id=80
            )
            print(f"[INFO] Cabeceras actualizadas: {filas_actualizadas}")
        else:
            print("[INFO] No hay cabeceras completas para actualizar a 80.")

        # Resumen
        total_origen = len(df_asignado)
        total_ya_publicado = int(df_asignado["ya_publicado"].sum()) if not df_asignado.empty else 0
        total_publicable_ahora = int(df_asignado["publicable_ahora"].sum()) if not df_asignado.empty else 0
        total_no_publicable = int((~df_asignado["publicable"] & ~df_asignado["ya_publicado"]).sum()) if not df_asignado.empty else 0

        print("\n[RESUMEN]")
        print(f"  - Detalles origen total              : {total_origen}")
        print(f"  - Detalles ya publicados            : {total_ya_publicado}")
        print(f"  - Detalles publicables ahora        : {total_publicable_ahora}")
        print(f"  - Detalles sin saldo suficiente     : {total_no_publicable}")
        print(f"  - Detalles insertados ahora         : {n}")
        print(f"  - Cabeceras actualizadas a estado 80: {len(header_uuids_actualizables)}")

        # Diagnóstico opcional
        if total_no_publicable > 0:
            print("\n[DETALLE NO PUBLICADOS - TOP 20]")
            cols = [
                "connexa_header_uuid",
                "connexa_detail_uuid",
                "origin_cd_num",
                "dest_store_num",
                "item_code_num",
                "qty_requested_num",
                "q_bultos_disponible",
                "saldo_antes",
                "saldo_despues",
                "motivo_no_publicado",
            ]
            df_no = df_asignado[
                (~df_asignado["ya_publicado"]) &
                (~df_asignado["publicable"])
            ][cols].head(20)

            for _, r in df_no.iterrows():
                print(
                    f"  header={r['connexa_header_uuid']} "
                    f"detail={r['connexa_detail_uuid']} "
                    f"orig={r['origin_cd_num']} dest={r['dest_store_num']} "
                    f"art={r['item_code_num']} req={r['qty_requested_num']} "
                    f"disp_ini={r['q_bultos_disponible']} saldo_antes={r['saldo_antes']} "
                    f"saldo_despues={r['saldo_despues']} motivo={r['motivo_no_publicado']}"
                )

        return 0

    except RuntimeError as e:
        print(f"\n[FATAL] Error de configuración de entorno: {e}")
        return 1
    except Exception as e:
        print(f"\n[FATAL] Ocurrió un error inesperado: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())