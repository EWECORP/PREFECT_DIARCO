#flujo_base_stock.py REEMPLASA SP_BASE_STOCK
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
import psycopg2 as pg2
from psycopg2.extras import execute_values

load_dotenv()

# === CONEXIONES ===
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

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

# === TAREAS ===
@task
def extraer_tabla(nombre_tabla):
    query = f"SELECT * FROM {nombre_tabla}"
    return pd.read_sql(query, sql_engine)

@task
def calcular_pedidos_pendientes(df_cabe, df_deta):
    df = df_deta.merge(
        df_cabe,
        on=["C_OC", "U_PREFIJO_OC", "U_SUFIJO_OC"],
        suffixes=("_d", "_c")
    )
    df = df[df["C_SITUAC"] == 1]
    df = df[df["C_SUCU_COMPRA"] != 300]
    df["Pendientes"] = (df["Q_BULTOS_PROV_PED"] * df["Q_FACTOR_PROV_PED"] - df["Q_UNID_CUMPLIDAS"]).clip(lower=0)
    return df.groupby(["C_SUCU_COMPRA", "C_ARTICULO"])["Pendientes"].sum().reset_index()

@task
def calcular_promociones(df1, df2, fecha):
    df1 = df1[(df1["F_DESDE"] <= fecha) & (df1["F_HASTA"] >= fecha) & (df1["Q_UNIDADES_KILOS_SALDO"] > 0)]
    df2 = df2[(df2["F_DESDE"] <= fecha) & (df2["F_HASTA"] >= fecha) & (df2["Q_UNIDADES_KILOS_SALDO"] > 0)]
    df = pd.concat([df1, df2])
    df["flag"] = 1
    return df[["C_SUCU_EMPR", "C_ARTICULO", "flag"]]

@task
def transformar_y_unir(stock, articulos, articulos_sucursal, costos, reposicion, sucursales, pedidos, promociones, fecha):
    df = stock.merge(articulos, on="C_ARTICULO")
    df = df.merge(articulos_sucursal, on=["C_ARTICULO", "C_SUCU_EMPR"], how="left")
    df = df.merge(costos, on=["C_ARTICULO", "C_SUCU_EMPR"], how="left")
    df = df.merge(reposicion, on=["C_ARTICULO", "C_SUCU_EMPR"], how="left")
    df = df.merge(sucursales, on="C_SUCU_EMPR")
    df = df.merge(pedidos, left_on=["C_ARTICULO", "C_SUCU_EMPR"], right_on=["C_ARTICULO", "C_SUCU_COMPRA"], how="left")
    df = df.merge(promociones, left_on=["C_ARTICULO", "C_SUCU_EMPR"], right_on=["C_ARTICULO", "C_SUCU_EMPR"], how="left")

    df = df[df["C_PROVEEDOR_PRIMARIO"] == df["C_PROVEEDOR"]]
    df = df[df["M_SUCU_VIRTUAL"] == "N"]
    df = df[df["M_HABILITADO_SUCU"] == "S"]

    df_final = pd.DataFrame({
        "codigo_articulo": df["C_ARTICULO"].astype(int),
        "codigo_sucursal": df["C_SUCU_EMPR"].astype(int),
        "codigo_proveedor": df["C_PROVEEDOR_PRIMARIO"].astype(int),
        "precio_venta": df["I_PRECIO_VTA"],
        "precio_costo": df["I_COSTO_ESTADISTICO"],
        "factor_venta": df["Q_FACTOR_VTA_SUCU"],
        "m_vende_por_peso": df["M_VENDE_POR_PESO"],
        "venta_unidades_1q": df["Q_VENTA_15_DIAS"] * df["Q_FACTOR_VTA_SUCU"],
        "venta_unidades_2q": df["Q_VENTA_30_DIAS"] * df["Q_FACTOR_VTA_SUCU"],
        "venta_mes_unidades": (df["Q_VENTA_15_DIAS"] + df["Q_VENTA_30_DIAS"]) * df["Q_FACTOR_VTA_SUCU"],
        "venta_mes_valorizada": (df["Q_VENTA_15_DIAS"] + df["Q_VENTA_30_DIAS"]) * df["Q_FACTOR_VTA_SUCU"] * df["I_COSTO_ESTADISTICO"],
        "dias_stock": np.where(
            (df["Q_VENTA_15_DIAS"] + df["Q_VENTA_30_DIAS"]) * df["Q_FACTOR_VTA_SUCU"] * df["I_COSTO_ESTADISTICO"] == 0,
            np.nan,
            ((df["Q_UNID_ARTICULO"].fillna(0) + df["Q_PESO_ARTICULO"].fillna(0)) * df["I_COSTO_ESTADISTICO"]) /
            ((df["Q_VENTA_15_DIAS"] + df["Q_VENTA_30_DIAS"]) * df["Q_FACTOR_VTA_SUCU"] * df["I_COSTO_ESTADISTICO"]) * 30
        ).round(0),
        "fecha_stock": fecha,
        "stock": np.where(df["M_VENDE_POR_PESO"] == "N", df["Q_UNID_ARTICULO"], df["Q_PESO_ARTICULO"]),
        "transfer_pendiente": df["Q_TRANSF_PEND"] * df["Q_FACTOR_VTA_SUCU"],
        "pedido_pendiente": df["Pendientes"].fillna(0),
        "promocion": np.where(df["M_OFERTA_SUCU"] == "S", 1, 0),
        "lote": "",
        "validez_lote": pd.to_datetime("2099-12-31"),
        "stock_reserva": 0,
        "validez_promocion": df["flag"].fillna(0).astype(int),
        "q_dias_stock": df["Q_DIAS_STOCK"],
        "q_dias_sobre_stock": df["Q_DIAS_SOBRE_STOCK"],
        "i_lista_calculado": df["I_LISTA_CALCULADO"],
        "pedido_sgm": df["Q_REPONER_INCLUIDO_SOBRE_STOCK"] * df["Q_FACTOR_VTA_SUCU"],
        "fuente_origen": "SP_BASE_STOCK",
        "fecha_extraccion": datetime.now(),
        "estado_sincronizacion": 0
    })

    return df_final

@task
def cargar_postgres(df: pd.DataFrame, tabla_destino: str, batch_size: int = 50000):
    df = df.astype(object).where(pd.notnull(df), None)

    columnas_sql = {
        "codigo_articulo": "INTEGER",
        "codigo_sucursal": "INTEGER",
        "codigo_proveedor": "INTEGER",
        "precio_venta": "NUMERIC",
        "precio_costo": "NUMERIC",
        "factor_venta": "INTEGER",
        "m_vende_por_peso": "VARCHAR",
        "venta_unidades_1q": "NUMERIC",
        "venta_unidades_2q": "NUMERIC",
        "venta_mes_unidades": "NUMERIC",
        "venta_mes_valorizada": "NUMERIC",
        "dias_stock": "NUMERIC",
        "fecha_stock": "TIMESTAMP",
        "stock": "NUMERIC",
        "transfer_pendiente": "NUMERIC",
        "pedido_pendiente": "NUMERIC",
        "promocion": "INTEGER",
        "lote": "VARCHAR",
        "validez_lote": "TIMESTAMP",
        "stock_reserva": "NUMERIC",
        "validez_promocion": "INTEGER",
        "q_dias_stock": "INTEGER",
        "q_dias_sobre_stock": "INTEGER",
        "i_lista_calculado": "NUMERIC",
        "pedido_sgm": "NUMERIC",
        "fuente_origen": "VARCHAR",
        "fecha_extraccion": "TIMESTAMP",
        "estado_sincronizacion": "INTEGER"
    }

    columnas = ', '.join([f'"{col}" {tipo}' for col, tipo in columnas_sql.items()])
    insert_columns = ', '.join([f'"{col}"' for col in columnas_sql.keys()])

    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {tabla_destino} CASCADE')
            cur.execute(f'CREATE TABLE {tabla_destino} ({columnas})')

            values = [tuple(row) for row in df.itertuples(index=False, name=None)]
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                execute_values(cur, f'INSERT INTO {tabla_destino} ({insert_columns}) VALUES %s', batch)
        conn.commit()

# === FLOW ===
@flow(name="flujo_base_stock")
def flujo_base_stock():
    fecha = pd.to_datetime(datetime.today().date())
    log = get_run_logger()

    stock = extraer_tabla("T060_STOCK")
    articulos = extraer_tabla("T050_ARTICULOS")
    articulos_sucursal = extraer_tabla("T051_ARTICULOS_SUCURSAL")
    costos = extraer_tabla("T055_ARTICULOS_CONDCOMPRA_COSTOS")
    reposicion = extraer_tabla("T710_ESTADIS_REPOSICION")
    sucursales = extraer_tabla("T100_EMPRESA_SUC")
    oc_cabe = extraer_tabla("T080_OC_CABE")
    oc_deta = extraer_tabla("T081_OC_DETA")
    promo1 = extraer_tabla("T230_facturador_negocios_especiales_por_cantidad")
    promo2 = extraer_tabla("[DIARCO-BARRIO].DBO.T230_facturador_negocios_especiales_por_cantidad")

    pedidos = calcular_pedidos_pendientes(oc_cabe, oc_deta)
    promociones = calcular_promociones(promo1, promo2, fecha)

    df_resultado = transformar_y_unir(
        stock, articulos, articulos_sucursal, costos,
        reposicion, sucursales, pedidos, promociones, fecha
    )

    cargar_postgres(df_resultado, "stg.base_stock_sucursal")

    log.info("Proceso completado.")

if __name__ == "__main__":
    flujo_base_stock()
# This script is designed to extract, transform, and load (ETL) stock data from a SQL Server database into a PostgreSQL database.
# It includes tasks for extracting data from various tables, calculating pending orders and promotions, transforming the data, and loading it into PostgreSQL.
# The flow is orchestrated using Prefect, allowing for easy scheduling and monitoring of the ETL process.