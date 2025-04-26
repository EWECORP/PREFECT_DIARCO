
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

def test_sqlserver_connection():
    load_dotenv()  # Cargar variables de entorno

    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")

    if not all([server, database, username, password]):
        print("❌ Error: Faltan datos de conexión en el archivo .env")
        return

    print(f"🔎 Intentando conectar a SQL Server en: {server} (Base: {database})...")

    try:
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            result = conn.execute("SELECT GETDATE()")
            fecha = result.scalar()
            print(f"✅ Conexión exitosa. Fecha/hora del servidor: {fecha}")
    except Exception as e:
        print(f"❌ Error de conexión: {e}")

if __name__ == "__main__":
    test_sqlserver_connection()
