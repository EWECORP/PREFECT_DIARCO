
import os
import socket
from sqlalchemy import create_engine
from dotenv import load_dotenv

def diagnosticar_error_conexion(error_message, server, puerto=1433):
    print("\n🩺 Diagnóstico automático iniciado...")
    if "server is not found" in error_message.lower() or "could not open a connection" in error_message.lower():
        print("🔴 Parece un problema de red o nombre del servidor.")
        try:
            ip = socket.gethostbyname(server.split('\\')[0])
            print(f"✅ Resolución DNS OK. {server} -> {ip}")
        except socket.gaierror:
            print(f"❌ No se pudo resolver el nombre {server}. Verifique el nombre del servidor o archivo hosts.")
    if "login timeout expired" in error_message.lower():
        print("🟡 Tiempo de espera agotado. Puede ser problema de Firewall o puerto bloqueado.")
    if "tcp provider" in error_message.lower():
        print("🟠 Puede ser que el protocolo TCP/IP no esté habilitado en SQL Server.")
    print("🔎 Recomendaciones: Verificar conectividad de red, reglas de firewall y configuración del servidor SQL.")

def test_sqlserver_connection_with_diagnosis():
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
        error_message = str(e)
        print(f"❌ Error de conexión: {error_message}")
        diagnosticar_error_conexion(error_message, server)

if __name__ == "__main__":
    test_sqlserver_connection_with_diagnosis()
