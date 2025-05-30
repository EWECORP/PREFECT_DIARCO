import pyodbc
import csv
import zipfile
import os
import paramiko
import sys

# Datos de conexión MSSQL
db_config = {
    'server': '10.54.200.92',
    'database': 'data-sync',
    'username': 'data-sync',
    'password': 'aladelta10$',
    'timeout': '3000'
}

# Datos SFTP
sftp_config = {
    'host': '140.99.164.229',
    'port': 22,
    'username': 'usr_diarco',
    'password': 'diarco2024',
    'remote_path': '/archivos/usr_diarco/data-trx',
}

# Carpeta de salida
output_dir = "./output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def export_table_to_csv(table_name, filtro):
    """Exporta la tabla MSSQL especificada a un archivo CSV."""
    csv_file = os.path.join(output_dir, f"{table_name}.csv")
    conn = None
    try:
        # Conexión a MSSQL
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']};"
            f"Connection Timeout={db_config['timeout']}"
        )
        cursor = conn.cursor()

        # Consulta
        query = f"SELECT * FROM [dbo].[{table_name}] WHERE C_ANIO = {filtro}"
        cursor.execute(query)

        # Exportar a CSV en modo streaming
        with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)

            batch_size = 10000  # cantidad de filas a traer por bloque
            total_rows = 0

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    writer.writerow(['NULL' if v is None else v for v in row])
                total_rows += len(rows)
                if total_rows % 100000 == 0:
                    print(f"{total_rows} registros exportados...")

        print(f"Archivo CSV generado: {csv_file} ({total_rows} registros)")
        return csv_file
    except Exception as e:
        print(f"Error al exportar la tabla {table_name}: {e}")
        return None
    finally:
        if conn:
            conn.close()

def zip_csv(csv_file):
    """Comprime el archivo CSV en un ZIP."""
    zip_file = os.path.join(output_dir, f"{os.path.basename(csv_file)}.zip")
    try:
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_file, os.path.basename(csv_file))
        print(f"Archivo ZIP generado: {zip_file}")
        return zip_file
    except Exception as e:
        print(f"Error al comprimir el archivo {csv_file}: {e}")
        return None

def upload_to_sftp(zip_file):
    """Sube el archivo ZIP al servidor SFTP."""
    if not os.path.exists(zip_file):
        print(f"Archivo ZIP no encontrado: {zip_file}")
        return

    try:
        transport = paramiko.Transport((sftp_config['host'], sftp_config['port']))
        transport.connect(username=sftp_config['username'], password=sftp_config['password'])
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Cambiar a la carpeta remota explícitamente
        try:
            sftp.chdir(sftp_config['remote_path'])
        except IOError:
            print(f"La carpeta remota no existe: {sftp_config['remote_path']}")
            # Crear carpeta si no existe
            path_parts = sftp_config['remote_path'].strip("/").split("/")
            current_path = ""
            for part in path_parts:
                current_path = f"{current_path}/{part}".strip("/")
                try:
                    sftp.chdir(current_path)
                except IOError:
                    sftp.mkdir(current_path)
                    sftp.chdir(current_path)
            print(f"Carpeta remota creada: {sftp_config['remote_path']}")

        # Ruta remota
        remote_file_path = f"{sftp_config['remote_path']}/{os.path.basename(zip_file)}"
        print(f"Subiendo archivo a SFTP: {remote_file_path}")

        # Subir archivo
        sftp.put(zip_file, remote_file_path)
        print(f"Archivo subido correctamente a SFTP: {remote_file_path}")
        
        # Cerrar conexión
        sftp.close()
        transport.close()
    except Exception as e:
        print(f"Error al subir el archivo al SFTP: {e}")

def main():
    if len(sys.argv) < 2:
        print("Por favor, especifica el nombre de la tabla como argumento.")
        sys.exit(1)

    table_name = sys.argv[1]
    print(f"Procesando tabla: {table_name}")

    filtro_aplicado = sys.argv[2]
    print(f"Procesando tabla: {table_name}")

    # Exportar datos a CSV
    csv_file = export_table_to_csv(table_name, filtro_aplicado)
    if not csv_file:
        sys.exit(1)

    # Comprimir el archivo CSV
    zip_file = zip_csv(csv_file)
    if not zip_file:
        sys.exit(1)

    # Subir el archivo ZIP al servidor SFTP
    upload_to_sftp(zip_file)

    # Limpiar archivos locales
    if os.path.exists(csv_file):
        os.remove(csv_file)
    if os.path.exists(zip_file):
        os.remove(zip_file)

    print("Proceso completado.")

if __name__ == "__main__":
    main()
