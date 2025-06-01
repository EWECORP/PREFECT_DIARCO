# utils/sftp.py
import os
import paramiko

def enviar_archivo_sftp(archivo_local, destino, config):
    from paramiko import SFTPClient, Transport
    import os

    transport: Transport = paramiko.Transport((config['host'], config['port']))
    transport.connect(username=config['username'], password=config['password'])

    sftp: SFTPClient = paramiko.SFTPClient.from_transport(transport)  # type: ignore # 👈 anotación explícita

    try:
        #remote_path = os.path.join(config['remote_path'], os.path.basename(archivo_local))
        remote_path = f"{destino.rstrip('/')}/{os.path.basename(archivo_local)}"

        try:
            sftp.stat(remote_path)
            print("✅ Ruta remota existe.")
        except FileNotFoundError:
            print(f"❌ Ruta remota NO existe.{remote_path}")
            
        sftp.put(archivo_local, remote_path)
        print(f"✅ Archivo enviado a {remote_path}")
    finally:
        sftp.close()
        transport.close()


def descargar_archivo_sftp(nombre_archivo_zip, destino_local, config):
    from paramiko import SFTPClient, Transport
    import os
    transport: Transport = paramiko.Transport((config['host'], config['port']))
    transport.connect(username=config['username'], password=config['password'])

    sftp: SFTPClient = paramiko.SFTPClient.from_transport(transport) # type: ignore

    try:
        remote_file = os.path.join(config['remote_path'], nombre_archivo_zip)
        local_file = os.path.join(destino_local, nombre_archivo_zip)
        sftp.get(remote_file, local_file)
        print(f"✅ Archivo descargado desde {remote_file} a {local_file}")
        return local_file
    finally:
        sftp.close()
        transport.close()

