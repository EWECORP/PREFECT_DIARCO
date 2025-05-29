# utils/sftp.py
import os
import paramiko
from prefect import get_run_logger


def enviar_archivo_sftp(archivo_local, destino, config):
    logger = get_run_logger()
    from paramiko import SFTPClient, Transport
    import os

    transport: Transport = paramiko.Transport((config['host'], config['port']))
    transport.connect(username=config['username'], password=config['password'])

    sftp: SFTPClient = paramiko.SFTPClient.from_transport(transport)  # type: ignore # 👈 anotación explícita

    try:
        remote_path = os.path.join(config['remote_path'], os.path.basename(archivo_local))
        sftp.put(archivo_local, remote_path)
        logger.info(f"✅ Archivo enviado a {remote_path}")
    finally:
        sftp.close()
        transport.close()


def descargar_archivo_sftp(nombre_archivo_zip, destino_local, config):
    from paramiko import SFTPClient, Transport
    import os
    logger = get_run_logger()
    transport: Transport = paramiko.Transport((config['host'], config['port']))
    transport.connect(username=config['username'], password=config['password'])

    sftp: SFTPClient = paramiko.SFTPClient.from_transport(transport) # type: ignore

    try:
        remote_file = os.path.join(config['remote_path'], nombre_archivo_zip)
        local_file = os.path.join(destino_local, nombre_archivo_zip)
        sftp.get(remote_file, local_file)
        logger.info(f"✅ Archivo descargado desde {remote_file} a {local_file}")
        return local_file
    finally:
        sftp.close()
        transport.close()

