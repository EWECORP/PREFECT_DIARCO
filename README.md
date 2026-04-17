# Proyecto ETL_DIARCO

Este repositorio contiene el proyecto ETL_DIARCO, un conjunto de procesos de orquestación, extracción, transformación y carga de datos que conectan sistemas legados de DIARCO con PostgreSQL y la plataforma CONNEXA.

El proyecto está basado en Prefect 3.4.x y está diseñado principalmente para trabajar en un entorno Windows/DMZ con:
- SQL Server como origen de datos legacy DIARCO
- PostgreSQL como destino centralizado (`diarco_data`)
- SFTP para intercambio de archivos
- Prefect Cloud/Prefect Server para orquestación y deployments

---

## 🚧 Propósito

El objetivo principal es automatizar la sincronización y replicación de datos entre sistemas operativos y analíticos:
- publicar órdenes de compra en estado `90`
- cargar datos de proveedores, artículos y ventas
- actualizar tablas maestras y tablas tabulares en PostgreSQL
- soportar procesos de `pull`, `push`, `send` y `repl`

---

## 📁 Estructura del proyecto

```
ETL_DIARCO/
├── .env                      # Variables de entorno para conexiones y rutas locales
├── config.yaml               # Configuración base de PostgreSQL para Prefect
├── prefect.yaml              # Archivo de configuración / despliegue Prefect legacy
├── prefect_root_ETL_DIARCO.yaml  # Definición principal de deployments Prefect
├── requirements.txt          # Dependencias Python
├── install/                  # Scripts de instalación, despliegue y arranque
├── jobs/                     # Scripts y deployments de Prefect
├── scripts/                  # Flujos ETL divididos por tipo: pull/push/repl/send
├── logs/                     # Archivos de log generados por procesos ETL
├── state/                    # Scripts de control y monitoreo del estado de Prefect
├── tmp/                      # Archivos temporales y despliegues provisionales
├── utils/                     # Utilidades de conexión, logging y diagnosis
└── README.md                 # Documentación del proyecto
```

### Carpetas clave

- `scripts/pull/`: procesos que extraen datos de DIARCO/SQL Server y los publican hacia destino.
- `scripts/push/`: procesos que empujan datos hacia PostgreSQL o CONNEXA.
- `scripts/send/`: procesos de exportación e intercambio SFTP.
- `scripts/repl/`: replicación parametrizada y carga de staging.
- `scripts/transforms/`: transformaciones intermedias y preparación de datos.
- `jobs/`: operaciones de Prefect en lote y definiciones de despliegue.
- `install/`: utilidades para crear colas, aplicar deployments y arrancar workers.
- `utils/`: helpers para conexión a SQL Server, PostgreSQL, SFTP, logging y diagnósticos.

---

## 🧠 Arquitectura general

1. Prefect orquesta los flujos de ETL.
2. Las conexiones se configuran principalmente desde `.env` y `config.yaml`.
3. Los scripts ejecutan consultas contra SQL Server, transforman con pandas y escriben en PostgreSQL.
4. Algunos procesos generan archivos CSV/ZIP y los transfieren vía SFTP.
5. El `install/apply_all_deployments.bat` aplica deployments que están definidos en `jobs/`.

---

## 🔌 Conexiones y entornos

### Orígenes de datos SQL Server

El proyecto usa múltiples entornos SQL Server con conexión ODBC:
- `SQL_DRIVER`, `SQL_SERVER`, `SQL_DATABASE`, `SQL_USER`, `SQL_PASSWORD`
- `SQLE_DRIVER`, `SQLE_SERVER`, `SQLE_DATABASE`, `SQLE_USER`, `SQLE_PASSWORD`
- `SQLT_DRIVER`, `SQLT_SERVER`, `SQLT_DATABASE`, `SQLT_USER`, `SQLT_PASSWORD`
- `SQLP_DRIVER`, `SQLP_SERVER`, `SQLP_DATABASE`, `SQLP_USER`, `SQLP_PASSWORD`
- `SGMT_DRIVER`, `SGMT_SERVER`, `SGMT_DATABASE`, `SGMT_USER`, `SGMT_PASSWORD`

### Destino PostgreSQL

Las variables definidas en `.env` incluyen:
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`
- `PGP_HOST`, `PGP_PORT`, `PGP_DB`, `PGP_USER`, `PGP_PASSWORD`

### SFTP

Configuración de intercambio de archivos:
- `SFTP_HOST`
- `SFTP_PORT`
- `SFTP_USER`
- `SFTP_PASSWORD`
- `SFTP_REMOTE_PATH`

### Prefect

El proyecto define:
- `PREFECT_API_URL` para conectar con el orquestador Prefect
- `PREFECT_API_KEY` para autenticación
- `work_pool` y `work_queue` para ejecutar workers DMZ

---

## 📌 Tecnologías y dependencias

- Python
- Prefect 3.4.x
- pandas
- pyodbc
- sqlalchemy
- psycopg2 / psycopg2-binary
- python-dotenv
- paramiko (para SFTP)
- OpenAI (posible uso en automatizaciones adicionales)

---

## ⚙️ Configuración de entorno

El archivo `.env` contiene las variables de entorno principales y debe mantenerse fuera del control de versiones.

Ejemplo de valores que se configuran en `.env`:
- conexiones a SQL Server
- conexiones a PostgreSQL
- parámetros de SFTP
- rutas locales (`BASE_DIR`, `FOLDER_DATOS`, `FOLDER_TMP`, `FOLDER_BKP`, `FOLDER_LOG`)
- parámetros de performance (`COPY_CHUNK_SIZE`, `TRUNCATE_MAX_RETRIES`, `USE_STAGING`)
- Prefect API

---

## 🚀 Cómo ejecutar

### 1. Instalar dependencias
```powershell
cd E:\ETL\ETL_DIARCO
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configurar variables
- Crear o actualizar `.env` con las credenciales correctas.
- Validar `BASE_DIR` y las rutas de `FOLDER_DATOS`, `FOLDER_TMP`, `FOLDER_LOG`.
- Confirmar los endpoints de Prefect en `PREFECT_API_URL`.

### 3. Ejecutar un flujo manual
```powershell
install\run_flow_manual.bat
```

### 4. Aplicar deployments Prefect
```powershell
install\apply_all_deployments.bat
```

### 5. Arrancar worker local/DMZ
```powershell
install\install_worker_service.bat
```

---

## 💡 Notas importantes

- No dejes credenciales reales en el repositorio.
- `ETL_ENV_PATH` se usa en scripts para definir la ruta del `.env`.
- Muchos scripts leen `.env` dinámicamente y esperan que el archivo exista.
- El proyecto soporta tanto ejecución local como despliegue a Prefect Cloud/Servidor remoto.

---

## 🧪 Diagnóstico y soporte

Hay herramientas de diagnóstico incluidas en `utils/`:
- `utils/healthcheck_project.py`
- `utils/test_sqlserver_connection.py`
- `utils/test_sqlserver_connection_with_diagnosis.py`
- `utils/diagnostico_utf8.py`
- `utils/sftp.py`
- `utils/logger_prefect_sql.py`

Estas utilidades permiten validar conexiones y detectar problemas de encoding o conectividad antes de ejecutar la orquestación completa.

---

## 📚 Referencias rápidas

- `prefect_root_ETL_DIARCO.yaml`: despliegue principal de Prefect
- `install/apply_all_deployments.bat`: aplica deployments batch
- `scripts/pull/S90_PUBLICAR_OC_PRECARGA.py`: flujo de publicación de órdenes de compra
- `scripts/send/refresh_tablas_maestras.py`: ejemplo de sincronización de tablas maestras
- `.env`: configuración de conexión y parámetros operativos
- `requirements.txt`: dependencias Python

