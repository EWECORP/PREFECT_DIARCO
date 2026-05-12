# IOSdb

Modulo ETL orientado a sincronizar datos desde SQL Server en la DMZ de Diarco hacia la plataforma IOS.

## Objetivo

Separar la logica de negocio del modo de ejecucion local anterior y dejar el proyecto listo para:

- ejecutar en workers de Prefect
- publicar deployments desde `ETL_DIARCO/prefect.yaml`
- mantener configuracion por entorno
- mejorar trazabilidad de logs y fallos

## Estructura

- `config/settings.py`
  Configuracion tipada con compatibilidad hacia las claves actuales de `.env`.

- `clients/`
  Clientes reutilizables:
  - SQL Server
  - PostgreSQL IOS
  - API IOS

- `flows/`
  Flujos Prefect y logica cercana a la orquestacion.

- `flows/stock_shared.py`
  Core reutilizable para stock `mayorista`, `barrio` y `cadena`.

- `main.py`
  Entry points públicos de los flows para deployments.

## Compatibilidad

Se conserva la logica de negocio:

- queries SQL
- reglas de armado de payload
- comparacion de productos contra PostgreSQL IOS
- reintentos y persistencia de fallidos

## Variables de entorno

Claves actuales compatibles:

- SQL origen:
  - `DB_SERVER`
  - `DB_NAME`
  - `DB_USER`
  - `DB_PASSWORD`

- PostgreSQL IOS:
  - `PG_HOST`
  - `PG_PORT`
  - `PG_DB`
  - `PG_USER`
  - `PG_PASSWORD`

- API IOS:
  - `API_USERNAME`
  - `API_COMPANY`
  - `API_PASSWORD`

Claves nuevas opcionales:

- `IOSDB_SQL_SERVER`
- `IOSDB_SQL_DATABASE`
- `IOSDB_SQL_USER`
- `IOSDB_SQL_PASSWORD`
- `IOSDB_SQL_DRIVER`
- `IOSDB_PG_HOST`
- `IOSDB_PG_PORT`
- `IOSDB_PG_DB`
- `IOSDB_PG_USER`
- `IOSDB_PG_PASSWORD`
- `IOSDB_API_LOGIN_URL`
- `IOSDB_API_STOCK_URL`
- `IOSDB_API_CATEGORIES_URL`
- `IOSDB_API_PRODUCTS_URL`
- `IOSDB_API_USERNAME`
- `IOSDB_API_COMPANY`
- `IOSDB_API_PASSWORD`
- `IOSDB_BATCH_SIZE`
- `IOSDB_CATEGORY_BATCH_SIZE`
- `IOSDB_PRODUCT_BATCH_SIZE`
- `IOSDB_MAX_WORKERS`
- `IOSDB_MAX_RETRIES`
- `IOSDB_LOGS_DIR`
- `IOSDB_DISCORD_WEBHOOK`

## Flujos públicos

- `IOSdb/main.py:mayorista_flow`
- `IOSdb/main.py:barrio_flow`
- `IOSdb/main.py:cadena_flow`
- `IOSdb/main.py:products_flow`
- `IOSdb/main.py:categories_flow`
- `IOSdb/main.py:retry_flow`
- `IOSdb/main.py:initial_products_flow`
- `IOSdb/main.py:iosdb_master_flow`

## Notas operativas

- Los fallidos se escriben en `IOSDB_LOGS_DIR` o `IOSdb/logs`.
- `retry_flow` reprocesa esos archivos.
- `main.py` ya no usa `serve()`. La publicacion queda delegada a `prefect.yaml`.
