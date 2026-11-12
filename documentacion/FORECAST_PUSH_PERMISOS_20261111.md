# Diagnostico FORECAST_PUSH_INPUT_DATA_PROD - 2026-11-11

## Resultado

Mantener PG_USER=connexa_user. El fallo observado requiere corregir la
propiedad de src.base_productos_vigentes; no requiere volver a postgres,
otorgar superusuario ni ampliar los permisos DML del usuario.

Se consultaron el historial/logs de Prefect y los catalogos PostgreSQL mediante
una conexion de solo lectura. No se ejecutaron cargas, DDL ni cambios de permisos
en produccion. No se modifico .env ni el codigo de los flujos.

## Evidencia de la ultima ejecucion efectiva

- Deployment: FORECAST_PUSH_INPUT_DATA_PROD.
- Run: immortal-cuckoo, 01a1b044-04d4-739e-a171-255ba8e95f82.
- Enlace: https://orquestador.connexa-cloud.com/runs/flow-run/01a1b044-04d4-739e-a171-255ba8e95f82
- Inicio: 11/11/2026 06:50:03, fin fallido: 07:13:03, hora Argentina (UTC-3).
- La ejecucion del 10/11 termino Completed.
- Primer intento: carga de src.base_stock_sucursal completada con 1.339.542 filas
  a las 06:55:36; a las 06:55:37 falla Asegurar Indices Base Stock:
  `psycopg2.errors.InsufficientPrivilege: must be owner of table base_productos_vigentes`.
- Se repitio la carga completa otras dos veces; mismo error en ambos reintentos.
- El flujo se detuvo en el script 1/6, obtener_base_stock.py. Los otros cinco
  scripts no se iniciaron. La carga de stock se confirma antes de crear los
  indices: el flujo fallido dejo una actualizacion parcial, sin el ajuste de
  transferencias por CD ni los acumulados de ventas de sus pasos posteriores.

Fuente del fallo: scripts/push/obtener_base_stock.py, funcion
asegurar_indices_base_stock, que crea indices sobre base_stock_sucursal y
base_productos_vigentes en una misma transaccion. Al fallar la segunda tabla,
los indices de esa transaccion se revierten. No habia indices en ninguna de
estas dos tablas al consultar los catalogos.

## Estado de permisos verificado

Servidor PostgreSQL 14.24, base diarco_data, usuario efectivo connexa_user.
El usuario no es superusuario. Hereda connexa_runtime y connexa_owner;
src y datamart pertenecen a connexa_owner. Tiene USAGE y CREATE sobre ambos
esquemas y permiso TEMP sobre la base.

| Objeto | Propietario | Propiedad efectiva para connexa_user |
| --- | --- | --- |
| src.base_stock_sucursal | connexa_user | Si |
| src.base_productos_vigentes | postgres | No |
| src.base_forecast_oc_demoradas | postgres | No |
| src.base_transferencias_pendientes | postgres | No |
| src.base_productos_en_transito | postgres | No |
| src.base_ventas_extendida | connexa_owner | Si |
| datamart.dm_ventas_diarias | connexa_owner | Si |

En las siete tablas se verificaron SELECT, INSERT, UPDATE, DELETE y TRUNCATE
efectivos para connexa_user. Un GRANT ALL sobre la tabla no sustituye la
propiedad requerida para crear indices. Tampoco alcanza cambiar al rol
connexa_owner mientras la tabla siga perteneciendo a postgres.

Precision: DROP TABLE admite al propietario del esquema. Como connexa_user
hereda connexa_owner, las otras tres tablas de postgres no constituyen por si
solas el mismo bloqueo para el DROP del cargador. El error real documentado
es CREATE INDEX sobre base_productos_vigentes, no un DROP fallido.

## Correccion propuesta

Ejecutar como postgres o administrador autorizado en diarco_data:

```sql
ALTER TABLE src.base_productos_vigentes OWNER TO connexa_owner;
```

El archivo scripts/sql/FORECAST_PUSH_FIX_OWNER_20261111.sql incluye esta
correccion en una transaccion con limites de espera y consulta de verificacion.
No es necesario cambiar la propiedad de toda la base ni usar REASSIGN OWNED.
Si se desea normalizar las otras tablas generadas por el flujo, puede hacerse
por objeto, pero no es requisito para corregir el error observado.

Despues de aplicar: verificar propiedad efectiva conectado como connexa_user y
ejecutar controladamente el deployment. Comprobar los seis scripts y estado
Completed. Esa ejecucion completa no se realizo durante este diagnostico;
no se garantiza la ausencia de otros errores en pasos no ejecutados hoy.

## Evitar recurrencia y conservar accesos

El cargador comun etl_chunk_utils.replace_table_from_query_chunks y las variantes
de productos usan DROP TABLE ... CASCADE seguido de CREATE TABLE. Al recrear,
el propietario pasa a ser el rol creador (hoy connexa_user) y desaparecen los
grants especificos e indices del objeto anterior.

Se confirmo un efecto adicional: nexa_report_reader mantiene SELECT sobre las
otras cuatro tablas pero ya no sobre base_stock_sucursal. Los privilegios por
defecto de connexa_user y connexa_owner en src otorgan DML a connexa_runtime,
pero no SELECT a nexa_report_reader; el default de postgres si lo otorgaba.
El SQL propuesto incluye un bloque opcional comentado para restaurar SELECT
sobre stock y configurar defaults de lectura para ambos posibles creadores.
Los defaults son por rol creador y no se heredan de otros roles al crear objetos.
Revisar ese alcance: afectarian todas las futuras tablas de src de esos roles.

Como mejora de codigo posterior, reemplazar DROP/CREATE por una carga que
preserve el objeto, por ejemplo staging y recarga transaccional compatible con
sus dependencias. Si se mantiene DDL, usar un rol propietario coherente y
restaurar explicitamente los grants e indices de cada tabla. Configurar el
propietario una sola vez no conserva connexa_owner tras el proximo DROP/CREATE
ejecutado como connexa_user, aunque el usuario seguira siendo propietario efectivo.

No se encontraron vistas dependientes de las cinco tablas en la consulta de
pg_depend/pg_rewrite. Esto no reemplaza revisar otras clases de dependencias
antes de redisenar las cargas.

El worker productivo corre en E:\ETL\ETL_DIARCO. El launcher carga .env al
iniciar y los scripts usan load_dotenv() sin override: si en el futuro se cambia
el usuario en el .env productivo, reiniciar el worker para renovar las variables
heredadas. El estado de propietarios y el error actual son consistentes con la
transicion de usuario, aunque el log no imprime current_user de la conexion.

## Referencias

- Privilegios y derechos inherentes al propietario:
  https://www.postgresql.org/docs/14/ddl-priv.html
- DROP TABLE y propietario de esquema:
  https://www.postgresql.org/docs/14/sql-droptable.html
- Privilegios por defecto por rol creador:
  https://www.postgresql.org/docs/14/sql-alterdefaultprivileges.html
