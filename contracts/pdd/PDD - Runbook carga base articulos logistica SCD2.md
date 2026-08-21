# PDD - Runbook de carga canónica SCD2 de artículos logística

El flujo `scripts/pdd/cargar_base_articulos_logistica.py` mantiene
`diarco_data.src.base_articulos_logistica`. No ejecuta el DDL, no recrea la
tabla final y no publica snapshots hacia Stock Management.

## Entradas

El modo operativo recomendado es `source_mode=sqlserver_sp`. Reutiliza
`SQL_SERVER`, `SQL_DATABASE`, `SQL_USER` y `SQL_PASSWORD` del `.env` para
ejecutar `[dbo].[SP_BASE_ARTICULOS_LOGISTICA_DMZ]` en `data-sync`.

Para identificación GS1, el SP publica `T050_ARTICULOS.C_EAN` como
`c_gtin_unidad` únicamente cuando contiene 13 dígitos y `C_DUN14` como
`c_gtin_bulto` únicamente cuando contiene 14 dígitos. Además valida el dígito
GS1 Mod-10 y descarta cuerpos formados por un único dígito repetido. Otros
valores, incluidos los códigos de balanza de artículos pesables y placeholders
como `11111111111113` o `1111111111114`, quedan en `NULL`. No se consumen EAN
alternativos de `T085_ARTICULOS_EAN_EDI`.

El modo alternativo `source_mode=file` admite CSV UTF-8, JSON (array de
objetos) o JSON Lines. Los nombres de campo son los del DDL canónico.
`articulo_logistica_id`, las fechas de vigencia y los campos de auditoría de
PostgreSQL no se reciben: los administra el ETL. Puede utilizarse
`examples/pdd/base_articulos_logistica_modelo.csv` como modelo; todos sus
códigos y valores son ficticios.

Campos mínimos por fila:

- `c_articulo`;
- `c_unidad_base`;
- `m_vende_por_peso`.

`fuente_origen` puede venir por fila o completarse con `source_name`.
Los campos logísticos desconocidos se mantienen en `NULL`, nunca en cero.
El flujo deriva el volumen cuando recibe las tres dimensiones y deriva los
bultos por pallet cuando recibe capas y bultos por capa. Una contradicción se
rechaza, no se corrige silenciosamente.

## Primera carga

1. El responsable del DDL PostgreSQL aplica
   `scripts/sql/pdd/001_create_base_articulos_logistica.sql` en `diarco_data`.
2. Aplicar `scripts/sql/pdd/SP_BASE_ARTICULOS_LOGISTICA_DMZ.sql` en `data-sync`
   sobre el SQL Server DMZ. El SP conserva `Q_PESO_UNIT_ART` sólo como candidato
   de linaje y no lo publica como peso canónico.
3. Ejecutar manualmente el SP y verificar que devuelva una fila por clave
   `(c_articulo, c_proveedor, c_configuracion_logistica)`.
4. Crear/actualizar el deployment:

   ```powershell
   prefect deploy --name PDD_BASE_ARTICULOS_LOGISTICA_MANUAL
   ```

5. Ejecutarlo manualmente con `source_mode=sqlserver_sp`, `validate_only=true`
   y `full_snapshot=true`.
6. Revisar las métricas `new`, `changed`, `unchanged` y `closed_missing`.
7. Repetir con el mismo `effective_at` y `validate_only=false` para aplicar.
8. Validar que la vista actual tenga una sola configuración default por artículo:

   ```sql
   SELECT count(*) AS filas_actuales
   FROM src.v_base_articulos_logistica_actual;

   SELECT c_articulo, count(*)
   FROM src.v_base_articulos_logistica_actual
   GROUP BY c_articulo
   HAVING count(*) > 1;
   ```

9. Ejecutar nuevamente el mismo origen, primero con `validate_only=true`. Para
   confirmar idempotencia debe informar `new=0`, `changed=0`,
   `closed_missing=0` y `unchanged=source_rows`. Peso y volumen deben continuar
   clasificados como `MISSING` mientras no exista una fuente aprobada.

El deployment queda deliberadamente sin schedule. Se podrá programar sólo
después de aprobar esta primera carga y acordar la fuente logística recurrente.

## Actualizaciones

- `full_snapshot=false`: procesa un delta; no cierra claves ausentes.
- `full_snapshot=true`: la entrada representa el universo completo y cierra
  versiones vigentes ausentes. Un archivo vacío siempre se rechaza.
- `validate_only=true`: ejecuta precondiciones y preview dentro de una
  transacción que termina en rollback.
- Un checksum idéntico es idempotente y no crea una nueva versión.
- Un cambio contractual cierra la versión vigente e inserta una nueva usando
  el mismo `effective_at`, bajo un lock transaccional para impedir solapamientos.
