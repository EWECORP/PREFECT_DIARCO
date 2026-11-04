# Refresco programado de T055 para parámetros logísticos

El deployment existente `MASTER_SYNC_MAESTROS_POSTGRES_PROD` del pool
`dmz-diarco` se ejecuta todos los días a las 05:50 de Buenos Aires y toma su
código de `E:\ETL\ETL_DIARCO`. El proceso general
`scripts/send/actualizar_tablas_maestras.py` ahora incluye
`T055_ARTICULOS_CONDCOMPRA_COSTOS` inmediatamente después de T050.

La carga de T055 usa `scripts/send/refresh_t055_condcompra.py`. Lee la tabla
`repl.T055_ARTICULOS_CONDCOMPRA_COSTOS` de SQL Server con la conexión ya
configurada para el ETL, la copia por lotes a una tabla temporal en
PostgreSQL y comprueba el volumen. Reemplaza
`src.t055_articulos_condcompra_costos` dentro de una sola transacción. Si
falla la extracción, el volumen o la inserción, la tabla anterior permanece
disponible. El umbral impide sustituir 1,8 millones de filas por menos de
1 millón o por menos del 80 % del volumen anterior.
La marca `fecha_extraccion` del lote se guarda como hora local de Buenos Aires,
igual que los demás cortes SGM, para que la comprobación de frescura no dependa
de la zona horaria del servidor PostgreSQL.

No usa CDC. El piloto CDC de T055 tenía última corrida del 28/08/2026; su
configuración `enabled=true` no demostraba que estuviera en ejecución. El
04/11/2026 se instaló este refresco en el worker y se ejecutó manualmente:
`src` pasó de 1.818.954 a 3.298.370 filas, todas con fecha de extracción
`2026-11-04 17:02:38` y ninguna sin fecha. Queda verificar la siguiente corrida
programada de las 05:50.

Para publicar el cambio en el worker hay que copiar **ambos** archivos
`scripts/send/actualizar_tablas_maestras.py` y
`scripts/send/refresh_t055_condcompra.py` a sus rutas homónimas bajo
`E:\ETL\ETL_DIARCO`. Hacer antes un respaldo del archivo existente. No hace
falta crear otro deployment: el existente apunta al archivo modificado. La
primera ejecución debe verificarse por el estado `Completed` del flow y por
estas consultas de sólo lectura en `diarco_data`:

```sql
SELECT count(*) AS filas, max(fecha_extraccion) AS ultima_extraccion
FROM src.t055_articulos_condcompra_costos;

SELECT fuente_origen, count(*)
FROM src.t055_articulos_condcompra_costos
GROUP BY fuente_origen;
```

El flow de Connexa a las 07:30 exige que la extracción de T055 tenga menos
de 48 horas antes de habilitar `CNX_SYNC_LOGISTICS_ENABLED=true`. Verificar
que el refresco de las 05:50 termine antes del flow de Connexa.
