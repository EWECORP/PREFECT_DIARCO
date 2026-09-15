# Reconstrucción incremental del histórico de stock

Fecha: 2026-09-12. Fuente: `diarco_data.src.t710_estadis_stock`. Destino: `src.historico_stock_sucursal` en la misma base central.

El usuario autorizó reconstruir los faltantes, confirmó capacidad de disco suficiente y solicitó lotes por año, mes y sucursal con commits parciales. La operación conserva registros existentes y no actualiza cantidades históricas discrepantes.

## Relevamiento previo

- Histórico: 7.300.000 filas, 2025-04-01 a 2025-04-30, 588 MB, sin índices. No se encontraron claves diarias duplicadas ni fechas nulas/inconsistentes con año/mes/día.
- T710: clave única `(c_anio,c_mes,c_sucu_empr,c_articulo)`, meses disponibles desde julio de 2025 a agosto de 2026.
- Corte autorizado: 2026-08-28. No generar días posteriores de agosto, aunque existan columnas en la matriz.
- Rango solicitado al proceso: 2025-05-01 a 2026-08-28. Mayo y junio de 2025 no existen en esta fuente; quedan pendientes, no se inventan ceros.
- Plan: 665.529.991 filas diarias de origen, 424 fechas disponibles, 2.087 lotes año/mes/sucursal.

## Implementación

Script: [reconstruir_historico_stock_pg.py](../scripts/datamart/reconstruir_historico_stock_pg.py).

1. Planifica meses/sucursales existentes y registra fechas sin origen.
2. En modo aplicación crea concurrentemente un índice único por `(fecha_stock,articulo,sucursal)`, necesario para idempotencia y acceso por fecha. Si el índice existe pero es incompatible/inválido, se detiene sin eliminarlo.
3. Por lote bloquea otras escrituras del histórico durante la transacción, permitiendo lecturas; no bloquea toda la reconstrucción en una única transacción.
4. Expande Q_DIA1…Q_DIA31 sólo a fechas válidas y dentro del corte, en una tabla temporal que se elimina al commit.
5. Conserva ceros y negativos; rechaza cantidades nulas/no finitas y claves duplicadas.
6. Inserta con `ON CONFLICT DO NOTHING`, sin borrar ni sobrescribir. Marca las nuevas filas con fecha de proceso actual y `procesado=false`.
7. Verifica que cada fila del lote exista con igual cantidad y año/mes/día. Ante discrepancias, rollback del lote y detención; los lotes previos permanecen confirmados.
8. Guarda un reporte local después de cada commit. Un corte entre commit y reporte puede dejar una fila de avance ausente: reejecutar es seguro por la clave única, aunque vuelve a verificar los lotes previos.

Los cambios posteriores de T710 no corrigen automáticamente cantidades ya cargadas: deben tratarse como una reconciliación separada. La fuente se lee por lote; no se declara una fotografía global única de los 2.087 lotes.

## Ejecución

Desde `C:\PROYECTOS\ETL`, primero planificar:

```powershell
python ETL_DIARCO/scripts/datamart/reconstruir_historico_stock_pg.py --env FORECAST_CONNEXA/.env --prefix PG --from-date 2025-05-01 --through-date 2026-08-28 --report FORECAST_CONNEXA/data/stock_rebuild_20260912/plan.json
```

Aplicación autorizada, iniciada con:

```powershell
python ETL_DIARCO/scripts/datamart/reconstruir_historico_stock_pg.py --env FORECAST_CONNEXA/.env --prefix PG --from-date 2025-05-01 --through-date 2026-08-28 --report FORECAST_CONNEXA/data/stock_rebuild_20260912/apply.json --apply --capacity-verified
```

Para reanudar, usar un nombre de reporte nuevo; nunca sobrescribir evidencia. Puede limitarse el rango a meses pendientes, conservando los mismos controles. El flag de capacidad registra una comprobación operativa del usuario, no mide el espacio libre.

## Estado y cierre

### Resultado final de la carga priorizada de 2026

La ejecución `apply_2026.json` terminó con estado **COMPLETED** y el proceso finalizó con código 0. Se confirmaron **1.039 de 1.039 lotes**, con **339.607.673 filas insertadas**, exactamente las previstas, y **cero diferencias** en las verificaciones de cantidades y claves de cada lote contra T710. Rango reconstruido: **2026-01-01 a 2026-08-28**. Último lote: agosto de 2026, sucursal 412.

El reporte final se actualizó el 2026-09-12 a las 20:14:16 UTC (17:14:16 de Argentina). Esta conclusión se basa en los controles transaccionales por lote, el reporte final y la salida exitosa del proceso; no representa una nueva conciliación global posterior a posibles modificaciones de T710.

La carga de 2025 permanece parcial por decisión del usuario. No se reanudó después de completar 2026 y sus registros confirmados se conservan.

### Cambio de prioridad solicitado por el usuario

La carga original fue detenida a pedido del usuario para concentrarse en enero de 2026 en adelante. Se verificó el cierre de la sesión de carga en PostgreSQL. El reporte `apply.json` quedó marcado `INTERRUPTED_BY_USER`: registra 398 lotes y 129.231.115 filas confirmadas, hasta septiembre de 2025, sucursal 71. Los registros confirmados se conservan; no se eliminó lo recuperado de 2025.

Se inició una nueva ejecución con rango **2026-01-01 a 2026-08-28**, por año/mes/sucursal, y reporte independiente `FORECAST_CONNEXA/data/stock_rebuild_20260912/apply_2026.json`. La fuente verificada no contiene septiembre de 2026; «hasta hoy» se limita al último cierre disponible confirmado por el usuario. No hay dos cargas concurrentes.

```powershell
python ETL_DIARCO/scripts/datamart/reconstruir_historico_stock_pg.py --env FORECAST_CONNEXA/.env --prefix PG --from-date 2026-01-01 --through-date 2026-08-28 --report FORECAST_CONNEXA/data/stock_rebuild_20260912/apply_2026.json --apply --capacity-verified
```

El script identifica las nuevas sesiones como `stock_history_rebuild`, registra RUNNING durante la ejecución y maneja interrupciones de teclado guardando estado INTERRUPTED y haciendo rollback del lote abierto cuando el proceso recibe la señal normalmente. Si el proceso se termina abruptamente, prevalece la evidencia de commits de la base y el último checkpoint local.

El plan original fue ejecutado en lectura. Consultar `apply_2026.json` para la carga actualmente priorizada y `apply.json` para los lotes confirmados antes del cambio de alcance. Este documento no certifica por sí solo que haya finalizado.

Al terminar: verificar estado COMPLETED, suma de insertados/existentes, diferencias cero y cobertura hasta el corte; preservar la evidencia de abril de 2025 y registrar explícitamente el hueco mayo/junio de 2025. La validación por lote compara cantidades, no sólo conteos. Los reportes y el `.env` permanecen fuera de Git.
