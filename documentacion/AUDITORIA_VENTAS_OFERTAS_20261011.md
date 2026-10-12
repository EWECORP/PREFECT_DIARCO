# Auditoría de ventas y ofertas después de la reconciliación

## Hallazgos del 11/10/2026

- La ejecución de `PDD_SALES_RECONCILIATION_WEEKLY` terminó en `Completed`. Su corrida `01a0bee7-ba08-7a44-a726-740ec2b161e1` detectó y reparó ventas de origen de los días 08, 09 y 10 de octubre.
- `datamart.sp_procesar_promos_mes` reconstruyó el baseline de octubre y la venta enriquecida del 01 al 10 de octubre, porque el baseline es mensual. El log `datamart.dm_bve_proceso_log.id=39` informa `OK`, 219.172 filas de baseline, 1.138.220 ventas enriquecidas y 6.008 promos fuertes.
- La consulta de comparación con el scope de TEST `c533e7e7-3363-4dc3-8127-24f48fe33522` encontró, para el 01 al 09 de octubre, 2.682 pares día/artículo/sucursal con venta basal distinta entre PDD y la fuente enriquecida actual. La suma de diferencias absolutas fue 31.330 unidades. La venta promocional mostró la misma diferencia en sentido inverso en la comprobación puntual; esto indica que PDD conserva clasificaciones previas al recálculo.
- La auditoría de origen versus enriquecida del 01 al 10 de octubre comparó 1.138.220 filas en cada lado. No encontró diferencias de filas ni unidades, ni registros con baseline desactualizado, puntaje incorrecto, promo fuerte incorrecta o reparto basal/promocional incorrecto. Los puntajes actuales fueron: 42.868 filas con score 40, 35.539 con score 70 y 6.008 con score 100.
- El reproceso manual de `PG_Genera_Dia_VENTA_ENRIQUECIDA.sql` usa el baseline mensual existente. El archivo local fija `2026-08-21` a `2026-08-23` como ejemplo de ejecución; no se puede inferir de ese archivo qué fechas se ejecutaron efectivamente en el servidor. Por ello agosto y septiembre requieren auditoría con filtros y evidencia de ejecución antes de afirmar que quedaron correctos.
- En agosto, origen y enriquecida contienen 3.371.414 filas cada uno, sin diferencias de cantidad ni unidades. Hay 1.100 filas con score distinto a la regla vigente, 921 con marca de promo fuerte distinta y 19 con reparto basal/promocional distinto. Las 1.100 filas con score distinto tienen `unidades_mediana <= 0`; 921 de ellas están marcadas como promo fuerte. Esto coincide con el SQL manual, que no valida `unidades_mediana > 0` antes de asignar scores 40/70/100, a diferencia del procedimiento actual. Entre ellas hay 15 ventas con unidades positivas y mediana no positiva clasificadas con score >= 70; requieren revisión prioritaria. La coincidencia de lógica no prueba qué ejecución produjo cada fila.
- En septiembre, origen y enriquecida contienen 2.801.067 filas cada uno, sin diferencias de cantidad ni unidades. Hay 777 filas con score distinto a la regla vigente, 661 con marca de promo fuerte distinta y 16 con reparto distinto. Las 777 filas con score distinto tienen `unidades_mediana <= 0`; 12 son ventas con unidades positivas y score >= 70. También coinciden con la ausencia de la condición de mediana positiva en el SQL manual. El log de cálculo de estas filas debe contrastarse con la secuencia real de reprocesos antes de atribuir su origen.

## Utilidad de consulta

Desde la raíz de `ETL_DIARCO`, con Python que tenga `psycopg2` y `python-dotenv`:

```powershell
python scripts/pdd/auditar_ventas_ofertas.py --desde 2026-08-01 --hasta 2026-08-31 --articulo 42758 --csv auditoria_articulo_42758.csv
python scripts/pdd/auditar_ventas_ofertas.py --desde 2026-09-01 --hasta 2026-09-30 --sucursal 18 --csv auditoria_sucursal_18.csv
python scripts/pdd/auditar_ventas_ofertas.py --desde 2026-08-01 --hasta 2026-08-31 --solo-anomalias --csv anomalias_agosto.csv
python scripts/pdd/auditar_ventas_ofertas.py --desde 2026-10-01 --hasta 2026-10-10 --scope-version-uuid c533e7e7-3363-4dc3-8127-24f48fe33522 --csv auditoria_octubre.csv
```

Las fechas `--desde` y `--hasta` son inclusivas. Sin artículo ni sucursal, la salida resume por día; `--solo-anomalias` lista los pares afectados sin necesidad de conocer el artículo. Con filtro de artículo o sucursal, muestra cada par día/artículo/sucursal. Lee la conexión desde `ETL_ENV_PATH` o `ETL_DIARCO/.env`, con prioridad para variables de entorno `PG_*`. El CSV usa UTF-8 con BOM para Excel. La conexión es de sólo lectura y tiene timeout configurable. Una consulta mensual sin filtros puede tardar varios minutos.

Las columnas `diferencia_filas` y `diferencia_unidades` comparan `src.base_ventas_extendida` con `datamart.dm_bve_ventas_enriquecidas`. `baseline_desactualizado` cuenta filas enriquecidas que no coinciden con el baseline mensual vigente. `score_incorrecto`, `promo_fuerte_incorrecta` y `reparto_incorrecto` recalculan las reglas de `sp_procesar_promos_mes.sql`. Las diferencias de PDD comparan las unidades basales/promocionales positivas con `dm_pdd_venta_diaria`. `pdd_desactualizado` compara las marcas de cálculo.

La comparación de PDD incluye únicamente pares con venta de origen o enriquecida; los días sin venta que PDD genera por calendario quedan fuera. Una diferencia de PDD puede requerir inspeccionar la política de exclusiones y el método de ajuste antes de catalogarla como error. Las diferencias de origen y las reglas del enriquecimiento son controles de consistencia, no una verificación contra transacciones individuales de SGM.

## Siguiente paso operativo

Primero revisar y guardar el CSV de cada mes, y contrastar las fechas reprocesadas manualmente con los logs. Corregir la clasificación de las filas de agosto con mediana no positiva mediante el procedimiento vigente, previa evaluación del alcance mensual y de los consumidores. Después de validar la fuente, regenerar `dm_pdd_venta_diaria` para el período afectado usando el flujo de preparación de features de PDD con el scope correcto. Luego volver a comparar y ejecutar de nuevo el backtest cuyo histórico quedó anterior al recálculo. Evitar tomar decisiones con las métricas previas del backtest hasta esa actualización.
