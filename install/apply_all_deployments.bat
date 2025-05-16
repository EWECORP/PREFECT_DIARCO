@echo off
REM === Activar entorno virtual ===
cd /d D:\Services\ETL_DIARCO
call venv\Scripts\activate

REM === Aplicar todos los deployments actualizados ===
prefect deployment apply jobs\actualizar_stock_historico-deployment.yaml
prefect deployment apply jobs\obtener_actualizar_stock-deployment.yaml
prefect deployment apply jobs\obtener_articulos_proveedor-deployment.yaml
prefect deployment apply jobs\obtener_datos_proveedor-deployment.yaml
prefect deployment apply jobs\obtener_ventas_proveedor-deployment.yaml
prefect deployment apply jobs\replicacion_parametrizada_excel-deployment.yaml
