@echo off
REM Activar entorno virtual
call D:\services\PREFECT\Scripts\activate.bat

REM Establecer variable de entorno Prefect API
set PREFECT_API_URL=http://orquestador.connexa-cloud.com:4200/api

REM Ir al directorio donde est� el flujo
cd /d D:\services\ETL_DIARCO\flows

REM Ejecutar el flujo
python replicacion_parametrizada_excel.py

