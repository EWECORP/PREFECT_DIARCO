REM === Script para registrar el Prefect Worker como servicio en Windows ===

@echo off
REM === Activar entorno virtual ===
cd /d D:\Services\ETL_DIARCO
call venv\Scripts\activate

REM === Configurar URL del Prefect API ===
set PREFECT_API_URL=http://140.99.164.229:4200/api

REM === Iniciar el Worker Prefect ===
prefect worker start -p dmz-diarco