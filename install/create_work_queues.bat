@echo off
REM === Activar entorno virtual ===
cd /d D:\Services\ETL_DIARCO
call venv\Scripts\activate

REM === Crear work-queues requeridas en el pool 'dmz-diarco' ===
prefect work-queue create -p dmz-diarco default
prefect work-queue create -p dmz-diarco articulos-queue
prefect work-queue create -p dmz-diarco ventas-queue
prefect work-queue create -p dmz-diarco stock-queue
prefect work-queue create -p dmz-diarco precios-queue
prefect work-queue create -p dmz-diarco maestros-queue
prefect work-queue create -p dmz-diarco transacionales-queue


ork pool with name: 'dmz-pool' not found.
(venv) PS D:\Services\ETL_DIARCO\install> prefect work-queue create -p dmz-diarco default
Work queue with name: 'default' already exists.
(venv) PS D:\Services\ETL_DIARCO\install>