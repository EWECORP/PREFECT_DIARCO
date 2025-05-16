@echo off
setlocal enabledelayedexpansion
@echo off
setlocal enabledelayedexpansion

REM === CONFIGURACIÓN GENERAL ===
SET BASE_DIR=D:\Services\ETL_DIARCO
SET VENV_PATH=%BASE_DIR%\venv
SET ETL_ENV_PATH=%BASE_DIR%\.env
SET LOG_DIR=%BASE_DIR%\logs\worker_startup
SET LOG_FILE=%LOG_DIR%\startup_%DATE:~10,4%%DATE:~7,2%%DATE:~4,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%.log

REM === CREAR DIRECTORIO DE LOG ===
if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

REM === DEFINIR CONFIGURACION REGIONAL ===
chcp 65001 > nul
SET PYTHONUTF8=1

REM === DEFINIR VARIABLES DE ENTORNO NECESARIAS ===
SET ETL_ENV_PATH=%ETL_ENV_PATH%
SET BASE_DIR=%BASE_DIR%

REM === REGISTRAR INICIO ===
echo [%DATE% %TIME%] 🟢 Iniciando worker dmz-diarco > "%LOG_FILE%"
echo Python: %VENV_PATH%\Scripts\python.exe >> "%LOG_FILE%"
echo ETL_ENV_PATH: %ETL_ENV_PATH% >> "%LOG_FILE%"
echo Directorio base: %BASE_DIR% >> "%LOG_FILE%"
echo PYTHONUTF8: %PYTHONUTF8% >> "%LOG_FILE%"
echo CHCP: 65001 (UTF-8) >> "%LOG_FILE%"

REM === CAMBIAR A DIRECTORIO DE TRABAJO ===
cd /d %BASE_DIR%

REM === ACTIVAR ENTORNO VIRTUAL ===
call "%VENV_PATH%\Scripts\activate.bat"

REM === INICIAR EL WORKER ===
echo [%DATE% %TIME%] 🚀 Ejecutando prefect worker... >> "%LOG_FILE%"
start "PrefectWorker" cmd /k ^
"chcp 65001 & set PYTHONUTF8=1 & set ETL_ENV_PATH=%ETL_ENV_PATH% & ^
echo Worker iniciado con entorno virtual & ^
prefect worker start -p dmz-diarco >> \"%LOG_FILE%\" 2>&1"

REM === FINAL ===
echo [%DATE% %TIME%] ✅ Comando enviado para levantar worker >> "%LOG_FILE%"
endlocal
exit


