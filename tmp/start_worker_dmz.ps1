# === CONFIGURACIÓN GENERAL ===
$BaseDir    = "E:\ETL\ETL_DIARCO"
$VenvPath   = "$BaseDir\venv"
$EnvPath    = "$BaseDir\.env"
$LogDir     = "$BaseDir\logs\worker_startup"
$DateStamp  = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile    = "$LogDir\startup_$DateStamp.log"
$WorkerPool = "dmz-diarco"

# === CREAR DIRECTORIO DE LOG ===
if (!(Test-Path -Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

# === FUNCIÓN DE LOG ===
function Log($msg) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content $LogFile "[$timestamp] $msg"
}

# === INICIO ===
Log "[INFO] Iniciando Prefect Worker para '$WorkerPool'"
Log "[INFO] BaseDir: $BaseDir"
Log "[INFO] Activando entorno virtual con activate.ps1"
Log "[INFO] Archivo .env: $EnvPath"

# === DEFINIR VARIABLES DE ENTORNO ===
$env:ETL_ENV_PATH = $EnvPath
$env:BASE_DIR     = $BaseDir
$env:PYTHONUTF8   = "1"
$env:PREFECT_API_URL = "https://orquestador.connexa-cloud.com/api"

chcp 65001 | Out-Null

# === ACTIVAR ENTORNO VIRTUAL ===
$activateScript = "$VenvPath\Scripts\Activate.ps1"
if (!(Test-Path $activateScript)) {
    Log "[ERROR] No se encontró Activate.ps1 en $activateScript"
    throw "Activación del entorno fallida."
}

. $activateScript
Log "[OK] Entorno virtual activado correctamente."
Log "[INFO] Ejecutando prefect worker start -p $WorkerPool"

# === INICIAR WORKER ===
try {
    prefect worker start -p $WorkerPool >> $LogFile 2>&1
    $ExitCode = $LASTEXITCODE

    if ($ExitCode -ne 0) {
        Log "[ERROR] Worker terminó con código $ExitCode"
        throw "Fallo en la ejecución del worker"
    } else {
        Log "[OK] Worker ejecutado correctamente."
    }
}
catch {
    Log "[ERROR] Excepción al ejecutar el worker: $_"
    exit 1
}

Log "[INFO] Proceso finalizado correctamente."
