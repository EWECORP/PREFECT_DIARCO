# Objetivo:
# - Arrancar Prefect Worker (pool: dmz-diarco) de forma determinística
# - Logs en UTF-8 (evita texto “espaciado” y caracteres rotos)
# - Cargar variables desde .env (si existe)
# - Fijar PREFECT_API_URL (self-hosted)
# - (Opcional) deshabilitar events websocket si DMZ lo bloquea
#
# Archivo:  E:/ETL/ETL_DIARCO/start_worker_dmz.ps1
# Arranque robusto de Prefect Worker en Windows (sin locks de redirección)

$ErrorActionPreference = "Stop"

# === CONFIGURACIÓN GENERAL ===
$BaseDir    = "E:\ETL\ETL_DIARCO"
$VenvPath   = "$BaseDir\venv"
$EnvPath    = "$BaseDir\.env"

$LogDir     = "$BaseDir\logs\worker_startup"
$WorkerLogDir = "$BaseDir\logs\worker"

$DateStamp  = Get-Date -Format "yyyyMMdd_HHmmss"
$RunId      = [guid]::NewGuid().ToString("N")

# Log del launcher (este script)
$LauncherLog = "$LogDir\launcher_${DateStamp}_$RunId.log"

# Log dedicado del worker (stdout+stderr unificados)
$WorkerLog   = "$WorkerLogDir\worker_${DateStamp}_$RunId.log"

$WorkerPool = "dmz-diarco"

# === CREAR DIRECTORIOS DE LOG ===
if (!(Test-Path -Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
if (!(Test-Path -Path $WorkerLogDir)) { New-Item -ItemType Directory -Path $WorkerLogDir | Out-Null }

# === FUNCIÓN DE LOG (launcher) UTF-8 ===
function Log([string]$msg) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    ("[$timestamp] $msg") | Out-File -FilePath $LauncherLog -Append -Encoding utf8
}

# === CARGA SIMPLE DE .env (KEY=VALUE) ===
function Import-DotEnv([string]$path) {
    if (!(Test-Path $path)) {
        Log "[WARN] No existe .env en $path (se continúa)."
        return
    }
    Log "[INFO] Cargando variables desde .env: $path"

    Get-Content $path -Encoding UTF8 | ForEach-Object {
        $line = $_.Trim()
        if (-not $line) { return }
        if ($line.StartsWith("#")) { return }

        $parts = $line.Split("=", 2)
        if ($parts.Count -ne 2) { return }

        $k = $parts[0].Trim()
        $v = $parts[1].Trim()

        if (($v.StartsWith('"') -and $v.EndsWith('"')) -or ($v.StartsWith("'") -and $v.EndsWith("'"))) {
            $v = $v.Substring(1, $v.Length - 2)
        }

        if ($k) {
            [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
        }
    }
}

# === INICIO ===
Log "[INFO] Iniciando Prefect Worker para pool '$WorkerPool'"
Log "[INFO] BaseDir: $BaseDir"
Log "[INFO] VenvPath: $VenvPath"
Log "[INFO] Archivo .env: $EnvPath"
Log "[INFO] LauncherLog: $LauncherLog"
Log "[INFO] WorkerLog: $WorkerLog"

# === VARIABLES DE ENTORNO PROPIAS ===
$env:ETL_ENV_PATH = $EnvPath
$env:BASE_DIR     = $BaseDir
$env:PYTHONUTF8   = "1"

# Forzar UTF-8 en consola
chcp 65001 | Out-Null

# === CARGAR .env ===
Import-DotEnv $EnvPath

# === PREFECT (self-hosted) ===
$env:PREFECT_API_URL = "https://orquestador.connexa-cloud.com/api"
Log "[INFO] PREFECT_API_URL=$($env:PREFECT_API_URL)"

# (Opcional) si DMZ vuelve a molestar con events websocket:
# $env:PREFECT_API_EVENTS_STREAM_OUT_ENABLED = "false"

# === ACTIVAR ENTORNO VIRTUAL ===
$activateScript = "$VenvPath\Scripts\Activate.ps1"
if (!(Test-Path $activateScript)) {
    Log "[ERROR] No se encontró Activate.ps1 en $activateScript"
    throw "Activación del entorno fallida."
}

. $activateScript
Log "[OK] Entorno virtual activado correctamente."
Log "[INFO] Ejecutando: prefect worker start --pool $WorkerPool"

# === INICIAR WORKER (stdout+stderr a un solo stream) ===
try {
    # Evitar que stderr de comando nativo se convierta en excepción (PS 7+)
    $oldErr = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    $oldNative = $null
    if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -Scope Global -ErrorAction SilentlyContinue) {
        $oldNative = $global:PSNativeCommandUseErrorActionPreference
        $global:PSNativeCommandUseErrorActionPreference = $false
    }

    # Unificar stderr -> stdout y volcar a archivo (una sola apertura del archivo)
    & prefect worker start --pool $WorkerPool 2>&1 |
        Out-File -FilePath $WorkerLog -Append -Encoding utf8

    $ExitCode = $LASTEXITCODE

    # Restaurar flags
    if ($oldNative -ne $null) { $global:PSNativeCommandUseErrorActionPreference = $oldNative }
    $ErrorActionPreference = $oldErr

    # Normalmente no llega aquí porque el worker queda corriendo.
    if ($ExitCode -ne 0) {
        Log "[ERROR] Worker terminó con código $ExitCode (ver WorkerLog: $WorkerLog)"
        throw "Fallo en la ejecución del worker (ExitCode=$ExitCode)"
    } else {
        Log "[OK] Worker finalizó con ExitCode 0 (ver WorkerLog: $WorkerLog)"
    }
}
catch {
    Log "[ERROR] Excepción al ejecutar el worker: $($_.Exception.Message)"
    Log "[ERROR] Revisar también WorkerLog: $WorkerLog"
    exit 1
}

Log "[INFO] Proceso finalizado correctamente."