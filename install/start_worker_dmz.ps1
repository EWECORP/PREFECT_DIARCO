# === CONFIGURACIÓN GENERAL ===
$BaseDir      = "D:\Services\ETL_DIARCO"
$VenvPath     = "$BaseDir\venv"
$PythonPath   = "$VenvPath\Scripts\python.exe"
$EnvPath      = "$BaseDir\.env"
$LogDir       = "$BaseDir\logs\worker_startup"
$DateStamp    = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile      = "$LogDir\startup_$DateStamp.log"

# === CREAR DIRECTORIO DE LOG ===
if (!(Test-Path -Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

# === REGISTRAR INICIO ===
Add-Content $LogFile "`n[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] 🟢 Iniciando Prefect Worker"
Add-Content $LogFile "BaseDir: $BaseDir"
Add-Content $LogFile "PythonPath: $PythonPath"
Add-Content $LogFile "ETL_ENV_PATH: $EnvPath"
Add-Content $LogFile "PYTHONUTF8=1"
Add-Content $LogFile "Consola en código UTF-8 (chcp 65001)"

# === CONFIGURAR ENTORNO ===
$env:ETL_ENV_PATH = $EnvPath
$env:BASE_DIR = $BaseDir
$env:PYTHONUTF8 = "1"
chcp 65001 | Out-Null

# === ACTIVAR ENTORNO VIRTUAL ===
$activateScript = "$VenvPath\Scripts\Activate.ps1"
if (!(Test-Path $activateScript)) {
    Add-Content $LogFile "❌ Error: No se encontró Activate.ps1 en $activateScript"
    throw "Activación del entorno virtual fallida."
}

& $activateScript
Add-Content $LogFile "✅ Entorno virtual activado correctamente."

# === INICIAR EL WORKER DE PREFECT ===
Add-Content $LogFile "🚀 Ejecutando Prefect Worker para dmz-diarco..."

Start-Process -NoNewWindow powershell -ArgumentList @"
    -NoExit -Command `
    "`$env:PYTHONUTF8='1'; `
     `$env:ETL_ENV_PATH='$EnvPath'; `
     chcp 65001; `
     Write-Host '🎯 Worker lanzado'; `
     prefect worker start -p dmz-diarco"
"@ >> $LogFile 2>&1

Add-Content $LogFile "✅ Worker iniciado en segundo plano."
