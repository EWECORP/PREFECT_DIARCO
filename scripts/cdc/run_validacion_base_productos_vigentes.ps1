param(
    [switch]$SkipConfirmation
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptDir)
$flowScript = Join-Path $projectRoot "scripts\push\obtener_base_productos_vigentes.py"
$validationSql = Join-Path $projectRoot "cdc\postgres\130_validate_base_productos_vigentes_modes.sql"

function Write-Step {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] $Message"
}

function Invoke-Snapshot {
    param(
        [string]$Mode,
        [string]$TargetTable
    )

    $payload = "{'mode': '$Mode', 'target_table': '$TargetTable'}"
    $startedAt = Get-Date

    Write-Step "Iniciando snapshot mode=$Mode target_table=$TargetTable"
    & $pythonExe $flowScript $payload
    if ($LASTEXITCODE -ne 0) {
        throw "La corrida mode=$Mode finalizo con exit code $LASTEXITCODE."
    }

    $elapsed = (Get-Date) - $startedAt
    Write-Step ("Snapshot completado mode={0} | duracion={1:n1} min" -f $Mode, $elapsed.TotalMinutes)
}

function Resolve-PythonExecutable {
    $candidates = @()

    if ($env:VIRTUAL_ENV) {
        $candidates += (Join-Path $env:VIRTUAL_ENV "Scripts\python.exe")
    }

    $candidates += (Join-Path $projectRoot "venv\Scripts\python.exe")

    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return $candidate
        }
    }

    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        return "python"
    }

    throw "No se encontro un interprete Python utilizable."
}

$pythonExe = Resolve-PythonExecutable

if (-not (Test-Path $flowScript)) {
    throw "No se encontro el flujo esperado: $flowScript"
}

if (-not (Test-Path $validationSql)) {
    throw "No se encontro la validacion SQL esperada: $validationSql"
}

Write-Host ""
Write-Host "Validacion manual de base_productos_vigentes"
Write-Host "Proyecto: $projectRoot"
Write-Host "Python : $pythonExe"
Write-Host ""
Write-Host "Precondiciones recomendadas:"
Write-Host "1. Ya corrio la replicacion legacy de la manana."
Write-Host "2. Aun no comenzo la actividad operativa fuerte en DIARCO."
Write-Host "3. Las dos corridas se ejecutaran back-to-back."
Write-Host ""

if (-not $SkipConfirmation) {
    $answer = Read-Host "Confirmar que queres ejecutar ahora la validacion (S/N)"
    if ($answer -notin @("S", "s", "SI", "Si", "si", "Y", "y")) {
        Write-Step "Operacion cancelada por el usuario."
        exit 0
    }
}

$globalStartedAt = Get-Date

Invoke-Snapshot -Mode "sqlserver_sp" -TargetTable "src.base_productos_vigentes_cmp_sqlserver_sp"
Invoke-Snapshot -Mode "hybrid_src" -TargetTable "src.base_productos_vigentes_cmp_hybrid_src"

$globalElapsed = (Get-Date) - $globalStartedAt

Write-Host ""
Write-Step ("Snapshots finalizados | duracion_total={0:n1} min" -f $globalElapsed.TotalMinutes)
Write-Host ""
Write-Host "Siguiente paso:"
Write-Host "Ejecutar en PostgreSQL el archivo:"
Write-Host "  $validationSql"
Write-Host ""
Write-Host "Consultas clave a revisar:"
Write-Host "- 2. Conteo total y claves distintas por modo"
Write-Host "- 2b. Diferencia temporal entre snapshots"
Write-Host "- 5. Diferencia de cobertura por clave operativa"
Write-Host "- 7. Conteo de diferencias de atributos sobre claves compartidas"
Write-Host ""
