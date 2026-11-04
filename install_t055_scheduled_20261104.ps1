$ErrorActionPreference = 'Stop'

$target = 'E:\ETL\ETL_DIARCO'
$archive = Join-Path $PSScriptRoot 'deploy_t055_scheduled_20261104.zip'
$expectedSha256 = '81800EF1EDBAF21C7B0122BA50D81C94EA278A48437CF1A4F8608404E60039E1'
$main = Join-Path $target 'scripts\send\actualizar_tablas_maestras.py'
$helper = Join-Path $target 'scripts\send\refresh_t055_condcompra.py'

if (-not (Test-Path -LiteralPath $target -PathType Container)) {
    throw "No existe la instalación del worker: $target"
}
if (-not (Test-Path -LiteralPath $main -PathType Leaf)) {
    throw "No existe el script actual: $main"
}
if (-not (Test-Path -LiteralPath $archive -PathType Leaf)) {
    throw "No se encuentra el paquete: $archive"
}
$actualSha256 = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash
if ($actualSha256 -ne $expectedSha256) {
    throw "El SHA-256 del paquete no coincide: $actualSha256"
}

$backup = Join-Path $target ('backup_t055_' + (Get-Date -Format 'yyyyMMdd_HHmmss'))
New-Item -ItemType Directory -Path $backup | Out-Null
Copy-Item -LiteralPath $main -Destination (Join-Path $backup 'actualizar_tablas_maestras.py')
if (Test-Path -LiteralPath $helper -PathType Leaf) {
    Copy-Item -LiteralPath $helper -Destination (Join-Path $backup 'refresh_t055_condcompra.py')
}

Expand-Archive -LiteralPath $archive -DestinationPath $target -Force
if (-not (Test-Path -LiteralPath $helper -PathType Leaf)) {
    throw "Falta el nuevo módulo después de instalar: $helper"
}

Write-Output "Instalación T055 terminada. Respaldo: $backup"
Get-FileHash -LiteralPath $main, $helper -Algorithm SHA256 |
    Select-Object Path, Hash
