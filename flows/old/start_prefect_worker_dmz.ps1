# Activar entorno virtual Prefect
$envPath = "D:\Services\PREFECT\Scripts\Activate.ps1"
if (Test-Path $envPath) {
    & $envPath
} else {
    Write-Error "No se encontró el entorno virtual PREFECT en D:\Services\PREFECT"
    exit 1
}

# Configurar variable PREFECT_API_URL
$env:PREFECT_API_URL = "http://140.99.164.229:4200/api"
[Environment]::SetEnvironmentVariable("PREFECT_API_URL", "http://140.99.164.229:4200/api", "User")

# Iniciar worker en el pool dmz-pool
prefect worker start --pool dmz-pool