LOCKFILE="/python/diarco/inbound/import_online.lock"

# Verificar si el archivo de bloqueo existe
if [ -e $LOCKFILE ]; then
    echo "Locked. Exit..."
    exit 1
else

    # Crear el archivo de bloqueo
    touch $LOCKFILE

    # Tu código va aquí
    
    start_time=$(date +%s)

    export PYTHONUNBUFFERED=1
    echo "<import_online.py>"    
    cd /python/diarco/inbound
    python3 import_online.py  

    # Sync Data
    echo "<product_sync.py>"    
    cd /python/diarco/sync
    python3 product_sync.py  

    end_time=$(date +%s)
    execution_time=$((end_time - start_time))

    #!/bin/bash
    # Guardar las métricas en un archivo
    echo "script_execution_time_seconds $execution_time" > /var/lib/prometheus/node-exporter/script_metrics.prom
    
    # Eliminar el archivo de bloqueo al finalizar
    rm -f $LOCKFILE
fi


