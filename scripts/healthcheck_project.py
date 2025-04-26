
import os
import sys

def verificar_archivos():
    print("\n✅ Verificación de estructura de archivos y módulos Prefect Diarco:")

    estructura_esperada = [
        ('flows/sync_sql_env_with_logging.py', "Flujo con Logging"),
        ('flows/sync_sql_env.py', "Flujo simple con .env"),
        ('scripts/logger.py', "Logger de eventos"),
        ('scripts/dashboard_log_viewer.py', "Dashboard simple de Logs"),
        ('scripts/__init__.py', "Inicializador de paquete scripts"),
        ('.env', "Archivo de variables de entorno"),
        ('logs/', "Carpeta para logs de eventos"),
    ]

    errores = False
    for path, descripcion in estructura_esperada:
        if not os.path.exists(path):
            print(f"❌ FALTA: {descripcion} --> {path}")
            errores = True
        else:
            print(f"✅ OK: {descripcion}")

    if errores:
        print("\n⚠️ Se detectaron faltantes o errores. Verificar antes de ejecutar flujos Prefect.")
    else:
        print("\n🎯 Todo en orden para comenzar. Puede ejecutar sus flujos con Prefect sin inconvenientes.")

if __name__ == "__main__":
    verificar_archivos()
