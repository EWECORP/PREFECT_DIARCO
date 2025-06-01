import sys
import os
import locale
import subprocess
from pathlib import Path

def diagnostico_utf8():
    print("🧪 Diagnóstico de compatibilidad UTF-8 en el entorno Prefect / Windows\n")

    # Python ejecutado
    print(f"🐍 Python ejecutado desde: {sys.executable}")
    print(f"📦 Versión de Python: {sys.version}")
    
    # Codificaciones
    print(f"🔤 Codificación por defecto: {sys.getdefaultencoding()}")
    print(f"📁 Codificación de sistema de archivos: {sys.getfilesystemencoding()}")
    print(f"🌍 Codificación regional (locale): {locale.getpreferredencoding()}")

    # Variable de entorno
    utf_env = os.environ.get("PYTHONUTF8", "NO DEFINIDA")
    print(f"🌐 PYTHONUTF8 = {utf_env}")

    # Código de página actual (solo Windows)
    if os.name == "nt":
        try:
            chcp_output = subprocess.check_output("chcp", shell=True, text=True).strip()
            print(f"📄 Código de página actual (chcp): {chcp_output}")
        except Exception as e:
            print(f"⚠️ Error consultando CHCP: {e}")

    # Entorno de Prefect
    etl_env_path = os.environ.get("ETL_ENV_PATH", "NO DEFINIDA")
    print(f"📁 ETL_ENV_PATH = {etl_env_path}")

    # Prueba de impresión UTF-8
    print("\n🔠 Prueba de impresión de caracteres extendidos:")
    try:
        print("→ á, é, í, ó, ú, ñ, ü, ¿, ¡, 😊")
        print("✅ Si ves bien los acentos y emojis, la salida es compatible con UTF-8.")
    except UnicodeEncodeError as ue:
        print(f"❌ Error imprimiendo caracteres extendidos: {ue}")

    print("\n🧩 Sugerencias:")
    print("- Asegurate de tener configurado PYTHONUTF8=1")
    print("- Usá 'chcp 65001' en scripts .bat")
    print("- En Prefect, usá subprocess con encoding='utf-8', errors='replace'")
    print("- Evitá emojis o tildes si no controlás completamente el entorno")

if __name__ == "__main__":
    diagnostico_utf8()