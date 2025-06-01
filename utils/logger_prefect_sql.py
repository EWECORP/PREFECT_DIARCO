import pyodbc
import json
from datetime import datetime

def log_proceso_etl(
    nombre_flujo,
    nombre_tarea,
    estado,
    registros_afectados,
    mensaje_error=None,
    contexto_extra=None
):
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=data_sync;UID=sa;PWD=yourStrong(!)Password"
        )
        cursor = conn.cursor()
        fecha_inicio = datetime.utcnow().replace(microsecond=0)
        fecha_fin = datetime.utcnow().replace(microsecond=0)
        contexto_json = json.dumps(contexto_extra or {})

        cursor.execute("""
            INSERT INTO logs.procesos_etl (
                nombre_flujo,
                nombre_tarea,
                fecha_inicio,
                fecha_fin,
                estado,
                registros_afectados,
                mensaje_error,
                contexto
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        nombre_flujo,
        nombre_tarea,
        fecha_inicio,
        fecha_fin,
        estado,
        registros_afectados,
        mensaje_error,
        contexto_json
        )
        conn.commit()
    except Exception as e:
        print(f"[!] Error al registrar log ETL: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass