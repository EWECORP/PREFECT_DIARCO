from prefect import flow, task
from scripts.utils.logger_prefect_sql import log_proceso_etl

@task
def tarea_critica():
    # Simular procesamiento
    data = [{"id": i} for i in range(10)]
    return data

@flow
def flujo_con_log_prefect():
    try:
        datos = tarea_critica()
        log_proceso_etl(
            nombre_flujo="flujo_con_log_prefect",
            nombre_tarea="tarea_critica",
            estado="OK",
            registros_afectados=len(datos),
            contexto_extra={"descripcion": "Carga de prueba"}
        )
    except Exception as e:
        log_proceso_etl(
            nombre_flujo="flujo_con_log_prefect",
            nombre_tarea="tarea_critica",
            estado="ERROR",
            registros_afectados=0,
            mensaje_error=str(e)
        )

if __name__ == "__main__":
    flujo_con_log_prefect()