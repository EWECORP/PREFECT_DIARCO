from prefect import flow, task
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from scripts.utils.logger import setup_logger

logger = setup_logger("scheduler")

@task
def tarea_nocturna():
    logger.info("Ejecutando tarea nocturna (transferencia masiva de novedades)...")

@task
def tarea_recurrente():
    logger.info("Ejecutando tarea recurrente (actualización de pedidos desde Connexa)...")

@flow(name="flujo_nocturno")
def flujo_nocturno():
    tarea_nocturna()

@flow(name="flujo_recurrente")
def flujo_recurrente():
    tarea_recurrente()

if __name__ == "__main__":
    # Registrar los cronogramas
    Deployment.build_from_flow(
        flow=flujo_nocturno,
        name="nocturno-diario",
        schedule=CronSchedule(cron="0 2 * * *", timezone="America/Argentina/Buenos_Aires"),
        work_queue_name="dmz-diarco"
    ).apply()

    Deployment.build_from_flow(
        flow=flujo_recurrente,
        name="recurrente-cada-30-min",
        schedule=CronSchedule(cron="*/30 * * * *", timezone="America/Argentina/Buenos_Aires"),
        work_queue_name="dmz-diarco"
    ).apply()
    
## ▶️ Para registrar los flujos programados
#cd D:\Services\ETL_DIARCO
# venv\Scripts\activate
# python jobs\cronograma_flujos.py