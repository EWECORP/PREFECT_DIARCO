# Archivo: utils/rename_prefect_deployments.py
# Prefect OSS 3.4.1 compatible
#
# Estrategia:
#   - NO se renombra "in-place" (no es estable/soportado).
#   - Se CREA un nuevo Deployment con el nombre nuevo (clonando config básica).
#   - (Opcional) se PAUSA el Deployment viejo (en 3.4.1 el campo es is_paused).
#   - Es idempotente: si el deployment nuevo ya existe para el mismo flow_id, no lo recrea.
#
# Uso:
#   DRY-RUN:
#     python utils\rename_prefect_deployments.py
#   APLICAR + pausar viejos:
#     python utils\rename_prefect_deployments.py --apply --pause-old

from __future__ import annotations

import argparse
import asyncio
from typing import Any, Dict, List, Optional, Tuple

from prefect.client.orchestration import get_client


RENAME_MAP: Dict[str, str] = {
    # --- FORECAST / CONNEXA ---
    "flujo_forecast_MS_Diurno": "FORECAST_RUN_DIURNO_PROD",
    "flujo_forecast_MS_Vespertino": "FORECAST_RUN_VESPERTINO_PROD",
    "flujo_actualizar_maestros_MS": "MASTER_SYNC_MAESTROS_PROD",

    # --- DMZ / ETL ---
    "publicar_oc_precarga": "OC_PUBLISH_PRECARGA_PROD",
    "replicar_dmz_lotes": "REPL_SYNC_CDC_LOTES_PROD",
    "replicar_OC_dmz": "REPL_SYNC_OC_CDC_PROD",
    "Push_datos_Diarios_para_FORECAST": "FORECAST_PUSH_INPUT_PROVEEDORES_DIARIO_PROD",
    "Refrecar_datos_Diarios_para_FORECAST": "FORECAST_REFRESH_INPUT_PROVEEDORES_MEDIODIA_PROD",
    "Push_datos_Ventas_Stock_Diarios": "FORECAST_PUSH_VENTAS_STOCK_DIARIO_PROD",
    "Actualizar_tablas_maestras_Postgres": "MASTER_SYNC_MAESTROS_POSTGRES_PROD",
    "Actualizar_tablas_Tabulares_Postgres": "MASTER_SYNC_TABULARES_POSTGRES_PROD",
    "actualizar_base_ventas_extendida_diario": "ETL_REFRESH_BVE_DIARIO_PROD",
    "actualizar_base_ventas_extendida_BARRIO": "ETL_REFRESH_BVE_BARRIO_DIARIO_PROD",
    "Refrescar_Tablas_Maestras_Postgres": "MASTER_REFRESH_MAESTROS_MEDIODIA_PROD",
    "Publicar_Transferencias_VKM": "TRANSFER_PUBLISH_VKM_PROD",
}


# -----------------------------
# Helpers de serialización
# -----------------------------
def _dump_model(obj: Any) -> Any:
    """Serializa objetos Pydantic v2 a dict, si aplica."""
    if obj is None:
        return None
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    return obj


def _to_schedule_create_dict(ds: Any) -> Dict[str, Any]:
    """
    Convierte un DeploymentSchedule (read model) a un dict compatible con DeploymentScheduleCreate (create model).
    Prefect 3.4.1: create_deployment() requiere schedules como dicts (no objetos read).
    """
    raw = _dump_model(ds)
    if not isinstance(raw, dict):
        raise TypeError(f"Schedule inesperado: {type(ds)}")

    raw_schedule = raw.get("schedule")
    raw["schedule"] = _dump_model(raw_schedule)

    # remover campos de solo lectura (defensivo)
    for k in ("id", "deployment_id", "created", "updated", "created_by", "updated_by"):
        raw.pop(k, None)

    # defaults razonables
    if raw.get("parameters") is None:
        raw["parameters"] = {}

    # "active" existe en muchos esquemas; si no está, no lo forzamos
    return raw


# -----------------------------
# Lectura paginada deployments
# -----------------------------
async def read_all_deployments(client, page_size: int = 200) -> List[Any]:
    all_deps: List[Any] = []
    offset = 0
    while True:
        batch = await client.read_deployments(limit=page_size, offset=offset)
        if not batch:
            break
        all_deps.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_deps


async def find_deployment_by_flow_and_name(client, flow_id: str, name: str) -> Optional[Any]:
    """
    Busca un deployment por (flow_id, name) de forma compatible:
    - Prefect 3.4.1: no siempre hay filtros server-side uniformes,
      así que leemos y filtramos del lado cliente.
    """
    deps = await read_all_deployments(client)
    for d in deps:
        if str(getattr(d, "flow_id", "")) == str(flow_id) and (d.name or "") == name:
            return d
    return None


# -----------------------------
# Clonado + (opcional) pausa old
# -----------------------------
async def clone_deployment_with_new_name(
    client,
    deployment_id: str,
    new_name: str,
    pause_old: bool = True,
    continue_on_pause_error: bool = True,
) -> Any:
    dep = await client.read_deployment(deployment_id)

    # Idempotencia: si ya existe el deployment nuevo para este flow_id, no lo recrea
    existing_new = await find_deployment_by_flow_and_name(client, str(dep.flow_id), new_name)
    if existing_new is not None:
        new_dep = existing_new
    else:
        # Schedules: convertir a dicts "Create"
        schedules_create: Optional[List[Dict[str, Any]]] = None
        if getattr(dep, "schedules", None):
            schedules_create = [_to_schedule_create_dict(s) for s in dep.schedules]

        payload: Dict[str, Any] = {
            "flow_id": dep.flow_id,
            "name": new_name,
            "description": getattr(dep, "description", None),
            "tags": list(getattr(dep, "tags", []) or []),
            "parameters": dict(getattr(dep, "parameters", {}) or {}),
            "schedules": schedules_create,
            # Infra / pool / queue vars (si existen en su deployment)
            "work_pool_name": getattr(dep, "work_pool_name", None),
            "work_queue_name": getattr(dep, "work_queue_name", None),
            "job_variables": getattr(dep, "job_variables", None),
        }

        # Limpiar None
        payload = {k: v for k, v in payload.items() if v is not None}

        new_dep = await client.create_deployment(**payload)

    # Pausar el viejo (Prefect 3.4.1 usa is_paused)
    if pause_old:
        try:
            await client.update_deployment(deployment_id, is_paused=True)
        except Exception as e:
            if continue_on_pause_error:
                # No abortar todo el batch por un pause fallido
                print(f"  [WARN] No se pudo pausar el deployment viejo '{dep.name}' (id={dep.id}): {e}")
            else:
                raise

    return new_dep


# -----------------------------
# Main
# -----------------------------
async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Aplica cambios (por defecto: dry-run).")
    parser.add_argument("--pause-old", action="store_true", help="Pausa el deployment viejo después de clonar.")
    args = parser.parse_args()

    async with get_client() as client:
        deployments = await read_all_deployments(client)

        plan: List[Tuple[str, str, str]] = []
        for d in deployments:
            old = d.name or ""
            if old in RENAME_MAP:
                new = RENAME_MAP[old]
                if new and new != old:
                    plan.append((str(d.id), old, new))

        if not plan:
            print("No hay deployments a migrar (según el mapeo actual).")
            return

        print("\nPlan (crear nuevo + pausar viejo opcional):")
        for _, old, new in sorted(plan, key=lambda x: x[1].lower()):
            print(f"  {old} -> {new}")

        if not args.apply:
            print("\n(DRY-RUN) No se aplicaron cambios. Ejecutar con --apply para confirmar.")
            return

        print("\nAplicando...")
        for dep_id, old, new in plan:
            new_dep = await clone_deployment_with_new_name(
                client,
                deployment_id=dep_id,
                new_name=new,
                pause_old=args.pause_old,
                continue_on_pause_error=True,
            )
            print(f"  OK {old} -> {new} (new_id={new_dep.id})")

        print("\nListo. Validar en UI que:")
        print("  - Los deployments nuevos existan y tengan schedules correctos.")
        if args.pause_old:
            print("  - Los deployments viejos hayan quedado pausados (o revisar warnings).")


if __name__ == "__main__":
    asyncio.run(main())