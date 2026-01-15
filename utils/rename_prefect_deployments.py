from __future__ import annotations

import argparse
import asyncio
from typing import Dict, List, Tuple

from prefect.client.orchestration import get_client


RENAME_MAP: Dict[str, str] = {
    # --- FORECAST / CONNEXA ---
    "flujo_forecast_MS_Diurno": "FORECAST_RUN_DIURNO_PROD",
    "flujo_forecast_MS_Vespertino": "FORECAST_RUN_VESPERTINO_PROD",
    # "flujo_publicar_precarga_MS": "OC_PUBLISH_PRECARGA_PROD",
    "flujo_actualizar_maestros_MS": "MASTER_SYNC_MAESTROS_PROD",

    # --- DMZ / ETL ---
    "publicar_oc_precarga": "OC_PUBLISH_PRECARGA_PROD",
    "replicar_dmz_lotes": "REPL_SYNC_CDC_LOTES_PROD",
    "replicar_OC_dmz": "REPL_SYNC_OC_CDC_PROD",
    "Push_datos_Diarios_para_FORECAST": "FORECAST_PUSH_INPUT_PROVEEDORES_DIARIO_PROD",
    "Refrecar_datos_Diarios_para_FORECAST": "FORECAST_REFRESH_INPUT_PROVEEDORES_MEDIODIA_PROD",
    "Push_datos_Ventas_Stock_Diarios": "FORECAST_PUSH_VENTAS_STOCK_DIARIO_PROD",
    # "exportar_tabla_sql_sftp": "REPL_EXPORT_SQLSERVER_SFTP_PROD", # No se Renombra - Es un Objeto reutilizabe
    "Push_tablas_DMZ_a_Postgres": "REPL_ORCH_DMZ_TO_POSTGRES_PROD",
    "Actualizar_tablas_maestras_Postgres": "MASTER_SYNC_MAESTROS_POSTGRES_PROD",
    "Actualizar_tablas_Tabulares_Postgres": "MASTER_SYNC_TABULARES_POSTGRES_PROD",
    "actualizar_base_ventas_extendida_diario": "ETL_REFRESH_BVE_DIARIO_PROD",
    "actualizar_base_ventas_extendida_BARRIO": "ETL_REFRESH_BVE_BARRIO_DIARIO_PROD",
    "Refrescar_Tablas_Maestras_Postgres": "MASTER_REFRESH_MAESTROS_MEDIODIA_PROD",
    "Publicar_Transferencias_VKM": "TRANSFER_PUBLISH_VKM_PROD",
}

async def read_all_deployments(client, page_size: int = 200):
    all_deps = []
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


async def rename_deployment(client, deployment_id: str, new_name: str) -> None:
    """
    Renombra deployment usando la API (Prefect Server).
    Compatible con Prefect 3.4.x / API 0.8.x.
    """
    await client.request(
        "PATCH",
        f"/deployments/{deployment_id}",
        json={"name": new_name},
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Aplica cambios (por defecto: dry-run).")
    args = parser.parse_args()

    async with get_client() as client:
        deployments = await read_all_deployments(client, page_size=200)

        plan: List[Tuple[str, str, str]] = []
        for d in deployments:
            old = d.name or ""
            if old in RENAME_MAP:
                new = RENAME_MAP[old]
                if new != old:
                    plan.append((str(d.id), old, new))

        if not plan:
            print("No hay deployments a renombrar (según el mapeo actual).")
            return

        print("\nPlan de renombre:")
        for _, old, new in sorted(plan, key=lambda x: x[1].lower()):
            print(f"  {old} -> {new}")

        if not args.apply:
            print("\n(DRY-RUN) No se aplicaron cambios. Ejecutar con --apply para confirmar.")
            return

        print("\nAplicando cambios...")
        for dep_id, old, new in plan:
            await rename_deployment(client, dep_id, new)
            print(f"  OK {old} -> {new}")

        print("\nRenombre completo.")


if __name__ == "__main__":
    asyncio.run(main())