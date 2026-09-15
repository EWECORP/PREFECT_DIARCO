"""Cancel backlog in one Prefect pool. Preview by default; workers must stay stopped."""
import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, WorkPoolFilter
from prefect.client.schemas.sorting import FlowRunSort
from prefect.settings import PREFECT_API_URL
from prefect.states import Cancelled, to_state_create


def prepare_state_schema():
    """Resolve Prefect 3.4.1's type-only import with newer Pydantic versions."""
    from prefect.client.schemas.actions import StateCreate

    if not StateCreate.__pydantic_complete__:
        from prefect._result_records import ResultRecordMetadata

        StateCreate.model_rebuild(
            _types_namespace={"ResultRecordMetadata": ResultRecordMetadata}
        )
    # Fail locally before querying the backlog if serialization is incompatible.
    to_state_create(Cancelled()).model_dump(mode="json")


def eligible(run, cutoff, include_future=False):
    if run.created is None or run.created > cutoff:
        return False
    if not run.state or run.state.name not in {"Scheduled", "Late", "Pending"}:
        return False
    if run.start_time is not None:
        return False
    if include_future or run.state.name == "Pending":
        return True
    return run.expected_start_time is not None and run.expected_start_time <= cutoff


async def main(args):
    prepare_state_schema()
    if not PREFECT_API_URL.value():
        raise RuntimeError("Configure PREFECT_API_URL con la API del orquestador.")
    cutoff = datetime.fromisoformat(args.before) if args.before else datetime.now(timezone.utc)
    if cutoff.tzinfo is None:
        raise ValueError("--before requiere zona horaria, por ejemplo 2026-09-14T12:00:00-03:00")
    filters = dict(
        work_pool_filter=WorkPoolFilter(name={"any_": [args.pool]}),
        flow_run_filter=FlowRunFilter(
            state={"name": {"any_": ["Scheduled", "Late", "Pending"]}},
        ),
    )
    async with get_client() as client:
        await client.read_work_pool(args.pool)  # Fail if the pool does not exist.
        runs = {}
        offset = 0
        # Finish discovery before mutations, so cancellation cannot shift pages.
        while True:
            page = await client.read_flow_runs(
                **filters, limit=200, offset=offset, sort=FlowRunSort.ID_DESC
            )
            if not page:
                break
            for run in page:
                if eligible(run, cutoff, args.include_future):
                    runs[run.id] = run
            offset += len(page)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        report = Path(f"prefect_backlog_{stamp}.jsonl")
        with report.open("x", encoding="utf-8") as audit:
            def record(data):
                audit.write(json.dumps(data, default=str) + "\n")
                audit.flush()

            record({"pool": args.pool, "cutoff": cutoff, "apply": args.apply})
            for run in runs.values():
                record({"candidate": run.id, "name": run.name, "state": run.state.name,
                        "queue": run.work_queue_name, "expected_start_time": run.expected_start_time})
            print(f"Pool: {args.pool}; corte: {cutoff.isoformat()}; candidatos: {len(runs)}")
            print(dict(Counter((r.work_queue_name, r.state.name) for r in runs.values())))
            print(f"Auditoria: {report.resolve()}")
            if not args.apply:
                print("Vista previa. Agregue --apply para cancelar. Mantenga los workers detenidos.")
                return
            counts = Counter()
            for index, run in enumerate(runs.values(), 1):
                try:
                    # Revalidate pool membership and state immediately before writing.
                    current = await client.read_flow_runs(
                        work_pool_filter=filters["work_pool_filter"],
                        flow_run_filter=FlowRunFilter(id={"any_": [run.id]}), limit=1,
                    )
                    if not current or not eligible(current[0], cutoff, args.include_future):
                        outcome = "skipped"
                    else:
                        await client.set_flow_run_state(
                            run.id, Cancelled(message="Limpieza de backlog por caida DMZ Diarco"),
                            force=True,
                        )
                        verified = await client.read_flow_run(run.id)
                        if not verified.state or not verified.state.is_cancelled():
                            raise RuntimeError("La API no confirmo el estado Cancelled")
                        outcome = "cancelled"
                    counts[outcome] += 1
                    record({"id": run.id, "outcome": outcome})
                except Exception as exc:
                    record({"id": run.id, "outcome": "error", "error": str(exc)})
                    raise  # Stop on API errors instead of flooding an unhealthy server.
                if index % 100 == 0 or index == len(runs):
                    print(f"{index}/{len(runs)}: {dict(counts)}", flush=True)
                await asyncio.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool", default="dmz-diarco")
    parser.add_argument("--before", help="Fecha de corte ISO con zona; por defecto ahora")
    parser.add_argument("--include-future", action="store_true", help="Incluir Scheduled futuros ya creados")
    parser.add_argument("--apply", action="store_true", help="Ejecutar cancelacion; requiere workers detenidos")
    asyncio.run(main(parser.parse_args()))
