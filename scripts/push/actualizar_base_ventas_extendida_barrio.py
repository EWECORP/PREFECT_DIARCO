# Wrapper legado: la lógica de Barrio quedó integrada en el flujo canónico.
import sys

from prefect import flow

from actualizar_base_ventas_extendida import actualizar_base_ventas_extendida, logger


@flow(name="actualizar_base_ventas_extendida_dbarrio_legacy")
def actualizar_base_ventas_extendida_barrio(
    window_days: int = 14,
    analyze: bool = True,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    modo_reproceso: str | None = None,
):
    logger.warning(
        "actualizar_base_ventas_extendida_barrio.py quedó obsoleto: "
        "la cobertura de Barrio ya está integrada en actualizar_base_ventas_extendida.py."
    )
    return actualizar_base_ventas_extendida(
        window_days=window_days,
        analyze=analyze,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        modo_reproceso=modo_reproceso,
    )


if __name__ == "__main__":
    wdays = int(sys.argv[1]) if len(sys.argv) >= 2 else 14
    do_analyze = True
    if len(sys.argv) >= 3:
        arg = sys.argv[2].strip().lower()
        do_analyze = arg in ("true", "1", "yes", "y", "t")
    start_date = sys.argv[3] if len(sys.argv) >= 4 else None
    end_date = sys.argv[4] if len(sys.argv) >= 5 else None
    reprocess_mode = sys.argv[5] if len(sys.argv) >= 6 else None

    actualizar_base_ventas_extendida_barrio(
        window_days=wdays,
        analyze=do_analyze,
        fecha_desde=start_date,
        fecha_hasta=end_date,
        modo_reproceso=reprocess_mode,
    )
