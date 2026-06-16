# Wrapper de compatibilidad para la versión canónica.
import sys

from actualizar_base_ventas_extendida import actualizar_base_ventas_extendida, logger


if __name__ == "__main__":
    wdays = int(sys.argv[1]) if len(sys.argv) >= 2 else 14
    do_analyze = True
    if len(sys.argv) >= 3:
        arg = sys.argv[2].strip().lower()
        do_analyze = arg in ("true", "1", "yes", "y", "t")
    start_date = sys.argv[3] if len(sys.argv) >= 4 else None
    end_date = sys.argv[4] if len(sys.argv) >= 5 else None
    reprocess_mode = sys.argv[5] if len(sys.argv) >= 6 else None

    logger.warning(
        "actualizar_base_ventas_extendida_corregido.py quedó como wrapper de compatibilidad. "
        "Usar actualizar_base_ventas_extendida.py como entrada canónica."
    )
    actualizar_base_ventas_extendida(
        window_days=wdays,
        analyze=do_analyze,
        fecha_desde=start_date,
        fecha_hasta=end_date,
        modo_reproceso=reprocess_mode,
    )
