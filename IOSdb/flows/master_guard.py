from __future__ import annotations

import os


def ensure_master_writes_enabled() -> None:
    enabled = os.getenv("IOSDB_MASTER_WRITES_ENABLED", "false").strip().lower()
    if enabled != "true":
        raise RuntimeError(
            "Las publicaciones legacy de maestros IOSdb están suspendidas. "
            "IOSDB_MASTER_WRITES_ENABLED=true queda reservado para una contingencia aprobada."
        )
