from __future__ import annotations

from datetime import datetime

import requests
from prefect import get_run_logger

from IOSdb.config.settings import load_settings


def notify_discord(message: str) -> None:
    logger = get_run_logger()
    webhook = load_settings().runtime.discord_webhook

    if not webhook:
        logger.info("[Notify] Webhook de Discord no configurado, se omite notificacion.")
        return

    try:
        requests.post(webhook, json={"content": message}, timeout=10)
    except Exception as exc:
        logger.warning("[Notify] Error al notificar a Discord: %s", exc)


def build_summary_message(title: str, metrics: dict[str, int]) -> str:
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")
    parts = [f"{key}: {value}" for key, value in metrics.items()]
    return f"{title} - {timestamp}\n" + " | ".join(parts)
