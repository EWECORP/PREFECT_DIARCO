import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logger(name, log_file="logs/etl.log", level=logging.INFO, max_bytes=5_000_000, backup_count=3):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    return logger

import csv
import os
from datetime import datetime

def log_event(event_type, message, log_file='logs/events_log.csv'):
    """Función para registrar eventos en un archivo CSV"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    timestamp = datetime.now().isoformat()
    with open(log_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, event_type, message])