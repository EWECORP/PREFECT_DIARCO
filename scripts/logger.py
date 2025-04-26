
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
