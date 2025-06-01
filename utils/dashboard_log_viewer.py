
import pandas as pd
import os

def visualizar_dashboard(log_file='logs/events_log.csv'):
    if not os.path.exists(log_file):
        print("\nNo se encontró el archivo de logs.")
        return

    # Leer el archivo de logs
    df = pd.read_csv(log_file, names=["timestamp", "event_type", "message"])

    # Estadísticas básicas
    print("\n--- Resumen de Eventos ---")
    print(df['event_type'].value_counts())

    # Últimos eventos
    print("\n--- Últimos 10 eventos registrados ---")
    print(df.tail(10).to_string(index=False))

if __name__ == "__main__":
    visualizar_dashboard()
