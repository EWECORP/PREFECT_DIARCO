# Proyecto ETL_DIARCO

Este proyecto gestiona los flujos de integración y sincronización de datos entre los sistemas legados de DIARCO, la base de datos PostgreSQL `diarco_data` y la plataforma CONNEXA, utilizando Prefect para la orquestación de procesos.

---

## 📁 Estructura del Proyecto

```
ETL_DIARCO/
│
├── venv/                      # Entorno virtual Python
├── logs/                     # Logs de ejecución de flujos ETL
├── scripts/
│   ├── push/                 # Flujos de carga hacia PostgreSQL / Connexa
│   ├── pull/                 # Flujos de consulta a Connexa (pull)
│   ├── transforms/           # Transformaciones de datos intermedias
│   ├── utils/                # Funciones auxiliares y loggers
│
├── config/
│   ├── .env.example          # Variables de entorno para conexión
│   └── estructura_repl_logs.sql  # SQL con definición de esquemas base
│
├── jobs/                     # Flujos Prefect organizados por tipo
│   ├── flujo_push_ejemplo.py
│   ├── flujo_con_log_prefect.py
│   └── cronograma_flujos.py
│
├── install/
│   ├── install_worker_service.bat   # Script para iniciar worker como servicio
│   └── run_flow_manual.bat          # Ejecutar flujo desde consola
│
├── dags/                     # (Opcional) estructura DAG para ejecución compleja
├── state/                    # Persistencia del estado de sincronización
└── README.md                 # Este archivo
```

---

## ⚙️ Componentes Destacados

### 🔁 Replicación
- Utiliza un esquema `repl` en `data_sync` para staging de datos extraídos desde SQL Server (2008/2016/2022).
- Compatible con mecanismos como CDC o extracciones masivas.

### 📦 Destino
- Los datos procesados se cargan en `diarco_data` (PostgreSQL) para su uso por CONNEXA o análisis en Metabase.

### 🧩 Prefect
- Orquestación de flujos con `prefect==3.4.1`
- Worker asignado: `dmz-diarco`
- Flujos programados y ad-hoc
- Logging automático en la tabla `logs.procesos_etl`

### 🧾 Logging
- Logger rotativo (`logs/etl.log`)
- Registro detallado de cada ejecución: flujo, tarea, tiempo, estado y errores.

---

## 🚀 Ejecución

### Crear entorno virtual
```bash
cd D:\Services\ETL_DIARCO
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Ejecutar flujo manual
```bash
install\run_flow_manual.bat
```

### Registrar flujos programados
```bash
python jobs\cronograma_flujos.py
```

### Iniciar worker Prefect
```bash
install\install_worker_service.bat
```

---

## 📈 Futuro
- Migración a SQL 2016/2022 para habilitar CDC
- Construcción de data warehouse Connexa en PostgreSQL
- Automatización completa de flujos nocturnos y recurrentes

