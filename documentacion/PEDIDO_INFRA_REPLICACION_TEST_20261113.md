# Pedido a Infraestructura: recreación de datos y coordinación de cargas masivas

**Asunto:** Incidente TEST — la sincronización entre esquemas vuelve a crear relaciones inválidas y bloquea la actualización diaria de maestros

**Destinatarios sugeridos:** Infraestructura / Operaciones, con participación del equipo responsable de Inventory y de los consumidores de eventos.

## Texto para enviar

Solicito revisar el circuito de replicación y sincronización de maestros de TEST. Detectamos que registros corregidos o eliminados vuelven a aparecer en `inventory.inv_product_supplier`, incluso después de darles de baja por la API. Esto impide completar `maestros-diario-test-prod` y hace que las correcciones manuales no queden estables.

El flujo de Prefect se ejecuta en producción (`186.158.182.127`, pool `cnx-sync-prod`, worker `cnx-diarco-prod`), pero procesa TEST y PROD. El error actual está en los datos de TEST (`186.158.182.223`, base `connexa_platform_test`). Al fallar la planificación de TEST, la ejecución no llega a aplicar los maestros de ninguno de los dos destinos.

**Evidencia del incidente del 13/11/2026:**

- Dos relaciones del producto de prueba SKU `22142215`, sin `ext_code`, vinculadas a proveedores `12471` y `5424`, reaparecen activas después de corregirlas. Las filas muestran `updated_by=inventory-service` y nuevos timestamps.
- La API de TEST aceptó su baja con HTTP 200 y `active=false`; posteriormente se observaron nuevamente con `active=true`.
- Las escrituras observadas son INSERT/UPDATE de una conexión Java/JDBC local a la base de TEST. Durante la comprobación no había ejecuciones activas de maestros en los pools CNX DEV y PROD.
- Existen copias de las relaciones en otros esquemas y seis slots lógicos activos. La tabla `inventory.inv_product_supplier` está incluida en `inventory_publication`.
- En TEST están instalados Debezium Connect 2.7 y Kafka 7.7.7. Inventory y otros componentes funcionan como servicios systemd del usuario `connexa`, UID `1002`, entre ellos `connexa-inventory.service` y `connexa-platform-replication-event.service`.
- La última ejecución diaria exitosa consultada fue el 06/11; las siete ejecuciones del 07/11 al 13/11 fallaron. Los errores variaron entre relaciones duplicadas y productos sin código. No está confirmado que la causa sea un cambio reciente de infraestructura.

**Solicitamos:**

1. Identificar el productor, tópico, partición, consumer group y servicio que reintroducen estos UUID. Entregar el recorrido del evento entre esquemas y la fuente autoritativa de cada entidad/campo.
2. Revisar con Desarrollo posibles bucles de realimentación, reprocesamiento de mensajes antiguos, snapshots, reinicios o cambios de consumer group/offsets, conectores duplicados y tratamiento de bajas y del campo `active`. Confirmar por qué una baja aceptada vuelve como alta activa.
3. Comparar versiones, despliegues, migraciones y configuraciones modificadas desde el 06/11. TEST y PROD tienen diferencias de esquema verificadas; se necesita confirmar si son previstas y compatibles con las versiones desplegadas.
4. Coordinar una intervención sobre los componentes responsables para completar la limpieza respaldada y comprobar que persiste después de reanudar el procesamiento. Detener únicamente un worker de Prefect no resolvería las escrituras observadas.
5. Definir un procedimiento para las cargas masivas de madrugada: dependencias y orden de ejecución, ventana acordada, tratamiento de eventos pendientes, pausas selectivas si hacen falta y condiciones de reanudación. La operación normal puede ser continua; necesitamos que la replicación respete las cargas y correcciones, no un apagado general de Kafka por horario.
6. Incorporar monitoreo de lag, reintentos, errores, recreación de registros, bloqueos PostgreSQL y retención de WAL; establecer responsable y alertas para las cargas nocturnas.

**Criterios de resolución:** las relaciones inválidas no reaparecen; una baja no vuelve a activarse por un evento anterior; no se generan claves artículo/proveedor duplicadas; se completa una carga masiva con sus consumidores recuperados y datos consistentes; la validación previa de maestros queda sin errores y se coordina la siguiente ejecución del deployment TEST+PROD.

Solicitamos un responsable de Infraestructura y otro del servicio/replicación, diagnóstico de causa, plan de corrección y ventana propuesta para intervenir y validar.

## Anexo técnico

### Identificadores para trazar mensajes

| Entidad | Identificador / valor |
| --- | --- |
| Relación producto/proveedor 12471 | `680d2ff7-8335-4c69-a41c-0fa7733e45ed` |
| Relación producto/proveedor 5424 | `52d4a510-e75d-407a-8075-5f63920f2e59` |
| Producto TEST sin código | `8ac90f23-e46e-44ad-a005-9bfd021e46c9` |
| SKU del producto TEST | `22142215` |
| Otra relación TEST sin equivalente PROD, artículo 40/proveedor 4647 | `0b0381c1-001d-40db-b641-11947714a667` |
| Run fallido del 13/11 | `01a1c501-fe5b-7f78-9e08-bedae6e5277d` |
| Deployment | `a58d2f07-d8db-4b4f-9726-19ec0fa48895` |

[Ejecución fallida en Prefect](https://orquestador.connexa-cloud.com/runs/flow-run/01a1c501-fe5b-7f78-9e08-bedae6e5277d).

### Componentes observados en TEST

Servicios de usuario systemd, no unidades del sistema:

- `connexa-inventory.service`
- `connexa-platform-replication-event.service`
- `connexa-procurement-and-sourcing.service`
- `connexa-supply-planning.service`
- `connexa-stock-management.service`
- `connexa-cost-management.service` y `connexa-cost-management-event.service`

Contenedores informados por el usuario:

- `connect`: `quay.io/debezium/connect:2.7`
- `kafka`: `confluentinc/cp-kafka:7.7.7`
- `zookeeper`: `confluentinc/cp-zookeeper:7.7.7`

La presencia de un servicio o contenedor no demuestra que sea el responsable del evento incorrecto. Falta correlacionar UUID, logs y mensajes Kafka antes de decidir qué componente intervenir.

### Captura PostgreSQL del 13/11, 12:28 Argentina (15:28 UTC)

La consulta de solo lectura confirmó estas publicaciones sobre tablas relevantes:

| Publicación | Tabla |
| --- | --- |
| `inventory_publication` | `inventory.inv_product_supplier` |
| `inventory_publication` | `inventory.inv_barcode` |
| `procurement_and_sourcing_publication` | `procurement_and_sourcing.pas_product_supplier_replicator` |

Slots activos de `connexa_platform_test`, todos con plugin `pgoutput`:

- `authentication_replication_slot`
- `procurement_replication_slot`
- `foundation_replication_slot`
- `stock_management_replication_slot`
- `procurement_and_sourcing_replication_slot`
- `inventory_replication_slot`

En esa captura, la distancia entre WAL actual y `restart_lsn` era de aproximadamente 15–17 MB por slot. Es una medición puntual, no prueba de ausencia de lag en Kafka. Esos valores no deben sumarse como almacenamiento independiente: los slots pueden retener segmentos compartidos.

Confirmar el propósito de los dos slots con nombre procurement y su correspondencia con conectores/publicaciones. Sus nombres por sí solos no demuestran duplicación.

### Diferencias de esquema verificadas

TEST contiene columnas que no están en PROD:

| Tabla | Columnas adicionales en TEST |
| --- | --- |
| `inventory.inv_barcode` | `created_by`, `updated_at`, `updated_by` |
| `inventory.inv_product_supplier` | `created_by`, `updated_at`, `updated_by` |
| `inventory.inv_product` | `active`, `buyer_code`, `buyer_name`, `created_by`, `updated_at`, `updated_by` |

Esto demuestra diferencias de esquema; no demuestra cuándo se introdujeron ni que sean la causa. Solicitar historial de versiones/migraciones y compatibilidad de DTO/eventos entre servicios.

### Operación nocturna: decisiones necesarias

La replicación CDC está diseñada para capturar cambios de manera continua. Mantenerla activa durante cargas masivas puede ser correcto si hay capacidad, consistencia y propiedad de datos definidas. El incidente observado requiere resolver quién vuelve a escribir y con qué versión del dato.

Si hace falta una ventana de mantenimiento, distinguir tres niveles: captura PostgreSQL→Kafka, consumidores que escriben en los esquemas y procesos de carga/lectura dependientes. Pausar la captura no detiene necesariamente a consumidores que ya tienen mensajes pendientes. Reanudar consumidores sin resolver el orden o la autoridad de los eventos puede volver a introducir los registros corregidos; esto debe probarse en TEST.

Kafka Connect ofrece pausa/reanudación por conector; la pausa es asíncrona y exige comprobar el estado de sus tareas. Elegir el componente concreto y verificarlo antes de la carga. [Documentación Confluent 7.7](https://docs.confluent.io/platform/7.7/connect/references/restapi.html).

Si se pausa un conector PostgreSQL, supervisar la retención de WAL y preservar el punto de recuperación. Una pausa no descarta el historial pendiente. No usar eliminación de tópicos, offsets o slots como limpieza rutinaria; cualquier reconstrucción requiere un plan explícito de consistencia y recuperación. [Conector PostgreSQL de Debezium 2.7](https://debezium.io/documentation/reference/2.7/connectors/postgresql.html).

Revisar también cómo se transforman y consumen los eventos de borrado y tombstones: la configuración de las transformaciones puede cambiar su representación o descartarlos. No se obtuvo la configuración de los conectores de esta instalación, por lo que esto es un punto de investigación, no una causa confirmada. [Transformación de eventos de Debezium 2.7](https://debezium.io/documentation/reference/2.7/transformations/event-flattening.html).

### Evidencia y alcance de la intervención

- Diagnóstico y cronología: `documentacion/MAESTROS_DIARIO_ERROR_20261113.md`.
- Captura de replicación y datos: `data/repairs/infra_replicacion_test_20261113_evidencia.json`.
- Bajas API aceptadas: `data/repairs/maestros_test_api_deactivate_20261113.json`.
- Respaldos previos a las limpiezas: `data/repairs/maestros_test_*.json`.
- Restauración opcional, no ejecutada: `data/repairs/maestros_test_20261113_restore.sql`.

La intervención modificó únicamente registros diagnosticados de TEST y sus copias locales respaldadas. PROD se consultó en modo lectura. No se detuvieron servicios, conectores o Kafka desde esta intervención. El pedido a Infraestructura es un borrador: no fue enviado.
