# Recuperación de data-sync

## Estado y evidencia

La referencia de esta guía es la [captura completa del 11/09/2026](capturas/20260911T144448547950Z/RESUMEN.json). Se documentó el servidor reconstruido; no hay evidencia suficiente para comparar su contenido con el servidor anterior a la caída.

El objetivo de este paquete es conservar la estructura y la lógica visibles. **No sustituye un backup recuperable**, ni contiene los datos para reconstruir las aproximadamente 453 millones de filas estimadas por el catálogo. Ese volumen es una estimación de particiones, no un recuento transaccional validado.

## Pendientes concretos

| Hallazgo | Acción pendiente |
| --- | --- |
| No hay historial de backups de data-sync en msdb | Localizar los archivos o la solución de respaldo externa y registrar ubicación, fecha, responsable y retención. La ausencia de historial no prueba ausencia de copias. |
| CDC habilitado, pero sin instancias de captura | Confirmar qué tablas deben usar CDC y si la reconstrucción dejó su configuración incompleta. No habilitar tablas por inferencia. |
| Jobs cdc.data-sync_capture y cdc.data-sync_cleanup presentes | Verificar su relación con el estado actual de CDC, ejecución e historial. La captura conserva configuración, no certifica funcionamiento. |
| SQL_DRIVER=17 y Driver 18 instalado localmente | Alinear instalaciones y configuración en el servidor de ejecución, después de probar los procesos ETL. No se modificó el entorno actual. |
| Cuenta de captura con sysadmin | La visibilidad fue amplia. Definir una cuenta con permisos de lectura de metadatos adecuados para futuras capturas; no se requieren escrituras para este inventario. |
| 8 linked servers | Conservar por canal seguro mapeos de autenticación y configuración completa de proveedores. No se exportan contraseñas ni provider_string. |
| 10 jobs inventariados | Exportar sus scripts completos con revisión de secretos por el DBA; aquí sólo se documentan nombres, pasos sin comandos y horarios. |

Responsable de recuperación, RPO (pérdida máxima aceptable), RTO (tiempo objetivo), ubicación de backups y fecha de último simulacro: **pendientes de definición por el equipo**.

## Ruta preferida: recuperar una copia de seguridad

1. Identificar la última copia utilizable y la cadena de copias aplicable. Conservar los originales en almacenamiento independiente del servidor. Registrar tamaño, fecha y hash de los archivos.
2. Preparar una instancia de laboratorio compatible. Comparar versión, collation, configuración, rutas y espacio contra `ENTORNO.md` y los JSON de configuración. Mantener aislados los jobs y las salidas hacia sistemas externos.
3. Inspeccionar el backup con `RESTORE HEADERONLY` y `RESTORE FILELISTONLY`. Ejecutar `RESTORE VERIFYONLY` y realizar una restauración real en laboratorio; VERIFYONLY no verifica por completo la estructura interna de los datos. [Referencia Microsoft](https://learn.microsoft.com/en-us/sql/t-sql/statements/restore-statements-verifyonly-transact-sql?view=sql-server-ver17).
4. Restaurar hacia rutas de laboratorio elegidas explícitamente, sin sobrescribir producción. Registrar el resultado y realizar controles de integridad mediante `DBCC CHECKDB` en la base restaurada.
5. Recuperar las dependencias de instancia: logins y SID, roles, jobs, proveedores, linked servers, credenciales y accesos de servicio. Al restaurar en otra instancia puede ser necesario recrear logins y jobs. [Referencia Microsoft](https://learn.microsoft.com/en-us/sql/relational-databases/databases/copy-databases-with-backup-and-restore?view=sql-server-ver17).
6. Comparar objetos y definiciones con esta captura, probar accesos y validar muestras y totales de negocio con los responsables. Revisar expresamente el estado de CDC.
7. Probar los flujos del proyecto siguiendo los runbooks existentes. Habilitar sus programaciones únicamente después de validar precedencias y destinos. Registrar duración del ensayo y pérdidas respecto de la fecha objetivo.

No se incluyen rutas ficticias ni un comando RESTORE listo para ejecutar: los archivos disponibles y el destino de recuperación no fueron identificados durante esta documentación.

## Ruta alternativa: reconstrucción sin backup

Esta ruta requiere recuperar o volver a cargar los datos desde sus orígenes y puede perder información que ya no esté disponible allí. El paquete no permite una reconstrucción automática completa.

1. Crear una base vacía de laboratorio con opciones revisadas contra la captura y crear sus esquemas.
2. Revisar `sql/TABLAS_BORRADOR.sql`. Contiene columnas, identidades, columnas calculadas y defaults de las 185 tablas; **omite claves de tabla, índices, filegroups, compresión, particionamiento, CDC y seguridad**. Su ejecución no fue probada.
3. Reconstruir las 50 PK, la restricción única y los índices desde `claves.json`, `indices.json` y `columnas_indices.json`. Preservar orden, dirección, columnas incluidas y filtros; atender especialmente el índice clustered columnstore. Contrastar particiones y almacenamiento con sus JSON. No agregar FKs por suposición: no se encontraron declaradas.
4. Revisar dependencias y crear vistas, funciones y procedimientos en el orden requerido. Los 138 archivos de `sql/modulos` conservan la definición del catálogo, que puede empezar con CREATE o ALTER; no forman una migración ordenada. Cualquier `[REDACTADO]` exige intervención manual.
5. Restaurar permisos y usuarios con sus SID, dependencias externas y jobs desde las fuentes administrativas correspondientes. Sus credenciales se recuperan desde almacenamiento seguro, no desde este repositorio.
6. Definir y probar la recarga de cada tabla; las identidades y los controles de incrementalidad deben quedar consistentes con los datos recuperados. Revisar los flujos de replicación y el runbook de CDC antes de reiniciarlos.
7. Aplicar las mismas verificaciones técnicas y funcionales de la ruta de backup.

Para obtener un DDL integral, complementar con una exportación de esquema mediante SSMS/SMO revisada por el DBA y ensayada en una base vacía. Este trabajo no ejecutó ninguna restauración, DDL, backup ni modificación de configuración.

## Conservación y mantenimiento

Guardar esta carpeta en el repositorio privado y en una copia externa al host de SQL Server. Guardar backups, certificados y secretos mediante el mecanismo administrativo apropiado, separados de la documentación. La copia local por sí sola no protege frente a otra pérdida del equipo.

Actualizar la captura después de cambios de esquema, procedimientos, jobs o infraestructura. Verificar las huellas SHA-256 para detectar alteraciones accidentales de los archivos; no constituyen una firma autenticada. Repetir el simulacro de restauración según los objetivos que acuerde el equipo y conservar sus resultados.
