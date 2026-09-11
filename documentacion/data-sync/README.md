# Documentación de SQL Server · data-sync

Captura directa realizada el **11 de septiembre de 2026**, mediante consultas de metadatos, sin modificar SQL Server ni extraer filas de negocio. Proyecto confirmado: `C:\PROYECTOS\ETL\ETL_DIARCO`. La conexión corresponde a `SQL_DATABASE` del `.env`, no a las conexiones `SQLT`, `SQLE` o PostgreSQL.

## Documentos principales

La captura completa de referencia es [20260911T144448547950Z](capturas/20260911T144448547950Z/RESUMEN.json), con **cero errores de consulta y cero módulos sin definición**.

- [Diccionario de tablas y vistas](capturas/20260911T144448547950Z/DICCIONARIO.md): columnas, tipos, nulabilidad, identidades, valores predeterminados e índices.
- [Inventario completo](capturas/20260911T144448547950Z/INVENTARIO.md): objetos, esquemas y fechas.
- [Entorno observado](capturas/20260911T144448547950Z/ENTORNO.md): versión, archivos, opciones, servidores vinculados, jobs y horarios.
- [Definiciones SQL de los 138 módulos](capturas/20260911T144448547950Z/sql/modulos): 109 procedimientos, 26 vistas y 3 funciones.
- [Borrador SQL de tablas](capturas/20260911T144448547950Z/sql/TABLAS_BORRADOR.sql): apoyo para reconstrucción manual, no instalador completo.
- [Metadatos JSON](capturas/20260911T144448547950Z/metadatos): detalle de claves e índices, dependencias, seguridad, configuración, CDC, particiones y volúmenes estimados.
- [Guía de recuperación](GUIA_RECUPERACION.md): procedimiento, dependencias y pendientes.
- [Consultas ejecutadas](capturas/20260911T144448547950Z/consultas.sql) y [huellas SHA-256](capturas/20260911T144448547950Z/SHA256.json).

La captura anterior `20260911T144325628737Z` se conserva como historial de elaboración: tuvo un problema de conversión ODBC al leer configuración de instancia y no incluye las ampliaciones finales. Usar la captura completa indicada arriba.

## Resumen observado

| Concepto | Resultado |
| --- | --- |
| Servidor | DCO-DIARCOCI-T0 · 10.54.200.92 |
| Motor | 13.0.4001.0 · Standard Edition (64-bit) |
| Compatibilidad | 130 |
| Collation | SQL_Latin1_General_CP1_CI_AS |
| Estado / recuperación | ONLINE / SIMPLE |
| Tablas / vistas | 185 / 26 |
| Procedimientos / funciones | 109 / 3 |
| Claves primarias / únicas | 50 / 1 |
| Foreign keys / checks declarados | 0 / 0 |
| Estructuras de almacenamiento | 128 heaps, 56 índices clustered, 27 nonclustered, 1 clustered columnstore |
| Dependencias registradas | 549 entradas; no equivalen a 549 relaciones únicas |
| Linked servers | 8 externos; sys.servers también devuelve la instancia local |
| SQL Agent | 10 jobs de instancia, 28 pasos y 9 asociaciones a horarios |
| CDC | Habilitado en base, 0 instancias de captura en cdc.change_tables |
| Historial de backups | 0 registros de esta base en msdb al consultar |

Los jobs inventariados son de toda la instancia; no todos pertenecen a data-sync. Las 4.335 columnas capturadas incluyen los objetos visibles del catálogo, no solamente las tablas.

## Dependencias para operar después de una reconstrucción

El catálogo registra referencias a `DIARCOP001` / `DiarcoP`, `DCO-DBCORE-P02` / `DiarcoEst`, `DIARCO-BARRIO` / `DiarcoBarrio` y `DIARCO-VKMSQL\SQL2008R2` / `VALKIMIA`. Revisar el [detalle de dependencias](capturas/20260911T144448547950Z/metadatos/dependencias.json); las referencias construidas mediante SQL dinámico requieren revisar los módulos. Los nombres anteriores provienen del catálogo, no de pruebas de disponibilidad de esos destinos.

La base participa en las extracciones y sincronizaciones SQL Server → PostgreSQL del proyecto. Para su operación consultar también [README del proyecto](../../README.md), [orquestación productiva](../../ORQUESTACION_PRODUCTIVA_ETL_DIARCO.md), [runbook PDD](../../PDD_SOURCE_SYNC_RUNBOOK.md) y [CDC](../../cdc/README.md). Esta captura no certifica el estado operativo de esos flujos.

## Actualizar la documentación

Desde la raíz del proyecto:

```powershell
.\.venv\Scripts\python.exe .\documentacion\data-sync\generar_documentacion.py --driver 'ODBC Driver 18 for SQL Server'
```

Requiere `pyodbc`, `python-dotenv`, acceso de red y permisos para leer los catálogos. El `.env` indica Driver 17, pero este equipo tiene Driver 18: se usó el parámetro sin modificar el `.env`. El script usa cifrado y `TrustServerCertificate=yes` para esta conexión interna; la validación de identidad mediante certificado no queda habilitada con esa opción.

Cada ejecución crea una carpeta UTC nueva y conserva las anteriores. Revisar `RESUMEN.json`, actualizar los enlaces de referencia de este README y guardar la documentación fuera del servidor reconstruido. Los resultados corresponden al momento de lectura y pueden variar durante despliegues concurrentes.

## Alcance y protección

No se copiaron el `.env`, contraseñas de logins, hashes de autenticación ni filas de negocio. El generador reemplaza valores sensibles conocidos del `.env` y patrones de asignación de secretos en los textos exportados; no garantiza detectar toda credencial incrustada con formatos arbitrarios. Los comandos de pasos de jobs y las cadenas privadas de proveedores se omiten deliberadamente. Revisar los SQL antes de compartir fuera del equipo; contienen lógica y nombres internos.

La documentación es técnica: no inventa el significado de columnas sin descripciones en la base. Las propiedades extendidas existentes están en `metadatos/propiedades.json`. No se probó una reconstrucción ni se creó un backup. Los scripts SQL exportados requieren revisión de dependencias, redacciones y contexto antes de ejecutarse.
