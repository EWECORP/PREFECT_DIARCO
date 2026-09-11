# Entorno observado

## servidor

| servidor | version | edition | collation |
| --- | --- | --- | --- |
| DCO-DIARCOCI-T0 | 13.0.4001.0 | Standard Edition (64-bit) | SQL_Latin1_General_CP1_CI_AS |


## base

| name | compatibility_level | collation_name | recovery_model_desc | state_desc | containment_desc | page_verify_option_desc | is_read_committed_snapshot_on | snapshot_isolation_state_desc | is_auto_close_on | is_auto_shrink_on | is_cdc_enabled | is_broker_enabled | is_trustworthy_on |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| data-sync | 130 | SQL_Latin1_General_CP1_CI_AS | SIMPLE | ONLINE | NONE | CHECKSUM | False | OFF | False | False | True | False | False |


## archivos

| name | type_desc | physical_name | size | growth | is_percent_growth | max_size |
| --- | --- | --- | --- | --- | --- | --- |
| data-sync | ROWS | D:\SQL Server\DATA\data-sync.mdf | 11006976 | 12800 | False | -1 |
| data-sync_log | LOG | D:\SQL Server\LOG\data-sync_log.ldf | 2147328 | 8192 | False | 268435456 |


## permisos_captura

| view_definition | sysadmin |
| --- | --- |
| 1 | 1 |


## linked_servers

| name | product | provider | data_source | catalog | is_linked | is_data_access_enabled | is_rpc_out_enabled |
| --- | --- | --- | --- | --- | --- | --- | --- |
| DCO-DIARCOCI-T0 | SQL Server | SQLNCLI | DCO-DIARCOCI-T0 |  | False | True | True |
| DIARCOP001 | SQL Server | SQLNCLI | DIARCOP001 |  | True | True | True |
| DIARCO-BARRIO | SQL Server | SQLNCLI | DIARCO-BARRIO |  | True | True | False |
| DIARCOP001-BK | SQL Server | SQLNCLI | DIARCOP001-BK |  | True | True | False |
| DCO-DBCORE-P02 | SQL Server | SQLNCLI | DCO-DBCORE-P02 |  | True | True | False |
| DIARCO-VKMSQL\SQL2008R2 | SQL Server | SQLNCLI | DIARCO-VKMSQL\SQL2008R2 |  | True | True | True |
| 10.54.200.88 | SQL Server | SQLNCLI | 10.54.200.88 |  | True | True | True |
| DIARCO_PROD_RPC |  | SQLNCLI |  |  | True | True | True |
| 10.10.41.13 | SQL Server | SQLNCLI | 10.10.41.13 |  | True | True | False |


## jobs

| job_id | name | enabled | description | date_created | date_modified |
| --- | --- | --- | --- | --- | --- |
| A90D5220-AB22-4AFD-90F0-25300EEC92F7 | ETL_T080 | 0 | No description available. | 2025-07-03 17:46:36.860000 | 2025-07-08 12:23:45.093000 |
| EE5AD048-89E5-41C5-97A2-5BE9649F5F8C | datasync-tablas-maestras | 1 | Ejecución SP heredados de parámetrosl | 2025-07-31 12:17:24.240000 | 2025-07-31 12:44:11.213000 |
| B8D54ACB-B3AA-42B5-807E-764F8236F3A6 | datasync-oc-recupero | 0 | No description available. | 2025-01-31 09:45:44.317000 | 2025-08-19 11:49:29.853000 |
| 78F049AC-2101-4F79-B05C-94A3005DF8A5 | ZEETREX_DATOS_DIARIOS | 0 | No description available. | 2024-09-11 11:32:00.327000 | 2024-11-06 15:08:49.690000 |
| 0DB73BEE-EF05-49DE-B3A7-995F14932512 | DBA-Rebuild Index.Subplan_1 | 1 | No description available. | 2026-06-01 21:28:16.430000 | 2026-06-01 21:28:17.170000 |
| 196140A4-ECB3-4C2B-982E-A01616E9C95D | cdc.data-sync_cleanup | 0 | CDC Cleanup Job | 2025-05-27 10:52:50.813000 | 2025-06-12 21:07:35.303000 |
| BBB7A7EC-4EFA-4894-951D-B616DBF9CC52 | datasync-oc | 0 | No description available. | 2025-01-24 12:38:12.343000 | 2025-08-19 11:50:06.213000 |
| C99885B7-9418-44DC-A459-BD7209DADED2 | cdc.data-sync_capture | 0 | CDC Log Scan Job | 2025-05-27 10:52:46.813000 | 2025-06-12 21:07:32.867000 |
| 263678EE-C1CD-46B4-9531-DAF84493B16A | datasync-stock | 0 | No description available. | 2025-01-20 15:55:38.143000 | 2025-08-19 11:47:16.047000 |
| 455D39FC-F4EB-40BA-8C76-FA8F9491A1BA | syspolicy_purge_history | 1 | No description available. | 2022-06-21 10:39:12.397000 | 2022-06-21 10:39:13.617000 |


## horarios_jobs

| job_id | name | enabled | freq_type | freq_interval | freq_subday_type | freq_subday_interval | freq_relative_interval | freq_recurrence_factor | active_start_date | active_end_date | active_start_time | active_end_time |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 455D39FC-F4EB-40BA-8C76-FA8F9491A1BA | syspolicy_purge_history_schedule | 1 | 4 | 1 | 1 | 0 | 0 | 0 | 20080101 | 99991231 | 20000 | 235959 |
| 78F049AC-2101-4F79-B05C-94A3005DF8A5 | NOCTURNO Tardio | 1 | 4 | 1 | 1 | 0 | 0 | 0 | 20240912 | 99991231 | 43000 | 235959 |
| BBB7A7EC-4EFA-4894-951D-B616DBF9CC52 | Daily 04 | 1 | 4 | 1 | 1 | 0 | 0 | 0 | 20250124 | 99991231 | 40000 | 235959 |
| B8D54ACB-B3AA-42B5-807E-764F8236F3A6 | Daily 4:30am | 1 | 4 | 1 | 1 | 0 | 0 | 0 | 20250131 | 99991231 | 43000 | 235959 |
| C99885B7-9418-44DC-A459-BD7209DADED2 | CDC capture agent schedule. | 1 | 64 | 0 | 0 | 0 | 0 | 0 | 20250527 | 99991231 | 0 | 235959 |
| 196140A4-ECB3-4C2B-982E-A01616E9C95D | CDC cleanup agent schedule. | 1 | 4 | 1 | 1 | 1 | 1 | 0 | 20250527 | 99991231 | 20000 | 235959 |
| A90D5220-AB22-4AFD-90F0-25300EEC92F7 | Corrida_diaria_T080 | 1 | 4 | 1 | 1 | 0 | 0 | 0 | 20250704 | 99991231 | 63000 | 235959 |
| EE5AD048-89E5-41C5-97A2-5BE9649F5F8C | 3_Veces_x_Semana | 1 | 8 | 42 | 1 | 0 | 0 | 1 | 20250806 | 99991231 | 50000 | 235959 |
| 0DB73BEE-EF05-49DE-B3A7-995F14932512 | DBA-Rebuild Index.Subplan_1 | 1 | 4 | 1 | 1 | 0 | 0 | 0 | 20260601 | 99991231 | 0 | 235959 |


## backups

Sin registros visibles o consulta no disponible; consultar RESUMEN.json.