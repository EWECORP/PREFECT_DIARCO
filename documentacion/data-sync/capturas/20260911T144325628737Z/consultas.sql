-- servidor
SELECT @@SERVERNAME servidor, SERVERPROPERTY('ProductVersion') version, SERVERPROPERTY('Edition') edition, SERVERPROPERTY('Collation') collation;
GO

-- base
SELECT name, compatibility_level, collation_name, recovery_model_desc, state_desc, containment_desc, page_verify_option_desc, is_read_committed_snapshot_on, snapshot_isolation_state_desc, is_auto_close_on, is_auto_shrink_on, is_cdc_enabled, is_broker_enabled, is_trustworthy_on FROM sys.databases WHERE database_id=DB_ID();
GO

-- permisos_captura
SELECT HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION') view_definition, IS_SRVROLEMEMBER('sysadmin') sysadmin;
GO

-- archivos
SELECT name,type_desc,physical_name,size,growth,is_percent_growth,max_size FROM sys.database_files;
GO

-- filegroups
SELECT name,type_desc,is_default,is_read_only FROM sys.filegroups;
GO

-- esquemas
SELECT name,USER_NAME(principal_id) propietario FROM sys.schemas;
GO

-- objetos
SELECT o.object_id,SCHEMA_NAME(o.schema_id) esquema,o.name,o.type,o.type_desc,o.create_date,o.modify_date FROM sys.objects o WHERE o.is_ms_shipped=0 ORDER BY esquema,o.name;
GO

-- columnas
SELECT c.object_id,c.column_id,c.name,t.name tipo,SCHEMA_NAME(t.schema_id) esquema_tipo,c.max_length,c.precision,c.scale,c.is_nullable,c.collation_name,c.is_identity,CONVERT(varchar(100),ic.seed_value) identity_seed,CONVERT(varchar(100),ic.increment_value) identity_increment,c.is_computed,cc.definition computed_definition,cc.is_persisted,dc.name default_name,dc.definition default_definition,c.is_rowguidcol,c.is_sparse,c.generated_always_type_desc FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id LEFT JOIN sys.identity_columns ic ON c.object_id=ic.object_id AND c.column_id=ic.column_id LEFT JOIN sys.computed_columns cc ON c.object_id=cc.object_id AND c.column_id=cc.column_id LEFT JOIN sys.default_constraints dc ON c.default_object_id=dc.object_id WHERE c.object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped=0) ORDER BY c.object_id,c.column_id;
GO

-- tablas
SELECT object_id,temporal_type_desc,is_memory_optimized,durability_desc,is_tracked_by_cdc,lock_escalation_desc FROM sys.tables WHERE is_ms_shipped=0;
GO

-- indices
SELECT i.object_id,i.index_id,i.name,i.type_desc,i.is_unique,i.is_primary_key,i.is_unique_constraint,i.is_disabled,i.fill_factor,i.has_filter,i.filter_definition,ds.name data_space FROM sys.indexes i LEFT JOIN sys.data_spaces ds ON i.data_space_id=ds.data_space_id WHERE i.object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped=0);
GO

-- columnas_indices
SELECT ic.object_id,ic.index_id,ic.index_column_id,c.name,ic.key_ordinal,ic.is_descending_key,ic.is_included_column,ic.partition_ordinal FROM sys.index_columns ic JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id WHERE ic.object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped=0) ORDER BY ic.object_id,ic.index_id,ic.index_column_id;
GO

-- claves
SELECT parent_object_id,name,type_desc,unique_index_id FROM sys.key_constraints WHERE is_ms_shipped=0;
GO

-- foreign_keys
SELECT fk.name,fk.parent_object_id,pc.name columna,fk.referenced_object_id,rc.name columna_referenciada,fkc.constraint_column_id,fk.delete_referential_action_desc,fk.update_referential_action_desc,fk.is_disabled,fk.is_not_trusted FROM sys.foreign_keys fk JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id JOIN sys.columns pc ON pc.object_id=fkc.parent_object_id AND pc.column_id=fkc.parent_column_id JOIN sys.columns rc ON rc.object_id=fkc.referenced_object_id AND rc.column_id=fkc.referenced_column_id;
GO

-- checks
SELECT parent_object_id,name,definition,is_disabled,is_not_trusted FROM sys.check_constraints;
GO

-- modulos
SELECT o.object_id,SCHEMA_NAME(o.schema_id) esquema,o.name,o.type_desc,m.definition,m.uses_ansi_nulls,m.uses_quoted_identifier,m.is_schema_bound,m.execute_as_principal_id FROM sys.objects o LEFT JOIN sys.sql_modules m ON o.object_id=m.object_id WHERE o.is_ms_shipped=0 AND o.type IN ('P','V','FN','IF','TF','TR');
GO

-- parametros
SELECT p.object_id,p.parameter_id,p.name,TYPE_NAME(p.user_type_id) tipo,p.max_length,p.precision,p.scale,p.is_output FROM sys.parameters p WHERE p.object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped=0);
GO

-- dependencias
SELECT referencing_id,referenced_server_name,referenced_database_name,referenced_schema_name,referenced_entity_name,referenced_id,is_schema_bound_reference,is_ambiguous FROM sys.sql_expression_dependencies;
GO

-- sinonimos
SELECT SCHEMA_NAME(schema_id) esquema,name,base_object_name FROM sys.synonyms;
GO

-- secuencias
SELECT SCHEMA_NAME(schema_id) esquema,name,TYPE_NAME(user_type_id) tipo,CONVERT(varchar(100),start_value) start_value,CONVERT(varchar(100),increment) incremento,CONVERT(varchar(100),minimum_value) minimo,CONVERT(varchar(100),maximum_value) maximo,is_cycling,is_cached,cache_size FROM sys.sequences;
GO

-- tipos
SELECT SCHEMA_NAME(schema_id) esquema,name,is_table_type,is_assembly_type,TYPE_NAME(system_type_id) tipo_base,max_length,precision,scale,is_nullable FROM sys.types WHERE is_user_defined=1;
GO

-- propiedades
SELECT class_desc,major_id,minor_id,name,CONVERT(nvarchar(max),value) valor FROM sys.extended_properties;
GO

-- usuarios
SELECT name,type_desc,authentication_type_desc,default_schema_name,CONVERT(varchar(200),sid,1) sid FROM sys.database_principals WHERE principal_id>4;
GO

-- roles
SELECT USER_NAME(role_principal_id) rol,USER_NAME(member_principal_id) miembro FROM sys.database_role_members;
GO

-- permisos
SELECT USER_NAME(grantee_principal_id) usuario,class_desc,major_id,minor_id,permission_name,state_desc FROM sys.database_permissions;
GO

-- volumen_estimado
SELECT object_id,SUM(rows) filas_estimadas FROM sys.partitions WHERE index_id IN (0,1) AND object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped=0) GROUP BY object_id;
GO

-- particiones
SELECT object_id,index_id,partition_number,rows,data_compression_desc FROM sys.partitions WHERE object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped=0);
GO

-- linked_servers
SELECT name,product,provider,data_source,catalog,is_linked,is_data_access_enabled,is_rpc_out_enabled FROM sys.servers;
GO

-- jobs
SELECT job_id,name,enabled,description,date_created,date_modified FROM msdb.dbo.sysjobs;
GO

-- pasos_jobs
SELECT job_id,step_id,step_name,subsystem,database_name,on_success_action,on_fail_action,retry_attempts FROM msdb.dbo.sysjobsteps;
GO

-- horarios_jobs
SELECT j.job_id,s.name,s.enabled,s.freq_type,s.freq_interval,s.freq_subday_type,s.freq_subday_interval,s.freq_relative_interval,s.freq_recurrence_factor,s.active_start_date,s.active_end_date,s.active_start_time,s.active_end_time FROM msdb.dbo.sysjobschedules j JOIN msdb.dbo.sysschedules s ON j.schedule_id=s.schedule_id;
GO

-- backups
SELECT TOP (30) database_name,type,backup_start_date,backup_finish_date,is_copy_only,backup_size,compressed_backup_size,first_lsn,last_lsn,database_backup_lsn FROM msdb.dbo.backupset WHERE database_name=DB_NAME() ORDER BY backup_finish_date DESC;
GO

-- configuracion_instancia
SELECT name,value,value_in_use,description FROM sys.configurations;
GO