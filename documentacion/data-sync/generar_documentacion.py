"""Captura de metadatos SQL Server. No lee filas de negocio ni ejecuta DDL."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import re
import argparse
import pyodbc
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
QUERIES = {
    'servidor': "SELECT @@SERVERNAME servidor, SERVERPROPERTY('ProductVersion') version, SERVERPROPERTY('Edition') edition, SERVERPROPERTY('Collation') collation",
    'base': "SELECT name, compatibility_level, collation_name, recovery_model_desc, state_desc, containment_desc, page_verify_option_desc, is_read_committed_snapshot_on, snapshot_isolation_state_desc, is_auto_close_on, is_auto_shrink_on, is_cdc_enabled, is_broker_enabled, is_trustworthy_on FROM sys.databases WHERE database_id=DB_ID()",
    'permisos_captura': "SELECT HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION') view_definition, IS_SRVROLEMEMBER('sysadmin') sysadmin",
    'archivos': 'SELECT name,type_desc,physical_name,size,growth,is_percent_growth,max_size FROM sys.database_files',
    'filegroups': 'SELECT name,type_desc,is_default,is_read_only FROM sys.filegroups',
    'esquemas': 'SELECT name,USER_NAME(principal_id) propietario FROM sys.schemas',
    'objetos': "SELECT o.object_id,SCHEMA_NAME(o.schema_id) esquema,o.name,o.type,o.type_desc,o.create_date,o.modify_date FROM sys.objects o WHERE o.is_ms_shipped=0 ORDER BY esquema,o.name",
    'columnas': "SELECT c.object_id,c.column_id,c.name,t.name tipo,SCHEMA_NAME(t.schema_id) esquema_tipo,c.max_length,c.precision,c.scale,c.is_nullable,c.collation_name,c.is_identity,CONVERT(varchar(100),ic.seed_value) identity_seed,CONVERT(varchar(100),ic.increment_value) identity_increment,c.is_computed,cc.definition computed_definition,cc.is_persisted,dc.name default_name,dc.definition default_definition,c.is_rowguidcol,c.is_sparse,c.generated_always_type_desc FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id LEFT JOIN sys.identity_columns ic ON c.object_id=ic.object_id AND c.column_id=ic.column_id LEFT JOIN sys.computed_columns cc ON c.object_id=cc.object_id AND c.column_id=cc.column_id LEFT JOIN sys.default_constraints dc ON c.default_object_id=dc.object_id WHERE c.object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped=0) ORDER BY c.object_id,c.column_id",
    'tablas': 'SELECT object_id,temporal_type_desc,is_memory_optimized,durability_desc,is_tracked_by_cdc,lock_escalation_desc FROM sys.tables WHERE is_ms_shipped=0',
    'indices': 'SELECT i.object_id,i.index_id,i.name,i.type_desc,i.is_unique,i.is_primary_key,i.is_unique_constraint,i.is_disabled,i.fill_factor,i.has_filter,i.filter_definition,ds.name data_space FROM sys.indexes i LEFT JOIN sys.data_spaces ds ON i.data_space_id=ds.data_space_id WHERE i.object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped=0)',
    'columnas_indices': 'SELECT ic.object_id,ic.index_id,ic.index_column_id,c.name,ic.key_ordinal,ic.is_descending_key,ic.is_included_column,ic.partition_ordinal FROM sys.index_columns ic JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id WHERE ic.object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped=0) ORDER BY ic.object_id,ic.index_id,ic.index_column_id',
    'claves': 'SELECT parent_object_id,name,type_desc,unique_index_id FROM sys.key_constraints WHERE is_ms_shipped=0',
    'foreign_keys': 'SELECT fk.name,fk.parent_object_id,pc.name columna,fk.referenced_object_id,rc.name columna_referenciada,fkc.constraint_column_id,fk.delete_referential_action_desc,fk.update_referential_action_desc,fk.is_disabled,fk.is_not_trusted FROM sys.foreign_keys fk JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id JOIN sys.columns pc ON pc.object_id=fkc.parent_object_id AND pc.column_id=fkc.parent_column_id JOIN sys.columns rc ON rc.object_id=fkc.referenced_object_id AND rc.column_id=fkc.referenced_column_id',
    'checks': 'SELECT parent_object_id,name,definition,is_disabled,is_not_trusted FROM sys.check_constraints',
    'modulos': 'SELECT o.object_id,SCHEMA_NAME(o.schema_id) esquema,o.name,o.type_desc,m.definition,m.uses_ansi_nulls,m.uses_quoted_identifier,m.is_schema_bound,m.execute_as_principal_id FROM sys.objects o LEFT JOIN sys.sql_modules m ON o.object_id=m.object_id WHERE o.is_ms_shipped=0 AND o.type IN (\'P\',\'V\',\'FN\',\'IF\',\'TF\',\'TR\')',
    'parametros': 'SELECT p.object_id,p.parameter_id,p.name,TYPE_NAME(p.user_type_id) tipo,p.max_length,p.precision,p.scale,p.is_output FROM sys.parameters p WHERE p.object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped=0)',
    'dependencias': 'SELECT referencing_id,referenced_server_name,referenced_database_name,referenced_schema_name,referenced_entity_name,referenced_id,is_schema_bound_reference,is_ambiguous FROM sys.sql_expression_dependencies',
    'sinonimos': 'SELECT SCHEMA_NAME(schema_id) esquema,name,base_object_name FROM sys.synonyms',
    'secuencias': 'SELECT SCHEMA_NAME(schema_id) esquema,name,TYPE_NAME(user_type_id) tipo,CONVERT(varchar(100),start_value) start_value,CONVERT(varchar(100),increment) incremento,CONVERT(varchar(100),minimum_value) minimo,CONVERT(varchar(100),maximum_value) maximo,is_cycling,is_cached,cache_size FROM sys.sequences',
    'tipos': 'SELECT SCHEMA_NAME(schema_id) esquema,name,is_table_type,is_assembly_type,TYPE_NAME(system_type_id) tipo_base,max_length,precision,scale,is_nullable FROM sys.types WHERE is_user_defined=1',
    'propiedades': 'SELECT class_desc,major_id,minor_id,name,CONVERT(nvarchar(max),value) valor FROM sys.extended_properties',
    'usuarios': 'SELECT name,type_desc,authentication_type_desc,default_schema_name,CONVERT(varchar(200),sid,1) sid FROM sys.database_principals WHERE principal_id>4',
    'roles': 'SELECT USER_NAME(role_principal_id) rol,USER_NAME(member_principal_id) miembro FROM sys.database_role_members',
    'permisos': 'SELECT USER_NAME(grantee_principal_id) usuario,class_desc,major_id,minor_id,permission_name,state_desc FROM sys.database_permissions',
    'volumen_estimado': 'SELECT object_id,SUM(rows) filas_estimadas FROM sys.partitions WHERE index_id IN (0,1) AND object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped=0) GROUP BY object_id',
    'particiones': 'SELECT object_id,index_id,partition_number,rows,data_compression_desc FROM sys.partitions WHERE object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped=0)',
    'linked_servers': 'SELECT name,product,provider,data_source,catalog,is_linked,is_data_access_enabled,is_rpc_out_enabled FROM sys.servers',
    'jobs': 'SELECT job_id,name,enabled,description,date_created,date_modified FROM msdb.dbo.sysjobs',
    'pasos_jobs': 'SELECT job_id,step_id,step_name,subsystem,database_name,on_success_action,on_fail_action,retry_attempts FROM msdb.dbo.sysjobsteps',
    'horarios_jobs': 'SELECT j.job_id,s.name,s.enabled,s.freq_type,s.freq_interval,s.freq_subday_type,s.freq_subday_interval,s.freq_relative_interval,s.freq_recurrence_factor,s.active_start_date,s.active_end_date,s.active_start_time,s.active_end_time FROM msdb.dbo.sysjobschedules j JOIN msdb.dbo.sysschedules s ON j.schedule_id=s.schedule_id',
    'backups': "SELECT TOP (30) database_name,type,backup_start_date,backup_finish_date,is_copy_only,backup_size,compressed_backup_size,first_lsn,last_lsn,database_backup_lsn FROM msdb.dbo.backupset WHERE database_name=DB_NAME() ORDER BY backup_finish_date DESC",
    'configuracion_instancia': 'SELECT name,CONVERT(nvarchar(4000),value) value,CONVERT(nvarchar(4000),value_in_use) value_in_use,description FROM sys.configurations',
    'cdc_instancias': 'SELECT object_id,source_object_id,capture_instance,start_lsn,supports_net_changes,role_name,index_name,filegroup_name,create_date FROM cdc.change_tables',
    'cdc_columnas': 'SELECT object_id,column_name,column_id,column_type,column_ordinal FROM cdc.captured_columns',
    'logins': "SELECT name,type_desc,is_disabled,default_database_name,default_language_name,CONVERT(varchar(200),sid,1) sid FROM sys.server_principals WHERE type IN ('S','U','G')",
    'roles_servidor': 'SELECT SUSER_NAME(role_principal_id) rol,SUSER_NAME(member_principal_id) miembro FROM sys.server_role_members',
}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--driver', help='Driver ODBC instalado; por defecto SQL_DRIVER del .env')
    args = parser.parse_args()
    cfg = dotenv_values(ROOT / '.env')
    driver = args.driver or cfg.get('SQL_DRIVER')
    if driver not in pyodbc.drivers():
        raise SystemExit('Driver no instalado. Indicar --driver con un driver disponible.')
    if cfg.get('SQL_DATABASE') != 'data-sync':
        raise SystemExit('Se esperaba SQL_DATABASE=data-sync; revisar configuración.')
    secrets = [str(v) for k,v in cfg.items() if v and any(x in k.upper() for x in ('PASSWORD','SECRET','TOKEN','API_KEY'))]
    def redact(value):
        if isinstance(value, dict): return {k:redact(v) for k,v in value.items()}
        if isinstance(value, list): return [redact(v) for v in value]
        if isinstance(value, str):
            for secret in sorted(secrets,key=len,reverse=True): value=value.replace(secret,'[REDACTADO]')
            value=re.sub(r"(?i)((?:password|pwd|api_key|secret|token)\s*=\s*)(?:'[^']*'|\"[^\"]*\"|[^;\s]+)",r'\1[REDACTADO]',value)
        return value
    quote=lambda s:'{'+str(s).replace('}','}}')+'}'
    connection=';'.join(f'{k}={quote(v)}' for k,v in {'DRIVER':driver,'SERVER':cfg['SQL_SERVER'],'DATABASE':cfg['SQL_DATABASE'],'UID':cfg['SQL_USER'],'PWD':cfg['SQL_PASSWORD']}.items())+';Encrypt=yes;TrustServerCertificate=yes;ApplicationIntent=ReadOnly;'
    try: conn=pyodbc.connect(connection,timeout=15,autocommit=True)
    except pyodbc.Error as exc: raise SystemExit('No se pudo conectar. SQLSTATE: '+str(exc.args[0])) from None
    conn.timeout=30
    captured=datetime.now(timezone.utc)
    out=Path(__file__).parent/'capturas'/captured.strftime('%Y%m%dT%H%M%S%fZ')
    out.mkdir(parents=True,exist_ok=False)
    data={}; errors={}
    try:
        for name,sql in QUERIES.items():
            try:
                cur=conn.execute(sql)
                keys=[c[0] for c in cur.description]
                data[name]=redact([dict(zip(keys,row)) for row in cur.fetchall()])
            except pyodbc.Error as exc: errors[name]={'sqlstate':str(exc.args[0]),'estado':'No capturado; revisar permisos o compatibilidad.'}
    finally: conn.close()
    def write(path,content):
        target=out/path; target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(redact(content),encoding='utf-8')
    for name,rows in data.items(): write(Path('metadatos')/(name+'.json'),json.dumps(rows,ensure_ascii=False,indent=2,default=str))
    write(Path('consultas.sql'),'\n\n'.join('-- '+n+'\n'+q+';\nGO' for n,q in QUERIES.items()))
    def cell(v): return str('' if v is None else v).replace('|','\\|').replace('\n','<br>').replace('\r','')
    def table(rows,fields):
        return '| '+' | '.join(fields)+' |\n| '+' | '.join('---' for _ in fields)+' |\n'+''.join('| '+' | '.join(cell(r.get(f)) for f in fields)+' |\n' for r in rows)
    objects=data.get('objetos',[])
    write(Path('INVENTARIO.md'),'# Inventario de objetos\n\n'+table(objects,['object_id','esquema','name','type_desc','create_date','modify_date']))
    dictionary=['# Diccionario de datos\n\nTipos y longitudes extraídos de sys.columns. max_length se expresa en bytes; -1 significa MAX. Filas estimadas de particiones, sin leer datos de negocio.\n']
    for obj in objects:
        if obj['type'].strip() not in ('U','V'): continue
        oid=obj['object_id']
        dictionary.append(f"\n## {obj['esquema']}.{obj['name']}\n\n")
        dictionary.append(table([c for c in data.get('columnas',[]) if c['object_id']==oid],['name','tipo','max_length','precision','scale','is_nullable','is_identity','identity_seed','identity_increment','default_definition','computed_definition']))
        dictionary.append('\n'+table([c for c in data.get('indices',[]) if c['object_id']==oid],['name','type_desc','is_unique','is_primary_key','is_unique_constraint','filter_definition']))
    write(Path('DICCIONARIO.md'),''.join(dictionary))
    # Borrador de tablas convencionales. No pretende sustituir SMO/SSMS ni un backup.
    bracket=lambda s:'['+str(s).replace(']',']]')+']'
    ddl=['-- BORRADOR: revisar antes de ejecutar en una base vacía de laboratorio.\n-- No incluye datos, índices, restricciones de tabla, particionamiento, CDC ni permisos.\n-- Consultar metadatos y GUIA_RECUPERACION.md. No ejecutar sobre producción.\n']
    for obj in objects:
        if obj['type'].strip()!='U': continue
        cols=[]
        for c in data.get('columnas',[]):
            if c['object_id']!=obj['object_id']: continue
            part=bracket(c['name'])+' '
            if c['is_computed']:
                part+='AS '+str(c['computed_definition'])+(' PERSISTED' if c['is_persisted'] else '')
            else:
                typ=c['tipo']; part+=bracket(typ)
                if typ in ('varchar','char','varbinary','binary','nvarchar','nchar'):
                    length=c['max_length']; length=length//2 if typ in ('nvarchar','nchar') and length!=-1 else length
                    part+='('+('MAX' if length==-1 else str(length))+')'
                elif typ in ('decimal','numeric'): part+=f"({c['precision']},{c['scale']})"
                elif typ in ('datetime2','datetimeoffset','time'): part+=f"({c['scale']})"
                if c['collation_name']: part+=' COLLATE '+c['collation_name']
                if c['is_identity']: part+=f" IDENTITY({c['identity_seed']},{c['identity_increment']})"
                if c['is_rowguidcol']: part+=' ROWGUIDCOL'
                if c['is_sparse']: part+=' SPARSE'
                part+=' NULL' if c['is_nullable'] else ' NOT NULL'
                if c['default_definition']: part+=' CONSTRAINT '+bracket(c['default_name'])+' DEFAULT '+c['default_definition']
            cols.append('    '+part)
        ddl.append('CREATE TABLE '+bracket(obj['esquema'])+'.'+bracket(obj['name'])+' (\n'+',\n'.join(cols)+'\n);\nGO\n')
    write(Path('sql/TABLAS_BORRADOR.sql'),'\n'.join(ddl))
    missing=[]
    for mod in data.get('modulos',[]):
        if not mod['definition']: missing.append(f"{mod['esquema']}.{mod['name']}"); continue
        filename=re.sub(r'[^\w.-]','_',f"{mod['object_id']}_{mod['esquema']}.{mod['name']}")+'.sql'
        write(Path('sql/modulos')/filename,'-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.\nSET ANSI_NULLS '+('ON' if mod['uses_ansi_nulls'] else 'OFF')+';\nGO\nSET QUOTED_IDENTIFIER '+('ON' if mod['uses_quoted_identifier'] else 'OFF')+';\nGO\n'+mod['definition']+'\nGO\n')
    summary={'fecha_utc':captured.isoformat(),'base':cfg['SQL_DATABASE'],'driver_captura':driver,'captura_solo_metadatos':True,'cantidades':{k:len(v) for k,v in data.items()},'errores':errors,'modulos_sin_definicion':missing,'limites':['No es un backup de datos ni un script integral de restauración.','Visibilidad limitada a permisos del usuario conectado.','SQL dinámico puede no aparecer en dependencias.','Comandos de jobs y credenciales excluidos; definiciones pueden contener redacciones.','No se capturan contraseñas, claves de cifrado, certificados privados ni filas de negocio.','La captura no es transaccional; puede haber cambios concurrentes.']}
    write(Path('RESUMEN.json'),json.dumps(summary,ensure_ascii=False,indent=2))
    write(Path('ENTORNO.md'),'# Entorno observado\n\n'+ '\n\n'.join('## '+k+'\n\n'+table(data.get(k,[]),list(data[k][0])) if data.get(k) else '## '+k+'\n\nSin registros visibles o consulta no disponible; consultar RESUMEN.json.' for k in ['servidor','base','archivos','permisos_captura','linked_servers','jobs','horarios_jobs','backups']))
    manifest={str(p.relative_to(out)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.rglob('*')) if p.is_file()}
    write(Path('SHA256.json'),json.dumps(manifest,indent=2))
    print(json.dumps({'carpeta':str(out),'cantidades':summary['cantidades'],'errores':errors,'modulos_sin_definicion':missing},ensure_ascii=True))

if __name__=='__main__': main()
