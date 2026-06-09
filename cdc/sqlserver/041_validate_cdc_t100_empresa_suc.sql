USE [DiarcoP];
GO

SELECT
    DB_NAME() AS database_name,
    d.is_cdc_enabled,
    d.recovery_model_desc
FROM sys.databases d
WHERE d.name = DB_NAME();
GO

SELECT
    capture_instance,
    start_lsn,
    index_name,
    supports_net_changes
FROM cdc.change_tables
WHERE capture_instance = 'dbo_T100_EMPRESA_SUC';
GO

SELECT
    sys.fn_cdc_get_min_lsn('dbo_T100_EMPRESA_SUC') AS min_lsn,
    sys.fn_cdc_get_max_lsn() AS max_lsn;
GO

DECLARE @from_lsn binary(10) = sys.fn_cdc_get_min_lsn('dbo_T100_EMPRESA_SUC');
DECLARE @to_lsn binary(10) = sys.fn_cdc_get_max_lsn();

SELECT TOP (200) *
FROM cdc.fn_cdc_get_all_changes_dbo_T100_EMPRESA_SUC(
    @from_lsn,
    @to_lsn,
    'all'
)
ORDER BY __$start_lsn DESC, __$seqval DESC;
GO
