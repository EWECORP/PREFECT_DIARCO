-- 1. Estado consolidado de todos los pilotos CDC
SELECT
    config_name,
    health_status,
    enabled,
    target_schema || '.' || target_table AS target_table,
    last_status,
    monitor_window_active,
    minutes_since_last_activity,
    stale_threshold_minutes,
    failed_runs_last_5,
    last_rowcount,
    last_error,
    last_run_created_at
FROM etl.v_cdc_monitor_status
ORDER BY
    CASE health_status
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        WHEN 'ok' THEN 3
        ELSE 4
    END,
    config_name;

-- 2. Solo alertas abiertas
SELECT
    config_name,
    health_status,
    last_status,
    monitor_window_active,
    minutes_since_last_activity,
    stale_threshold_minutes,
    failed_runs_last_5,
    coalesce(last_error, last_run_error) AS error_text
FROM etl.v_cdc_monitor_status
WHERE health_status IN ('critical', 'warning')
ORDER BY config_name;

-- 3. Ultimas corridas registradas por piloto
SELECT
    config_name,
    status,
    rows_read,
    rows_upserted,
    rows_deleted,
    duration_ms,
    created_at
FROM (
    SELECT
        rl.*,
        row_number() OVER (
            PARTITION BY rl.config_name
            ORDER BY rl.run_id DESC
        ) AS rn
    FROM etl.cdc_run_log rl
) ranked
WHERE rn <= 5
ORDER BY config_name, created_at DESC;
