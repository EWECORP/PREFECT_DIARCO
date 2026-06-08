CREATE OR REPLACE VIEW etl.v_cdc_monitor_status AS
WITH monitor_config AS (
    SELECT
        now() AT TIME ZONE 'America/Argentina/Buenos_Aires' AS local_now,
        TIME '08:00' AS active_window_start,
        TIME '18:00' AS active_window_end
),
latest_run AS (
    SELECT
        rl.config_name,
        rl.status AS last_run_status,
        rl.rows_read,
        rl.rows_upserted,
        rl.rows_deleted,
        rl.duration_ms,
        rl.error_text AS last_run_error,
        rl.created_at AS last_run_created_at,
        row_number() OVER (
            PARTITION BY rl.config_name
            ORDER BY rl.run_id DESC
        ) AS rn
    FROM etl.cdc_run_log rl
),
recent_failures AS (
    SELECT
        rl.config_name,
        count(*) FILTER (WHERE rl.status = 'failed') AS failed_runs_last_5
    FROM (
        SELECT
            config_name,
            status,
            row_number() OVER (
                PARTITION BY config_name
                ORDER BY run_id DESC
            ) AS rn
        FROM etl.cdc_run_log
    ) rl
    WHERE rl.rn <= 5
    GROUP BY rl.config_name
)
SELECT
    cfg.config_name,
    cfg.enabled,
    cfg.mode,
    cfg.source_server,
    cfg.source_database,
    cfg.source_schema,
    cfg.source_table,
    cfg.target_schema,
    cfg.target_table,
    cfg.poll_seconds,
    st.last_status,
    st.last_rowcount,
    st.last_error,
    st.last_started_at,
    st.last_finished_at,
    st.updated_at,
    mc.local_now,
    (mc.local_now::time >= mc.active_window_start AND mc.local_now::time < mc.active_window_end)
        AS monitor_window_active,
    round(EXTRACT(EPOCH FROM (now() - COALESCE(st.last_finished_at, st.last_started_at, st.updated_at))) / 60.0, 2)
        AS minutes_since_last_activity,
    GREATEST(round((cfg.poll_seconds * 3.0) / 60.0, 2), 15.0) AS stale_threshold_minutes,
    lr.last_run_status,
    lr.rows_read AS last_run_rows_read,
    lr.rows_upserted AS last_run_rows_upserted,
    lr.rows_deleted AS last_run_rows_deleted,
    lr.duration_ms AS last_run_duration_ms,
    lr.last_run_error,
    lr.last_run_created_at,
    COALESCE(rf.failed_runs_last_5, 0) AS failed_runs_last_5,
    CASE
        WHEN cfg.enabled = false THEN 'disabled'
        WHEN st.config_name IS NULL OR st.last_status = 'never_run' THEN 'warning'
        WHEN st.last_status = 'failed' THEN 'critical'
        WHEN COALESCE(rf.failed_runs_last_5, 0) >= 2 THEN 'critical'
        WHEN COALESCE(st.last_finished_at, st.last_started_at, st.updated_at) IS NULL THEN 'warning'
        WHEN (mc.local_now::time >= mc.active_window_start AND mc.local_now::time < mc.active_window_end)
             AND EXTRACT(EPOCH FROM (now() - COALESCE(st.last_finished_at, st.last_started_at, st.updated_at))) / 60.0
             > GREATEST((cfg.poll_seconds * 3.0) / 60.0, 15.0) THEN 'critical'
        WHEN st.last_status = 'bootstrapped' THEN 'warning'
        ELSE 'ok'
    END AS health_status
FROM etl.cdc_table_config cfg
CROSS JOIN monitor_config mc
LEFT JOIN etl.cdc_state st
    ON st.config_name = cfg.config_name
LEFT JOIN latest_run lr
    ON lr.config_name = cfg.config_name
   AND lr.rn = 1
LEFT JOIN recent_failures rf
    ON rf.config_name = cfg.config_name
WHERE cfg.mode = 'cdc';
