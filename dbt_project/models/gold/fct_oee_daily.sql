{{
    config(
        materialized='table',
        file_format='delta',
        liquid_clustered_by=['event_date', 'machine_sk']
    )
}}

WITH events AS (
    SELECT
        event_date,
        machine_name,
        product_name,
        time_bucket,
        duration_minutes,
        raw_total_biscuits_made,
        has_total_counter_telemetry,
        delta_total_biscuits_made,
        is_counter_reset,
        is_counter_jitter,
        good_biscuits_made,
        good_counter_mode,
        delta_good_biscuits_made
    FROM {{ ref('stg_oee_events') }}
),

machines AS (
    SELECT
        machine_sk,
        machine_name
    FROM {{ ref('dim_machine') }}
),

products AS (
    SELECT
        product_sk,
        product_name
    FROM {{ ref('dim_product') }}
),

target_speeds AS (
    SELECT
        TRIM(machine) AS machine_name,
        TRIM(product) AS product_name,
        TRY_CAST(REPLACE(TRIM(target_biscuits_per_hour), ',', '.') AS DOUBLE) AS target_biscuits_per_hour,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(machine), TRIM(product)
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('bronze_oee', 'raw_oee_target_speed') }}
),

target_speeds_dedup AS (
    SELECT
        machine_name,
        product_name,
        target_biscuits_per_hour
    FROM target_speeds
    WHERE rn = 1
),

avg_target_speed AS (
    SELECT
        AVG(target_biscuits_per_hour) AS default_target_biscuits_per_hour
    FROM target_speeds_dedup
),

daily_agg AS (
    SELECT
        e.event_date,
        m.machine_sk,
        p.product_sk,
        MAX(e.good_counter_mode) AS good_counter_mode,
        COUNT(*) AS event_count,
        SUM(e.duration_minutes) AS total_duration_minutes,
        SUM(CASE WHEN e.time_bucket = 'NO_ORDER' THEN e.duration_minutes ELSE 0.0 END) AS no_order_minutes,
        SUM(CASE WHEN e.time_bucket <> 'NO_ORDER' THEN e.duration_minutes ELSE 0.0 END) AS planned_production_minutes,
        SUM(CASE WHEN e.time_bucket = 'MAINTENANCE' THEN e.duration_minutes ELSE 0.0 END) AS maintenance_minutes,
        SUM(CASE WHEN e.time_bucket = 'CHANGEOVER' THEN e.duration_minutes ELSE 0.0 END) AS changeover_minutes,
        SUM(CASE WHEN e.time_bucket = 'RUN_TIME' THEN e.duration_minutes ELSE 0.0 END) AS explicit_run_time_minutes,
        SUM(CASE WHEN e.time_bucket = 'UNCLASSIFIED' THEN e.duration_minutes ELSE 0.0 END) AS unclassified_minutes,
        SUM(e.raw_total_biscuits_made) AS raw_total_biscuits_made,
        SUM(e.delta_total_biscuits_made) AS delta_total_biscuits_made,
        SUM(e.good_biscuits_made) AS raw_good_biscuits_sum,
        SUM(e.delta_good_biscuits_made) AS delta_good_biscuits_made,
        SUM(CASE WHEN e.is_counter_reset THEN 1 ELSE 0 END) AS counter_reset_count,
        SUM(CASE WHEN e.is_counter_jitter THEN 1 ELSE 0 END) AS counter_jitter_count,
        MAX(COALESCE(ts.target_biscuits_per_hour, ats.default_target_biscuits_per_hour)) AS target_biscuits_per_hour
    FROM events e
    INNER JOIN machines m
        ON e.machine_name = m.machine_name
    INNER JOIN products p
        ON e.product_name = p.product_name
    LEFT JOIN target_speeds_dedup ts
        ON e.machine_name = ts.machine_name
       AND e.product_name = ts.product_name
    CROSS JOIN avg_target_speed ats
    GROUP BY
        e.event_date,
        m.machine_sk,
        p.product_sk
),

with_expected AS (
    SELECT
        event_date,
        machine_sk,
        product_sk,
        good_counter_mode,
        event_count,
        ROUND(total_duration_minutes, 6) AS total_duration_minutes,
        ROUND(no_order_minutes, 6) AS no_order_minutes,
        ROUND(planned_production_minutes, 6) AS planned_production_minutes,
        ROUND(maintenance_minutes, 6) AS maintenance_minutes,
        ROUND(changeover_minutes, 6) AS changeover_minutes,
        ROUND(explicit_run_time_minutes, 6) AS explicit_run_time_minutes,
        ROUND(unclassified_minutes, 6) AS unclassified_minutes,
        target_biscuits_per_hour,
        ROUND((explicit_run_time_minutes / 60.0) * target_biscuits_per_hour, 6) AS expected_biscuits,
        raw_total_biscuits_made,
        delta_total_biscuits_made,
        raw_good_biscuits_sum,
        delta_good_biscuits_made,
        counter_reset_count,
        counter_jitter_count
    FROM daily_agg
),

oee_components AS (
    SELECT
        *,
        CASE
            WHEN planned_production_minutes > 0 THEN
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        explicit_run_time_minutes / planned_production_minutes
                    )
                )
            ELSE CAST(NULL AS DOUBLE)
        END AS availability_rate,
        CASE
            WHEN expected_biscuits > 0
             AND delta_total_biscuits_made IS NOT NULL THEN
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        CAST(delta_total_biscuits_made AS DOUBLE) / expected_biscuits
                    )
                )
            ELSE CAST(NULL AS DOUBLE)
        END AS performance_rate,
        CASE
            WHEN delta_total_biscuits_made IS NOT NULL
             AND delta_total_biscuits_made > 0
             AND delta_good_biscuits_made IS NOT NULL THEN
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        CAST(delta_good_biscuits_made AS DOUBLE) / CAST(delta_total_biscuits_made AS DOUBLE)
                    )
                )
            ELSE CAST(NULL AS DOUBLE)
        END AS quality_rate
    FROM with_expected
)

SELECT
    event_date,
    machine_sk,
    product_sk,
    good_counter_mode,
    event_count,
    total_duration_minutes,
    no_order_minutes,
    planned_production_minutes,
    maintenance_minutes,
    changeover_minutes,
    explicit_run_time_minutes,
    unclassified_minutes,
    target_biscuits_per_hour,
    expected_biscuits,
    raw_total_biscuits_made,
    delta_total_biscuits_made,
    raw_good_biscuits_sum,
    delta_good_biscuits_made,
    counter_reset_count,
    counter_jitter_count,
    ROUND(availability_rate, 6) AS availability_rate,
    ROUND(performance_rate, 6) AS performance_rate,
    ROUND(quality_rate, 6) AS quality_rate,
    CASE
        WHEN availability_rate IS NOT NULL
         AND performance_rate IS NOT NULL
         AND quality_rate IS NOT NULL THEN
            ROUND(availability_rate * performance_rate * quality_rate, 6)
        ELSE CAST(NULL AS DOUBLE)
    END AS oee_score
FROM oee_components
