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
        oee_category,
        duration_minutes,
        total_biscuits_made,
        good_biscuits_made
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
        COUNT(*) AS event_count,
        SUM(e.duration_minutes) AS total_duration_minutes,
        SUM(
            CASE
                WHEN e.oee_category IN ('NO (No Order)', 'PM (Maintenance)')
                THEN e.duration_minutes
                ELSE 0.0
            END
        ) AS downtime_minutes,
        SUM(e.total_biscuits_made) AS total_biscuits_made,
        SUM(e.good_biscuits_made) AS good_biscuits_made,
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

oee_components AS (
    SELECT
        event_date,
        machine_sk,
        product_sk,
        event_count,
        ROUND(total_duration_minutes, 4) AS total_duration_minutes,
        ROUND(downtime_minutes, 4) AS downtime_minutes,
        ROUND(GREATEST(total_duration_minutes - downtime_minutes, 0.0), 4) AS run_time_minutes,
        target_biscuits_per_hour,
        total_biscuits_made,
        good_biscuits_made,
        CASE
            WHEN total_duration_minutes > 0 THEN
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        (total_duration_minutes - downtime_minutes) / total_duration_minutes
                    )
                )
            ELSE 0.0
        END AS availability_rate,
        CASE
            WHEN GREATEST(total_duration_minutes - downtime_minutes, 0.0) > 0
             AND target_biscuits_per_hour > 0 THEN
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        CAST(total_biscuits_made AS DOUBLE)
                        / (
                            GREATEST(total_duration_minutes - downtime_minutes, 0.0)
                            * (target_biscuits_per_hour / 60.0)
                        )
                    )
                )
            ELSE 0.0
        END AS performance_rate,
        CASE
            WHEN total_biscuits_made > 0 THEN
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        CAST(good_biscuits_made AS DOUBLE) / CAST(total_biscuits_made AS DOUBLE)
                    )
                )
            ELSE 0.0
        END AS quality_rate
    FROM daily_agg
)

SELECT
    event_date,
    machine_sk,
    product_sk,
    event_count,
    total_duration_minutes,
    downtime_minutes,
    run_time_minutes,
    target_biscuits_per_hour,
    total_biscuits_made,
    good_biscuits_made,
    ROUND(availability_rate, 6) AS availability_rate,
    ROUND(performance_rate, 6) AS performance_rate,
    ROUND(quality_rate, 6) AS quality_rate,
    ROUND(availability_rate * performance_rate * quality_rate, 6) AS oee_score
FROM oee_components
