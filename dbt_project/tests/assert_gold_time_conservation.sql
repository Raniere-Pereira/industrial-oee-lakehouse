-- Garante conservação aditiva de tempo na camada Gold (ISO 22400):
-- soma dos 5 buckets de tempo deve igualar total_duration_minutes (tolerância < 0.01 min).
SELECT
    event_date,
    machine_sk,
    product_sk,
    total_duration_minutes,
    (
        no_order_minutes
        + maintenance_minutes
        + changeover_minutes
        + explicit_run_time_minutes
        + unclassified_minutes
    ) AS sum_buckets_minutes
FROM {{ ref('fct_oee_daily') }}
WHERE ABS(
    (
        no_order_minutes
        + maintenance_minutes
        + changeover_minutes
        + explicit_run_time_minutes
        + unclassified_minutes
    ) - total_duration_minutes
) >= 0.01
