-- Garante consistência física da lente Efetiva por Telemetria:
-- o tempo efetivo com telemetria ativa nunca pode exceder o tempo efetivo total (+ tolerância 0.001 min).
SELECT
    event_date,
    machine_sk,
    product_sk,
    effective_run_time_minutes,
    effective_run_time_telemetry_minutes
FROM {{ ref('fct_oee_daily') }}
WHERE effective_run_time_telemetry_minutes > effective_run_time_minutes + 0.001
