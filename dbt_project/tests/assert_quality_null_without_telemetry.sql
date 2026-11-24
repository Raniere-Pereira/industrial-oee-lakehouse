-- Garante honestidade da métrica de Qualidade:
-- quality_rate deve ser NULL sempre que não houver telemetria válida de delta_total_biscuits_made.
SELECT
    event_date,
    machine_sk,
    product_sk,
    delta_total_biscuits_made,
    delta_good_biscuits_made,
    quality_rate
FROM {{ ref('fct_oee_daily') }}
WHERE (delta_total_biscuits_made IS NULL OR delta_total_biscuits_made <= 0)
  AND quality_rate IS NOT NULL
