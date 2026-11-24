-- Garante que todos os pilares e scores de OEE das duas lentes (Estrita MES e Efetiva Telemetria),
-- quando não nulos, permaneçam estritamente no intervalo [0, 1].
SELECT
    event_date,
    machine_sk,
    product_sk,
    availability_rate,
    performance_rate,
    quality_rate,
    oee_score,
    availability_rate_effective,
    performance_rate_effective,
    oee_score_effective
FROM {{ ref('fct_oee_daily') }}
WHERE (availability_rate IS NOT NULL AND (availability_rate < 0 OR availability_rate > 1))
   OR (performance_rate IS NOT NULL AND (performance_rate < 0 OR performance_rate > 1))
   OR (quality_rate IS NOT NULL AND (quality_rate < 0 OR quality_rate > 1))
   OR (oee_score IS NOT NULL AND (oee_score < 0 OR oee_score > 1))
   OR (availability_rate_effective IS NOT NULL AND (availability_rate_effective < 0 OR availability_rate_effective > 1))
   OR (performance_rate_effective IS NOT NULL AND (performance_rate_effective < 0 OR performance_rate_effective > 1))
   OR (oee_score_effective IS NOT NULL AND (oee_score_effective < 0 OR oee_score_effective > 1))
