-- Garante que os pilares de OEE e o oee_score, quando não nulos,
-- permaneçam estritamente no intervalo [0, 1].
SELECT
    event_date,
    machine_sk,
    product_sk,
    availability_rate,
    performance_rate,
    quality_rate,
    oee_score
FROM {{ ref('fct_oee_daily') }}
WHERE (availability_rate IS NOT NULL AND (availability_rate < 0 OR availability_rate > 1))
   OR (performance_rate IS NOT NULL AND (performance_rate < 0 OR performance_rate > 1))
   OR (quality_rate IS NOT NULL AND (quality_rate < 0 OR quality_rate > 1))
   OR (oee_score IS NOT NULL AND (oee_score < 0 OR oee_score > 1))
