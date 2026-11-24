-- Garante que delta_good_biscuits_made seja sempre >= 0 quando não nulo (Silver e Gold).
SELECT
    'stg_oee_events' AS model_name,
    CAST(event_date AS STRING) AS ref_date,
    machine_name AS entity_key,
    delta_good_biscuits_made
FROM {{ ref('stg_oee_events') }}
WHERE delta_good_biscuits_made IS NOT NULL
  AND delta_good_biscuits_made < 0

UNION ALL

SELECT
    'fct_oee_daily' AS model_name,
    CAST(event_date AS STRING) AS ref_date,
    machine_sk AS entity_key,
    delta_good_biscuits_made
FROM {{ ref('fct_oee_daily') }}
WHERE delta_good_biscuits_made IS NOT NULL
  AND delta_good_biscuits_made < 0
