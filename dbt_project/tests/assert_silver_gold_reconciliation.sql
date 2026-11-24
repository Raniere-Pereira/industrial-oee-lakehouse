-- Gate de reconciliação de volume e duração entre Silver (stg_oee_events) e Gold (fct_oee_daily).
WITH silver_agg AS (
    SELECT
        COUNT(*) AS silver_events,
        SUM(duration_minutes) AS silver_duration
    FROM {{ ref('stg_oee_events') }}
),
gold_agg AS (
    SELECT
        SUM(event_count) AS gold_events,
        SUM(total_duration_minutes) AS gold_duration
    FROM {{ ref('fct_oee_daily') }}
)
SELECT
    s.silver_events,
    g.gold_events,
    s.silver_duration,
    g.gold_duration
FROM silver_agg s
CROSS JOIN gold_agg g
WHERE s.silver_events <> g.gold_events
   OR ABS(s.silver_duration - g.gold_duration) >= 0.01
