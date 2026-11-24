-- Gate de reconciliação de eventos, duração e deltas de produção entre Silver (stg_oee_events) e Gold (fct_oee_daily).
WITH silver_agg AS (
    SELECT
        COUNT(*) AS silver_events,
        SUM(duration_minutes) AS silver_duration,
        COALESCE(SUM(delta_total_biscuits_made), 0) AS silver_delta_total,
        COALESCE(SUM(delta_good_biscuits_made), 0) AS silver_delta_good
    FROM {{ ref('stg_oee_events') }}
),
gold_agg AS (
    SELECT
        SUM(event_count) AS gold_events,
        SUM(total_duration_minutes) AS gold_duration,
        COALESCE(SUM(delta_total_biscuits_made), 0) AS gold_delta_total,
        COALESCE(SUM(delta_good_biscuits_made), 0) AS gold_delta_good
    FROM {{ ref('fct_oee_daily') }}
)
SELECT
    s.silver_events,
    g.gold_events,
    s.silver_duration,
    g.gold_duration,
    s.silver_delta_total,
    g.gold_delta_total,
    s.silver_delta_good,
    g.gold_delta_good
FROM silver_agg s
CROSS JOIN gold_agg g
WHERE s.silver_events <> g.gold_events
   OR ABS(s.silver_duration - g.gold_duration) >= 0.01
   OR s.silver_delta_total <> g.gold_delta_total
   OR s.silver_delta_good <> g.gold_delta_good
