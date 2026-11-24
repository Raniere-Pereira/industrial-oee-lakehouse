-- Garante que os biscoitos bons não excedam o total produzido
-- e que o oee_score permaneça estritamente no intervalo [0, 1].
SELECT
    event_date,
    machine_sk,
    product_sk,
    good_biscuits_made,
    total_biscuits_made,
    oee_score
FROM {{ ref('fct_oee_daily') }}
WHERE good_biscuits_made > total_biscuits_made
   OR oee_score < 0
   OR oee_score > 1
   OR oee_score IS NULL
