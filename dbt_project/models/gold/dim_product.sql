{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

WITH deduplicated AS (
    SELECT
        TRIM(product_name) AS product_name,
        TRY_CAST(REPLACE(TRIM(bsicuits_per_pallet), ',', '.') AS INT) AS biscuits_per_pallet,
        TRY_CAST(REPLACE(TRIM(biscuits_per_pack), ',', '.') AS INT) AS biscuits_per_pack,
        TRY_CAST(REPLACE(TRIM(biscuits_per_case), ',', '.') AS INT) AS biscuits_per_case,
        TRY_CAST(REPLACE(TRIM(biscuits_per_pallet), ',', '.') AS INT) AS cases_per_pallet,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(product_name)
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('bronze_oee', 'raw_oee_products') }}
    WHERE TRIM(product_name) IS NOT NULL
      AND TRIM(product_name) <> ''
)

SELECT
    SHA2(product_name, 256) AS product_sk,
    product_name,
    biscuits_per_pallet,
    biscuits_per_pack,
    biscuits_per_case,
    cases_per_pallet,
    ingested_at
FROM deduplicated
WHERE rn = 1
