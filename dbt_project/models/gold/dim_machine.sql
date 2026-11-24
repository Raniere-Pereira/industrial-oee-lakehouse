{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

WITH deduplicated AS (
    SELECT
        TRIM(machine_name) AS machine_name,
        TRIM(machine_type) AS machine_type,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(machine_name)
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('bronze_oee', 'raw_oee_machines') }}
    WHERE TRIM(machine_name) IS NOT NULL
      AND TRIM(machine_name) <> ''
)

SELECT
    SHA2(machine_name, 256) AS machine_sk,
    machine_name,
    machine_type,
    ingested_at
FROM deduplicated
WHERE rn = 1
