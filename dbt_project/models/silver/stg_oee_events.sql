{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        file_format='delta',
        liquid_clustered_by=['event_date', 'machine_name']
    )
}}

WITH source_data AS (
    SELECT
        row_hash,
        TRIM(machine) AS machine_name,
        TRIM(product) AS product_name,
        TRIM(oee_category) AS oee_category,
        COALESCE(
            TRY_TO_TIMESTAMP(TRIM(startdatetime), 'd/M/yy HH:mm'),
            TRY_TO_TIMESTAMP(TRIM(startdatetime), 'M/d/yy HH:mm')
        ) AS start_timestamp,
        COALESCE(
            TRY_TO_TIMESTAMP(TRIM(enddatetime), 'd/M/yy HH:mm'),
            TRY_TO_TIMESTAMP(TRIM(enddatetime), 'M/d/yy HH:mm')
        ) AS end_timestamp,
        COALESCE(TRY_CAST(REPLACE(TRIM(duration), ',', '.') AS DOUBLE), 0.0) AS duration_minutes,
        COALESCE(TRY_CAST(REPLACE(TRIM(totalbiscuitsmade), ',', '.') AS BIGINT), 0) AS raw_total_biscuits_made,
        COALESCE(TRY_CAST(REPLACE(TRIM(goodmadebiscuits), ',', '.') AS BIGINT), 0) AS good_biscuits_made,
        _source_file,
        ingestion_date,
        ingested_at
    FROM {{ source('bronze_oee', 'raw_oee_total_report') }}
    {% if is_incremental() %}
    WHERE ingested_at > (
        SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1900-01-01 00:00:00')
        FROM {{ this }}
    )
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY row_hash
            ORDER BY ingested_at DESC
        ) AS rn
    FROM source_data
    WHERE start_timestamp IS NOT NULL
      AND machine_name IS NOT NULL
      AND machine_name <> ''
      AND product_name IS NOT NULL
      AND product_name <> ''
)

SELECT
    row_hash AS event_id,
    row_hash,
    CAST(start_timestamp AS DATE) AS event_date,
    machine_name,
    product_name,
    oee_category,
    start_timestamp,
    end_timestamp,
    GREATEST(duration_minutes, 0.0) AS duration_minutes,
    raw_total_biscuits_made,
    GREATEST(raw_total_biscuits_made, good_biscuits_made) AS total_biscuits_made,
    GREATEST(good_biscuits_made, 0) AS good_biscuits_made,
    (good_biscuits_made > raw_total_biscuits_made) AS is_counter_anomaly,
    _source_file,
    ingestion_date,
    ingested_at
FROM deduplicated
WHERE rn = 1
