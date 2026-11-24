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
        GREATEST(
            COALESCE(TRY_CAST(REPLACE(TRIM(duration), ',', '.') AS DOUBLE), 0.0),
            0.0
        ) AS duration_minutes,
        GREATEST(
            COALESCE(TRY_CAST(REPLACE(TRIM(totalbiscuitsmade), ',', '.') AS BIGINT), 0),
            0
        ) AS raw_total_biscuits_made,
        GREATEST(
            COALESCE(TRY_CAST(REPLACE(TRIM(goodmadebiscuits), ',', '.') AS BIGINT), 0),
            0
        ) AS good_biscuits_made,
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
),

new_events AS (
    SELECT
        row_hash AS event_id,
        row_hash,
        machine_name,
        product_name,
        oee_category,
        start_timestamp,
        end_timestamp,
        duration_minutes,
        raw_total_biscuits_made,
        good_biscuits_made,
        _source_file,
        ingestion_date,
        ingested_at,
        FALSE AS is_existing_anchor
    FROM deduplicated
    WHERE rn = 1
),

{% if is_incremental() %}
existing_last_valid_reading AS (
    SELECT
        event_id,
        row_hash,
        machine_name,
        product_name,
        oee_category,
        start_timestamp,
        end_timestamp,
        duration_minutes,
        raw_total_biscuits_made,
        good_biscuits_made,
        _source_file,
        ingestion_date,
        ingested_at,
        TRUE AS is_existing_anchor
    FROM (
        SELECT
            event_id,
            row_hash,
            machine_name,
            product_name,
            oee_category,
            start_timestamp,
            end_timestamp,
            duration_minutes,
            raw_total_biscuits_made,
            good_biscuits_made,
            _source_file,
            ingestion_date,
            ingested_at,
            ROW_NUMBER() OVER (
                PARTITION BY machine_name
                ORDER BY start_timestamp DESC, end_timestamp DESC, row_hash DESC
            ) AS anchor_rn
        FROM {{ this }}
        WHERE raw_total_biscuits_made > 0
    )
    WHERE anchor_rn = 1
),

window_input AS (
    SELECT * FROM new_events
    UNION ALL
    -- Inclui a última leitura válida (> 0) já materializada por máquina como âncora
    -- da janela LAG(), garantindo idempotência no primeiro evento de novos lotes incrementais.
    SELECT a.*
    FROM existing_last_valid_reading a
    LEFT ANTI JOIN new_events n
        ON a.row_hash = n.row_hash
),
{% else %}
window_input AS (
    SELECT * FROM new_events
),
{% endif %}

with_odometer_prev AS (
    SELECT
        *,
        LAST_VALUE(
            CASE WHEN raw_total_biscuits_made > 0 THEN raw_total_biscuits_made END,
            TRUE
        ) OVER (
            PARTITION BY machine_name
            ORDER BY start_timestamp, end_timestamp, row_hash
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_valid_total_biscuits
    FROM window_input
)

SELECT
    event_id,
    row_hash,
    CAST(start_timestamp AS DATE) AS event_date,
    machine_name,
    product_name,
    oee_category,
    CASE
        WHEN UPPER(oee_category) = 'NO' OR UPPER(oee_category) LIKE 'NO (%' THEN 'NO_ORDER'
        WHEN UPPER(oee_category) = 'PM' OR UPPER(oee_category) LIKE 'PM (%' THEN 'MAINTENANCE'
        WHEN UPPER(oee_category) = 'CC' OR UPPER(oee_category) LIKE 'CC (%' THEN 'CHANGEOVER'
        WHEN UPPER(oee_category) = 'RUN TIME' THEN 'RUN_TIME'
        ELSE 'UNCLASSIFIED'
    END AS time_bucket,
    start_timestamp,
    end_timestamp,
    duration_minutes,
    raw_total_biscuits_made,
    (raw_total_biscuits_made > 0) AS has_total_counter_telemetry,
    CASE
        WHEN raw_total_biscuits_made = 0 THEN CAST(NULL AS BIGINT)
        WHEN prev_valid_total_biscuits IS NULL THEN CAST(0 AS BIGINT)
        WHEN raw_total_biscuits_made >= prev_valid_total_biscuits
            THEN CAST(raw_total_biscuits_made - prev_valid_total_biscuits AS BIGINT)
        ELSE CAST(raw_total_biscuits_made AS BIGINT)
    END AS delta_total_biscuits_made,
    CASE
        WHEN raw_total_biscuits_made > 0
         AND prev_valid_total_biscuits IS NOT NULL
         AND raw_total_biscuits_made < prev_valid_total_biscuits
            THEN TRUE
        ELSE FALSE
    END AS is_counter_reset,
    good_biscuits_made,
    _source_file,
    ingestion_date,
    ingested_at
FROM with_odometer_prev
WHERE NOT is_existing_anchor
