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
        CASE
            WHEN machine_name = 'Biscuit Filling Machine' THEN 'CUMULATIVE_ODOMETER'
            ELSE 'PER_EVENT_INCREMENTAL'
        END AS good_counter_mode,
        start_timestamp,
        end_timestamp,
        duration_minutes,
        raw_total_biscuits_made,
        good_biscuits_made,
        -- Na Biscuit Filling Machine (odômetro acumulado), eventos com raw_total = 0 e good <= 500
        -- representam perda de telemetria e são excluídos da janela de odômetro de bons biscoitos
        -- para evitar falsos resets.
        CASE
            WHEN machine_name = 'Biscuit Filling Machine' THEN
                CASE
                    WHEN good_biscuits_made > 0
                     AND (raw_total_biscuits_made > 0 OR good_biscuits_made > 500)
                    THEN good_biscuits_made
                    ELSE CAST(NULL AS BIGINT)
                END
            ELSE good_biscuits_made
        END AS effective_good_reading,
        _source_file,
        ingestion_date,
        ingested_at
    FROM deduplicated
    WHERE rn = 1
),

{% if is_incremental() %}
existing_total_anchor AS (
    SELECT
        machine_name,
        total_counter_cycle_id AS anchor_total_cycle_id,
        raw_total_biscuits_made AS anchor_last_valid_total,
        running_max_total_in_cycle AS anchor_running_max_total
    FROM (
        SELECT
            machine_name,
            total_counter_cycle_id,
            raw_total_biscuits_made,
            running_max_total_in_cycle,
            ROW_NUMBER() OVER (
                PARTITION BY machine_name
                ORDER BY
                    start_timestamp DESC,
                    end_timestamp DESC,
                    raw_total_biscuits_made DESC,
                    good_biscuits_made DESC,
                    row_hash DESC
            ) AS anchor_rn
        FROM {{ this }}
        WHERE raw_total_biscuits_made > 0
    )
    WHERE anchor_rn = 1
),

existing_good_anchor AS (
    SELECT
        machine_name,
        good_counter_cycle_id AS anchor_good_cycle_id,
        good_biscuits_made AS anchor_last_valid_good,
        running_max_good_in_cycle AS anchor_running_max_good
    FROM (
        SELECT
            machine_name,
            good_counter_cycle_id,
            good_biscuits_made,
            running_max_good_in_cycle,
            ROW_NUMBER() OVER (
                PARTITION BY machine_name
                ORDER BY
                    start_timestamp DESC,
                    end_timestamp DESC,
                    raw_total_biscuits_made DESC,
                    good_biscuits_made DESC,
                    row_hash DESC
            ) AS anchor_rn
        FROM {{ this }}
        WHERE good_counter_mode = 'CUMULATIVE_ODOMETER'
          AND running_max_good_in_cycle IS NOT NULL
    )
    WHERE anchor_rn = 1
),

events_with_anchor AS (
    SELECT
        e.*,
        ta.anchor_total_cycle_id,
        ta.anchor_last_valid_total,
        ta.anchor_running_max_total,
        ga.anchor_good_cycle_id,
        ga.anchor_last_valid_good,
        ga.anchor_running_max_good
    FROM new_events e
    LEFT JOIN existing_total_anchor ta
        ON e.machine_name = ta.machine_name
    LEFT JOIN existing_good_anchor ga
        ON e.machine_name = ga.machine_name
),
{% else %}
events_with_anchor AS (
    SELECT
        e.*,
        CAST(NULL AS BIGINT) AS anchor_total_cycle_id,
        CAST(NULL AS BIGINT) AS anchor_last_valid_total,
        CAST(NULL AS BIGINT) AS anchor_running_max_total,
        CAST(NULL AS BIGINT) AS anchor_good_cycle_id,
        CAST(NULL AS BIGINT) AS anchor_last_valid_good,
        CAST(NULL AS BIGINT) AS anchor_running_max_good
    FROM new_events e
),
{% endif %}

with_prev_readings AS (
    SELECT
        *,
        COALESCE(
            LAST_VALUE(
                CASE WHEN raw_total_biscuits_made > 0 THEN raw_total_biscuits_made END,
                TRUE
            ) OVER (
                PARTITION BY machine_name
                ORDER BY
                    start_timestamp,
                    end_timestamp,
                    raw_total_biscuits_made,
                    good_biscuits_made,
                    row_hash
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            anchor_last_valid_total
        ) AS prev_valid_total,
        COALESCE(
            LAST_VALUE(
                CASE
                    WHEN good_counter_mode = 'CUMULATIVE_ODOMETER'
                    THEN effective_good_reading
                END,
                TRUE
            ) OVER (
                PARTITION BY machine_name
                ORDER BY
                    start_timestamp,
                    end_timestamp,
                    raw_total_biscuits_made,
                    good_biscuits_made,
                    row_hash
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            anchor_last_valid_good
        ) AS prev_valid_good
    FROM events_with_anchor
),

with_reset_and_jitter AS (
    SELECT
        *,
        CASE
            WHEN raw_total_biscuits_made > 0
             AND prev_valid_total IS NOT NULL
             AND raw_total_biscuits_made < prev_valid_total * 0.5
            THEN TRUE
            ELSE FALSE
        END AS is_counter_reset,
        CASE
            WHEN raw_total_biscuits_made > 0
             AND prev_valid_total IS NOT NULL
             AND raw_total_biscuits_made >= prev_valid_total * 0.5
             AND raw_total_biscuits_made < prev_valid_total
            THEN TRUE
            ELSE FALSE
        END AS is_counter_jitter,
        CASE
            WHEN good_counter_mode = 'CUMULATIVE_ODOMETER'
             AND effective_good_reading IS NOT NULL
             AND prev_valid_good IS NOT NULL
             AND effective_good_reading < prev_valid_good * 0.5
            THEN TRUE
            ELSE FALSE
        END AS is_good_counter_reset
    FROM with_prev_readings
),

with_cycles AS (
    SELECT
        *,
        CAST(
            COALESCE(anchor_total_cycle_id, 0)
            + SUM(CASE WHEN is_counter_reset THEN 1 ELSE 0 END) OVER (
                PARTITION BY machine_name
                ORDER BY
                    start_timestamp,
                    end_timestamp,
                    raw_total_biscuits_made,
                    good_biscuits_made,
                    row_hash
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS BIGINT
        ) AS total_counter_cycle_id,
        CAST(
            COALESCE(anchor_good_cycle_id, 0)
            + SUM(CASE WHEN is_good_counter_reset THEN 1 ELSE 0 END) OVER (
                PARTITION BY machine_name
                ORDER BY
                    start_timestamp,
                    end_timestamp,
                    raw_total_biscuits_made,
                    good_biscuits_made,
                    row_hash
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS BIGINT
        ) AS good_counter_cycle_id
    FROM with_reset_and_jitter
),

with_high_water_mark AS (
    SELECT
        *,
        CASE
            WHEN anchor_total_cycle_id IS NOT NULL
             AND total_counter_cycle_id = anchor_total_cycle_id THEN
                GREATEST(
                    anchor_running_max_total,
                    COALESCE(
                        MAX(CASE WHEN raw_total_biscuits_made > 0 THEN raw_total_biscuits_made END) OVER (
                            PARTITION BY machine_name, total_counter_cycle_id
                            ORDER BY
                                start_timestamp,
                                end_timestamp,
                                raw_total_biscuits_made,
                                good_biscuits_made,
                                row_hash
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ),
                        anchor_running_max_total
                    )
                )
            ELSE
                MAX(CASE WHEN raw_total_biscuits_made > 0 THEN raw_total_biscuits_made END) OVER (
                    PARTITION BY machine_name, total_counter_cycle_id
                    ORDER BY
                        start_timestamp,
                        end_timestamp,
                        raw_total_biscuits_made,
                        good_biscuits_made,
                        row_hash
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
        END AS prev_running_max_total_in_cycle,
        CASE
            WHEN good_counter_mode <> 'CUMULATIVE_ODOMETER' THEN CAST(NULL AS BIGINT)
            WHEN anchor_good_cycle_id IS NOT NULL
             AND good_counter_cycle_id = anchor_good_cycle_id THEN
                GREATEST(
                    anchor_running_max_good,
                    COALESCE(
                        MAX(effective_good_reading) OVER (
                            PARTITION BY machine_name, good_counter_cycle_id
                            ORDER BY
                                start_timestamp,
                                end_timestamp,
                                raw_total_biscuits_made,
                                good_biscuits_made,
                                row_hash
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ),
                        anchor_running_max_good
                    )
                )
            ELSE
                MAX(effective_good_reading) OVER (
                    PARTITION BY machine_name, good_counter_cycle_id
                    ORDER BY
                        start_timestamp,
                        end_timestamp,
                        raw_total_biscuits_made,
                        good_biscuits_made,
                        row_hash
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
        END AS prev_running_max_good_in_cycle
    FROM with_cycles
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
    total_counter_cycle_id,
    CASE
        WHEN raw_total_biscuits_made = 0 AND prev_running_max_total_in_cycle IS NULL THEN CAST(NULL AS BIGINT)
        WHEN raw_total_biscuits_made = 0 THEN prev_running_max_total_in_cycle
        WHEN prev_running_max_total_in_cycle IS NULL THEN raw_total_biscuits_made
        ELSE GREATEST(prev_running_max_total_in_cycle, raw_total_biscuits_made)
    END AS running_max_total_in_cycle,
    CASE
        WHEN raw_total_biscuits_made = 0 THEN CAST(NULL AS BIGINT)
        WHEN is_counter_reset THEN CAST(raw_total_biscuits_made AS BIGINT)
        WHEN prev_running_max_total_in_cycle IS NULL THEN CAST(0 AS BIGINT)
        WHEN is_counter_jitter THEN CAST(0 AS BIGINT)
        ELSE GREATEST(
            CAST(0 AS BIGINT),
            CAST(raw_total_biscuits_made - prev_running_max_total_in_cycle AS BIGINT)
        )
    END AS delta_total_biscuits_made,
    is_counter_reset,
    is_counter_jitter,
    good_biscuits_made,
    good_counter_mode,
    good_counter_cycle_id,
    CASE
        WHEN good_counter_mode <> 'CUMULATIVE_ODOMETER' THEN CAST(NULL AS BIGINT)
        WHEN effective_good_reading IS NULL THEN prev_running_max_good_in_cycle
        WHEN prev_running_max_good_in_cycle IS NULL THEN effective_good_reading
        ELSE GREATEST(prev_running_max_good_in_cycle, effective_good_reading)
    END AS running_max_good_in_cycle,
    CASE
        WHEN good_counter_mode = 'PER_EVENT_INCREMENTAL' THEN CAST(good_biscuits_made AS BIGINT)
        WHEN effective_good_reading IS NULL THEN CAST(NULL AS BIGINT)
        WHEN is_good_counter_reset THEN CAST(effective_good_reading AS BIGINT)
        WHEN prev_running_max_good_in_cycle IS NULL THEN CAST(0 AS BIGINT)
        ELSE GREATEST(
            CAST(0 AS BIGINT),
            CAST(effective_good_reading - prev_running_max_good_in_cycle AS BIGINT)
        )
    END AS delta_good_biscuits_made,
    is_good_counter_reset,
    _source_file,
    ingestion_date,
    ingested_at
FROM with_high_water_mark
