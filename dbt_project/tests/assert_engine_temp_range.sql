/*
Custom dbt test: Assert engine temperature is within physical limits

This test ensures that all non-outlier engine temperature readings
fall within the physically possible range for CAT 777D engines.
*/

SELECT
    truck_id,
    timestamp,
    engine_temp_c
FROM {{ ref('stg_telemetry') }}
WHERE is_outlier = FALSE
    AND (
        engine_temp_c < {{ var('engine_temp_min') }}
        OR engine_temp_c > {{ var('engine_temp_max') }}
    )
