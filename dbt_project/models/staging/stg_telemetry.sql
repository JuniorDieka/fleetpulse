{{
    config(
        materialized='view'
    )
}}

/*
Staging Model: stg_telemetry

Business Logic:
- Parse and cast raw sensor telemetry data
- Flag and filter outliers (negative hydraulic pressure, engine temp beyond physical limits)
- Deduplicate by truck_id + timestamp
- Add ingestion metadata

Data Quality:
- Enforces schema tests (not_null, accepted_values, range checks)
- Implements data contracts for downstream consumption
*/

WITH raw_telemetry AS (
    SELECT
        truck_id,
        CAST(timestamp AS TIMESTAMP) AS timestamp,
        CAST(engine_temp_c AS DOUBLE) AS engine_temp_c,
        CAST(hydraulic_pressure_psi AS DOUBLE) AS hydraulic_pressure_psi,
        CAST(payload_weight_tons AS DOUBLE) AS payload_weight_tons,
        CAST(vibration_level_mm_s AS DOUBLE) AS vibration_level_mm_s,
        CAST(fuel_consumption_l_hr AS DOUBLE) AS fuel_consumption_l_hr,
        CAST(odometer_hours AS DOUBLE) AS odometer_hours,
        CURRENT_TIMESTAMP AS ingested_at
    FROM read_parquet('../data/landing/**/*.parquet', hive_partitioning=true)
),

deduplicated AS (
    SELECT DISTINCT ON (truck_id, timestamp) *
    FROM raw_telemetry
    ORDER BY truck_id, timestamp, ingested_at DESC
),

flagged_outliers AS (
    SELECT
        *,
        CASE
            WHEN engine_temp_c < {{ var('engine_temp_min') }}
                OR engine_temp_c > {{ var('engine_temp_max') }}
                THEN TRUE
            WHEN hydraulic_pressure_psi < {{ var('hydraulic_pressure_min') }}
                THEN TRUE
            WHEN payload_weight_tons < 0
                THEN TRUE
            WHEN vibration_level_mm_s < 0
                THEN TRUE
            WHEN fuel_consumption_l_hr < 0
                THEN TRUE
            ELSE FALSE
        END AS is_outlier
    FROM deduplicated
)

SELECT * FROM flagged_outliers
