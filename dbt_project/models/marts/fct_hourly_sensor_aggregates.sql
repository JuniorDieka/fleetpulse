{{
    config(
        materialized='table'
    )
}}

/*
Fact Model: fct_hourly_sensor_aggregates

Business Logic:
- Hourly aggregated sensor statistics per truck
- Calculates mean, stddev, min, max for each sensor
- Excludes outlier readings for accurate statistics
- Used for trend analysis and dashboard visualizations

Grain: One row per truck per hour
*/

WITH hourly_aggregates AS (
    SELECT
        truck_id,
        DATE_TRUNC('hour', timestamp) AS hour_timestamp,

        AVG(engine_temp_c) AS engine_temp_mean,
        STDDEV(engine_temp_c) AS engine_temp_stddev,
        MIN(engine_temp_c) AS engine_temp_min,
        MAX(engine_temp_c) AS engine_temp_max,

        AVG(hydraulic_pressure_psi) AS hydraulic_pressure_mean,
        STDDEV(hydraulic_pressure_psi) AS hydraulic_pressure_stddev,
        MIN(hydraulic_pressure_psi) AS hydraulic_pressure_min,
        MAX(hydraulic_pressure_psi) AS hydraulic_pressure_max,

        AVG(payload_weight_tons) AS payload_weight_mean,
        STDDEV(payload_weight_tons) AS payload_weight_stddev,
        MIN(payload_weight_tons) AS payload_weight_min,
        MAX(payload_weight_tons) AS payload_weight_max,

        AVG(vibration_level_mm_s) AS vibration_level_mean,
        STDDEV(vibration_level_mm_s) AS vibration_level_stddev,
        MIN(vibration_level_mm_s) AS vibration_level_min,
        MAX(vibration_level_mm_s) AS vibration_level_max,

        AVG(fuel_consumption_l_hr) AS fuel_consumption_mean,
        STDDEV(fuel_consumption_l_hr) AS fuel_consumption_stddev,
        MIN(fuel_consumption_l_hr) AS fuel_consumption_min,
        MAX(fuel_consumption_l_hr) AS fuel_consumption_max,

        COUNT(*) AS record_count

    FROM {{ ref('stg_telemetry') }}
    WHERE is_outlier = FALSE
    GROUP BY truck_id, DATE_TRUNC('hour', timestamp)
)

SELECT * FROM hourly_aggregates
ORDER BY truck_id, hour_timestamp
