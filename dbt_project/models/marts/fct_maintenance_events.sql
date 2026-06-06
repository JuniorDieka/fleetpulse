{{
    config(
        materialized='table'
    )
}}

/*
Fact Model: fct_maintenance_events

Business Logic:
- Historical breakdown and repair events
- Seeded with 3 years of synthetic failure data
- Used for MTBF, MTTR, and Weibull analysis
- Updated with new failure events as they occur

Grain: One row per maintenance event
*/

SELECT
    event_id,
    truck_id,
    CAST(failure_timestamp AS TIMESTAMP) AS failure_timestamp,
    failure_type,
    CAST(downtime_hours AS DOUBLE) AS downtime_hours,
    CAST(repair_hours AS DOUBLE) AS repair_hours,
    CAST(odometer_at_failure AS DOUBLE) AS odometer_at_failure
FROM {{ ref('maintenance_events_seed') }}
