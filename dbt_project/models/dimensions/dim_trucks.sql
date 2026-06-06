{{
    config(
        materialized='table'
    )
}}

/*
Dimension Model: dim_trucks

Business Logic:
- Static truck metadata for the fleet
- Seeded from CSV file containing truck specifications
- Used for joins with fact tables and dashboard filtering

Grain: One row per truck
*/

SELECT
    truck_id,
    model,
    purchase_year,
    engine_type,
    pit_zone,
    maintenance_tier
FROM {{ ref('dim_trucks_seed') }}
