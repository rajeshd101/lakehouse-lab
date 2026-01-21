{{ config(materialized='table') }}

with source_data as (
    select
        id,
        station_name,
        observation_time,
        ingested_at,
        -- Example of parsing the raw JSON metadata
        json_extract_scalar(raw_metadata, '$.properties.stn_nam-value') as station_name_raw
    from {{ source('iceberg', 'weather_raw') }}
)

select *
from source_data
where observation_time is not null
