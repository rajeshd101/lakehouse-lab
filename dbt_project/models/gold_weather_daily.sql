{{ config(materialized='table') }}

select
  date_trunc('day', observation_time) as obs_date,
  count(*) as obs_count
from {{ ref('silver_weather') }}
group by 1
