{{ config(
    materialized='table',
    unique_key='id'
)}}

select *
from {{ source('dev', 'raw_weather_data')}}