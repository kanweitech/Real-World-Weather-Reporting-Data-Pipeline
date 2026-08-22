with source as (
    select *
    from {{ source('weatherstack', 'weather_report') }}
),

renamed as (
    select
        id,
        city,
        temperature,
        weather_description,
        wind_speed,
        time as weather_time_local,
        (inserted_at + (utc_offset || ' hours')::interval) as inserted_at_local
    from source
)

select *
from renamed