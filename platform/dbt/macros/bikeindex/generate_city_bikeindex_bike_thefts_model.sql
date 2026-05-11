{% macro generate_city_bikeindex_bike_thefts_model(city, time_zone = 'America/Chicago') %}

select
    source,
    id as source_id,
    date_stolen as theft_date,
    extract(year from date_stolen)::int as theft_year,
    extract(hour from date_stolen at time zone '{{ time_zone }}')::int as theft_hour,
    title as bike_title,
    description as bike_description,
    theft_description,
    locking_description,
    lock_defeat_description,
    status as theft_status,
    latitude,
    longitude,
    geom as location
from {{ ref('stg_' ~ city ~ '_bikeindex_bike_thefts') }}

{% endmacro %}
