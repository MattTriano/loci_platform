{% macro generate_city_osm_bike_parking_model(city) %}

select *
from {{ ref('stg_' ~ city ~ '_osm_bike_parking') }}

{% endmacro %}
