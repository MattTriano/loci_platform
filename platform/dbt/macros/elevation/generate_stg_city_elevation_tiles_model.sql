{% macro generate_stg_city_elevation_tiles_model(city) %}
select tile_id, ST_ConvexHull(rast) as hull
from {{ source('threedep', city ~ '_3dep_elevation') }}
where valid_to is null
{% endmacro %}