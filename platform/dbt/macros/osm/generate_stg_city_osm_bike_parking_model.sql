{% macro generate_stg_city_osm_bike_parking_model(city) %}

select
    osm_id,
    'osm'::text as source,
    tags ->> 'amenity' as amenity,
    tags ->> 'bicycle_parking' as type,
    tags ->> 'covered' as covered,
    (tags ->> 'capacity')::int as capacity,
    tags ->> 'indoor' as indoor,
    tags ->> 'fee' as fee,
    tags ->> 'lit' as lit,
    tags ->> 'operator' as operator,
    tags ->> 'access' as access,
    geom as raw_geom,
    ST_Centroid(geom) as geom
from {{ source('osm', city ~ '_osm_bike_parking') }}
where valid_to is null

{% endmacro %}
