{#
    Precomputed tile footprints for elevation sampling. One row per current
    sub-tile of the region's 3DEP raster, carrying the tile's convex hull as a
    plain geometry. Exists so node-to-tile resolution is an indexed hull probe
    instead of a brute-force ST_Intersects that detoasts every raster per node.

    The hull is computed once here (detoasting each tile a single time at build)
    and GiST-indexed via the model's post_hook, so downstream sampling never
    touches the raster pixels just to figure out which tile a point lands in.

    Source: source('threedep', '<city>_3dep_elevation')
    Grain: one row per tile_id (current rows only).
#}
{% macro generate_stg_city_elevation_tiles_model(city) %}

select
    tile_id,
    ST_ConvexHull(rast) as hull
from {{ source('threedep', city ~ '_3dep_elevation') }}
where valid_to is null

{% endmacro %}
