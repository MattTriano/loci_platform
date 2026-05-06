select
    osm_id,
    tags ->> 'amenity' as amenity,
    tags ->> 'bicycle_parking' as type,
    tags ->> 'covered' as covered,
    (tags ->> 'capacity')::int as capacity,
    tags ->> 'indoor' as indoor,
    tags ->> 'fee' as fee,
    tags ->> 'lit' as lit,
    tags ->> 'operator' as operator,
    tags ->> 'access' as access,
    geom
from {{ source('raw_data', 'osm_nodes') }}
where
    tags ->> 'amenity' = 'bicycle_parking'
    and geom && ST_MakeEnvelope(-87.97, 41.62, -87.5, 42.05, 4326)
    and valid_to is null
