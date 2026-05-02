select
    location,
    name,
    quantity::int,
    type,
    latitude,
    longitude,
    the_geom as geom,
    socrata_id
from {{ source('raw_data', 'chicago_bike_racks') }}
where valid_to is null
