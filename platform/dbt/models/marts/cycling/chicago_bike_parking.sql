with socrata_parking as (
    select
        'socrata'::text as source,
        socrata_id::text as id,
        location,
        name,
        type,
        quantity as capacity,
        null::text as covered,
        null::text as indoor,
        null::text as fee,
        null::text as lit,
        null::text as operator,
        null::text as access,
        geom
    from {{ ref('stg__soc__chicago_bike_racks') }}
),
osm_parking as (
    select
        'osm'::text as source,
        osm_id::text as id,
        null::text as location,
        null::text as name,
        type,
        capacity,
        covered,
        indoor,
        fee,
        lit,
        operator,
        access,
        geom
    from {{ ref('stg__osm__chicago_bike_racks') }}
),
unioned as (
    select * from socrata_parking
        union all
    select * from osm_parking
)

select * from unioned
