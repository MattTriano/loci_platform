-- stg__osmnx__bike_network_edges.sql
-- Staging model for OSMnx bike network edges over the Chicagoland bbox.
--
-- Source: raw_data.osmnx_bike_network_edges (collected via OsmnxCollector)
-- Grain: one row per directed edge (u, v, key), current version only.
--
-- Notes:
--   - osmid may be a stringified list (e.g. "[123, 456]") for edges that
--     were simplified from multiple OSM ways. Treat as provenance only.
--   - reversed is an OSMnx-derived flag (not an OSM tag) indicating the
--     edge direction was synthesized by reversing an undirected OSM way.
--     Treat as provenance; useful for debugging unexpected routing.
--   - tile_id is ingestion metadata and is excluded from this model.

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

select
    -- Graph topology
    md5(u || '|' || v || '|' || key) as segment_id,
    u,
    v,
    key,
    osmid,

    -- Road attributes
    name,
    highway,
    oneway,
    reversed,
    lanes,
    ref,
    service,
    width,
    maxspeed,
    access,

    -- Bike infrastructure
    cycleway,
    cycleway_right,
    cycleway_left,
    bicycle,

    -- Physical conditions
    surface,
    lit,
    bridge,
    tunnel,

    -- Geometry and distance
    length_m,
    geom

from {{ source('raw_data', 'osmnx_bike_network_edges') }}
where
    valid_to is null
    and geom && ST_MakeEnvelope(-87.97, 41.62, -87.5, 42.05, 4326)
    and u is not null
    and v is not null
    and key is not null
