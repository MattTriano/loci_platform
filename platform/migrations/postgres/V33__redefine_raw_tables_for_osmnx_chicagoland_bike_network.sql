drop table raw_data.osmnx_bike_network_nodes;
drop table raw_data.osmnx_bike_network_edges;


create table raw_data.osmnx_bike_network_nodes (
    osmid bigint not null,
    latitude double precision,
    longitude double precision,
    street_count integer,
    highway text,
    ref text,
    geom geometry(Point, 4326),
    tile_id text,
    ingested_at timestamptz not null default (now() at time zone 'UTC'),
    record_hash text not null,
    valid_from timestamptz not null default (now() at time zone 'UTC'),
    valid_to timestamptz
);
alter table raw_data.osmnx_bike_network_nodes
    add constraint uq_osmnx_bike_network_nodes_entity_hash
    unique (osmid, record_hash);
create index ix_osmnx_bike_network_nodes_current
    on raw_data.osmnx_bike_network_nodes (osmid)
    where valid_to is null;
create index ix_osmnx_bike_network_nodes_geom
    on raw_data.osmnx_bike_network_nodes using gist (geom);
create index ix_osmnx_bike_network_nodes_tile_id
    on raw_data.osmnx_bike_network_nodes (tile_id)
    where valid_to is null;


create table raw_data.osmnx_bike_network_edges (
    u bigint not null,
    v bigint not null,
    key integer not null,
    osmid text,
    length_m double precision,
    name text,
    highway text,
    oneway boolean,
    reversed text,
    maxspeed text,
    surface text,
    lanes text,
    ref text,
    service text,
    width text,
    lit text,
    access text,
    bridge text,
    tunnel text,
    bicycle text,
    bicycle_lanes text,
    bicycle_lanes_backward text,
    bicycle_lanes_forward text,
    bicycle_right text,
    bicycle_road text,
    class_bicycle text,
    cyclestreet text,
    oneway_bicycle text,
    ramp_bicycle text,
    sidewalk_both_bicycle text,
    cycleway text,
    cycleway_buffer text,
    cycleway_lane text,
    cycleway_oneway text,
    cycleway_separation text,
    cycleway_shared_lane text,
    cycleway_smoothness text,
    cycleway_surface text,
    cycleway_both text,
    cycleway_both_buffer text,
    cycleway_both_colour text,
    cycleway_both_lane text,
    cycleway_both_separation text,
    cycleway_both_shared_lane text,
    cycleway_both_traffic_sign text,
    cycleway_left text,
    cycleway_left_buffer text,
    cycleway_left_lane text,
    cycleway_left_oneway text,
    cycleway_left_separation text,
    cycleway_left_shared_lane text,
    cycleway_left_traffic_sign text,
    cycleway_right text,
    cycleway_right_buffer text,
    cycleway_right_lane text,
    cycleway_right_oneway text,
    cycleway_right_separation text,
    cycleway_right_shared_lane text,
    cycleway_right_traffic_sign text,
    geom geometry(LineString, 4326),
    tile_id text,
    ingested_at timestamptz not null default (now() at time zone 'UTC'),
    record_hash text not null,
    valid_from timestamptz not null default (now() at time zone 'UTC'),
    valid_to timestamptz
);
alter table raw_data.osmnx_bike_network_edges
    add constraint uq_osmnx_bike_network_edges_entity_hash
    unique (u, v, key, record_hash);
create index ix_osmnx_bike_network_edges_current
    on raw_data.osmnx_bike_network_edges (u, v, key)
    where valid_to is null;
create index ix_osmnx_bike_network_edges_geom
    on raw_data.osmnx_bike_network_edges using gist (geom);
create index ix_osmnx_bike_network_edges_u
    on raw_data.osmnx_bike_network_edges (u)
    where valid_to is null;
create index ix_osmnx_bike_network_edges_v
    on raw_data.osmnx_bike_network_edges (v)
    where valid_to is null;
create index ix_osmnx_bike_network_edges_tile_id
    on raw_data.osmnx_bike_network_edges (tile_id)
    where valid_to is null;
