create table if not exists raw_data.chicago_osm_bike_network_edges (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "node_ids" bigint[],
    "highway" text,
    "name" text,
    "bicycle" text,
    "cycleway" text,
    "cycleway_left" text,
    "cycleway_right" text,
    "surface" text,
    "maxspeed" text,
    "oneway" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_bike_network_edges
    add constraint uq_chicago_osm_bike_network_edges_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_bike_network_edges_current
    on raw_data.chicago_osm_bike_network_edges ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_bike_network_edges_geom
    on raw_data.chicago_osm_bike_network_edges using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_bike_network_nodes (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "node_ids" bigint[],
    "highway" text,
    "crossing" text,
    "traffic_calming" text,
    "barrier" text,
    "bicycle" text,
    "name" text,
    "button_operated" text,
    "tactile_paving" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_bike_network_nodes
    add constraint uq_chicago_osm_bike_network_nodes_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_bike_network_nodes_current
    on raw_data.chicago_osm_bike_network_nodes ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_bike_network_nodes_geom
    on raw_data.chicago_osm_bike_network_nodes using gist (geom)
    where valid_to is null;
