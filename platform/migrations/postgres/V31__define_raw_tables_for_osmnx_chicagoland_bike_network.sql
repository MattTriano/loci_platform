create table raw_data.osmnx_bike_network_nodes (
    "osmid" bigint not null,
    "latitude" double precision,
    "longitude" double precision,
    "street_count" integer,
    "highway" text,
    "ref" text,
    "geom" geometry(Point, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.osmnx_bike_network_nodes
    add constraint uq_osmnx_bike_network_nodes_entity_hash
    unique ("osmid", "record_hash");
create index ix_osmnx_bike_network_nodes_current
    on raw_data.osmnx_bike_network_nodes ("osmid")
    where "valid_to" is null;
create index ix_osmnx_bike_network_nodes_geom
    on raw_data.osmnx_bike_network_nodes using gist (geom);

create table raw_data.osmnx_bike_network_edges (
    "u" bigint not null,
    "v" bigint not null,
    "key" integer not null,
    "osmid" text,
    "name" text,
    "highway" text,
    "oneway" boolean,
    "length_m" double precision,
    "maxspeed" text,
    "surface" text,
    "cycleway" text,
    "cycleway_right" text,
    "cycleway_left" text,
    "bicycle" text,
    "lanes" text,
    "width" text,
    "lit" text,
    "access" text,
    "bridge" text,
    "tunnel" text,
    "geom" geometry(LineString, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.osmnx_bike_network_edges
    add constraint uq_osmnx_bike_network_edges_entity_hash
    unique ("u", "v", "key", "record_hash");
create index ix_osmnx_bike_network_edges_current
    on raw_data.osmnx_bike_network_edges ("u", "v", "key")
    where "valid_to" is null;
create index ix_osmnx_bike_network_edges_geom
    on raw_data.osmnx_bike_network_edges using gist (geom);
create index ix_osmnx_bike_network_edges_u
    on raw_data.osmnx_bike_network_edges (u)
    where "valid_to" is null;
create index ix_osmnx_bike_network_edges_v
    on raw_data.osmnx_bike_network_edges (v)
    where "valid_to" is null;
