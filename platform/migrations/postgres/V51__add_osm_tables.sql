alter table raw_data.new_orleans_osm_bike_network_edges rename to nola_osm_bike_network_edges;
alter table raw_data.new_orleans_osm_bike_network_nodes rename to nola_osm_bike_network_nodes;

create table if not exists raw_data.nola_osm_bike_parking (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "node_ids" bigint[],
    "name" text,
    "bicycle_parking" text,
    "capacity" text,
    "covered" text,
    "access" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.nola_osm_bike_parking
    add constraint uq_nola_osm_bike_parking_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_nola_osm_bike_parking_current
    on raw_data.nola_osm_bike_parking ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_nola_osm_bike_parking_geom
    on raw_data.nola_osm_bike_parking using gist (geom)
    where valid_to is null;


create table if not exists raw_data.nyc_osm_bike_network_edges (
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
alter table raw_data.nyc_osm_bike_network_edges
    add constraint uq_nyc_osm_bike_network_edges_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_nyc_osm_bike_network_edges_current
    on raw_data.nyc_osm_bike_network_edges ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_nyc_osm_bike_network_edges_geom
    on raw_data.nyc_osm_bike_network_edges using gist (geom)
    where valid_to is null;


create table if not exists raw_data.nyc_osm_bike_network_nodes (
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
alter table raw_data.nyc_osm_bike_network_nodes
    add constraint uq_nyc_osm_bike_network_nodes_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_nyc_osm_bike_network_nodes_current
    on raw_data.nyc_osm_bike_network_nodes ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_nyc_osm_bike_network_nodes_geom
    on raw_data.nyc_osm_bike_network_nodes using gist (geom)
    where valid_to is null;


create table if not exists raw_data.nyc_osm_bike_parking (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "node_ids" bigint[],
    "name" text,
    "bicycle_parking" text,
    "capacity" text,
    "covered" text,
    "access" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.nyc_osm_bike_parking
    add constraint uq_nyc_osm_bike_parking_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_nyc_osm_bike_parking_current
    on raw_data.nyc_osm_bike_parking ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_nyc_osm_bike_parking_geom
    on raw_data.nyc_osm_bike_parking using gist (geom)
    where valid_to is null;


create table raw_data.nyc_bikeindex_bike_thefts (
    "id" integer not null,
    "title" text,
    "serial" text,
    "manufacturer_name" text,
    "frame_model" text,
    "frame_colors" jsonb,
    "year" integer,
    "stolen" boolean,
    "date_stolen" bigint,
    "description" text,
    "thumb" text,
    "url" text,
    "stolen_coordinates_lat" double precision,
    "stolen_coordinates_lon" double precision,
    "stolen_location" text,
    "latitude" double precision,
    "longitude" double precision,
    "theft_description" text,
    "locking_description" text,
    "lock_defeat_description" text,
    "police_report_number" text,
    "police_report_department" text,
    "propulsion_type_slug" text,
    "cycle_type_slug" text,
    "status" text,
    "registration_created_at" bigint,
    "registration_updated_at" bigint,
    "manufacturer_id" integer,
    "paint_description" text,
    "frame_size" text,
    "frame_material_slug" text,
    "handlebar_type_slug" text,
    "front_gear_type_slug" text,
    "rear_gear_type_slug" text,
    "rear_wheel_size_iso_bsd" integer,
    "front_wheel_size_iso_bsd" integer,
    "rear_tire_narrow" boolean,
    "front_tire_narrow" boolean,
    "extra_registration_number" text,
    "additional_registration" text,
    "components" jsonb,
    "public_images" jsonb,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.nyc_bikeindex_bike_thefts
    add constraint uq_nyc_bikeindex_bike_thefts_entity_hash
    unique ("id", "record_hash");
create index ix_nyc_bikeindex_bike_thefts_current
    on raw_data.nyc_bikeindex_bike_thefts ("id")
    where "valid_to" is null;
