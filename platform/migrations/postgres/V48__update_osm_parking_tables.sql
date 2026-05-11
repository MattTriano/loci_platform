drop table if exists raw_data.chicago_osm_bike_parking;
create table if not exists raw_data.chicago_osm_bike_parking (
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
alter table raw_data.chicago_osm_bike_parking
    add constraint uq_chicago_osm_bike_parking_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_bike_parking_current
    on raw_data.chicago_osm_bike_parking ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_bike_parking_geom
    on raw_data.chicago_osm_bike_parking using gist (geom)
    where valid_to is null;


drop table raw_data.boston_osm_bike_parking;
create table if not exists raw_data.boston_osm_bike_parking (
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
alter table raw_data.boston_osm_bike_parking
    add constraint uq_boston_osm_bike_parking_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_boston_osm_bike_parking_current
    on raw_data.boston_osm_bike_parking ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_boston_osm_bike_parking_geom
    on raw_data.boston_osm_bike_parking using gist (geom)
    where valid_to is null;


drop table raw_data.detroit_osm_bike_parking;
create table if not exists raw_data.detroit_osm_bike_parking (
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
alter table raw_data.detroit_osm_bike_parking
    add constraint uq_detroit_osm_bike_parking_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_detroit_osm_bike_parking_current
    on raw_data.detroit_osm_bike_parking ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_detroit_osm_bike_parking_geom
    on raw_data.detroit_osm_bike_parking using gist (geom)
    where valid_to is null;


drop table raw_data.madison_osm_bike_parking;
create table if not exists raw_data.madison_osm_bike_parking (
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
alter table raw_data.madison_osm_bike_parking
    add constraint uq_madison_osm_bike_parking_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_madison_osm_bike_parking_current
    on raw_data.madison_osm_bike_parking ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_madison_osm_bike_parking_geom
    on raw_data.madison_osm_bike_parking using gist (geom)
    where valid_to is null;
