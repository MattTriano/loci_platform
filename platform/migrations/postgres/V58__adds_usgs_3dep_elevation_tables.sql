create table if not exists raw_data.nyc_3dep_elevation (
    "tile_id" text not null,
    "rast" raster not null,
    "checksum" text not null,
    "srid" integer not null,
    "min_x" double precision,
    "min_y" double precision,
    "max_x" double precision,
    "max_y" double precision,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.nyc_3dep_elevation
    add constraint uq_nyc_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_nyc_3dep_elevation_current
    on raw_data.nyc_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_nyc_3dep_elevation_rast
    on raw_data.nyc_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;
