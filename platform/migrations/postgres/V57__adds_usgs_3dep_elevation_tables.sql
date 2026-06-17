create table if not exists raw_data.boston_3dep_elevation (
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
alter table raw_data.boston_3dep_elevation
    add constraint uq_boston_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_boston_3dep_elevation_current
    on raw_data.boston_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_boston_3dep_elevation_rast
    on raw_data.boston_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;


create table if not exists raw_data.chicago_3dep_elevation (
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
alter table raw_data.chicago_3dep_elevation
    add constraint uq_chicago_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_chicago_3dep_elevation_current
    on raw_data.chicago_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_chicago_3dep_elevation_rast
    on raw_data.chicago_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;


create table if not exists raw_data.dc_3dep_elevation (
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
alter table raw_data.dc_3dep_elevation
    add constraint uq_dc_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_dc_3dep_elevation_current
    on raw_data.dc_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_dc_3dep_elevation_rast
    on raw_data.dc_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;


create table if not exists raw_data.denver_3dep_elevation (
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
alter table raw_data.denver_3dep_elevation
    add constraint uq_denver_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_denver_3dep_elevation_current
    on raw_data.denver_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_denver_3dep_elevation_rast
    on raw_data.denver_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;


create table if not exists raw_data.detroit_3dep_elevation (
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
alter table raw_data.detroit_3dep_elevation
    add constraint uq_detroit_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_detroit_3dep_elevation_current
    on raw_data.detroit_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_detroit_3dep_elevation_rast
    on raw_data.detroit_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;


create table if not exists raw_data.madison_3dep_elevation (
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
alter table raw_data.madison_3dep_elevation
    add constraint uq_madison_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_madison_3dep_elevation_current
    on raw_data.madison_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_madison_3dep_elevation_rast
    on raw_data.madison_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;


create table if not exists raw_data.nola_3dep_elevation (
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
alter table raw_data.nola_3dep_elevation
    add constraint uq_nola_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_nola_3dep_elevation_current
    on raw_data.nola_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_nola_3dep_elevation_rast
    on raw_data.nola_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;


create table if not exists raw_data.portland_3dep_elevation (
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
alter table raw_data.portland_3dep_elevation
    add constraint uq_portland_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_portland_3dep_elevation_current
    on raw_data.portland_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_portland_3dep_elevation_rast
    on raw_data.portland_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;


create table if not exists raw_data.sf_3dep_elevation (
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
alter table raw_data.sf_3dep_elevation
    add constraint uq_sf_3dep_elevation_entity_hash
    unique ("tile_id", "record_hash");
create index if not exists ix_sf_3dep_elevation_current
    on raw_data.sf_3dep_elevation ("tile_id")
    where "valid_to" is null;
create index if not exists ix_sf_3dep_elevation_rast
    on raw_data.sf_3dep_elevation using gist (ST_ConvexHull(rast))
    where "valid_to" is null;
