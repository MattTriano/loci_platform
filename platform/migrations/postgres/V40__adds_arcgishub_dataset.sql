create table raw_data.detroit_boundary (
    "fid" bigint,
    "acres" double precision,
    "cnty_code" text,
    "fipscode" text,
    "fipsnum" double precision,
    "label" text,
    "layout" text,
    "link" text,
    "name" text,
    "objectid" double precision,
    "peninsula" text,
    "shape_area" double precision,
    "shape_leng" double precision,
    "sqkm" double precision,
    "sqmiles" double precision,
    "type" text,
    "ver" text,
    "shape__area" double precision,
    "shape__length" double precision,
    "globalid" text,
    "creationdate" timestamptz,
    "creator" text,
    "editdate" timestamptz,
    "editor" text,
    "geom" geometry(MultiPolygon, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.detroit_boundary
    add constraint uq_detroit_boundary_entity_hash
    unique ("fid", "record_hash");
create index ix_detroit_boundary_current
    on raw_data.detroit_boundary ("fid")
    where "valid_to" is null;
create index ix_detroit_boundary_geom
    on raw_data.detroit_boundary using gist ("geom");
