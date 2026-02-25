create table raw_data.tiger_states (
    "region" text,
    "division" text,
    "statefp" text,
    "statens" text,
    "geoid" text,
    "geoidfq" text,
    "stusps" text,
    "name" text,
    "lsad" text,
    "mtfcc" text,
    "funcstat" text,
    "aland" bigint,
    "awater" bigint,
    "intptlat" text,
    "intptlon" text,
    "vintage" integer not null,
    "geom" geometry(MultiPolygon, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_states
    add constraint uq_tiger_states_entity_hash
    unique ("geoid", "vintage", "record_hash");
create index ix_tiger_states_current
    on raw_data.tiger_states ("geoid", "vintage")
    where "valid_to" is null;
create index ix_tiger_states_geom
    on raw_data.tiger_states using gist ("geom");


create table raw_data.tiger_counties (
    "statefp" text,
    "countyfp" text,
    "countyns" text,
    "geoid" text,
    "geoidfq" text,
    "name" text,
    "namelsad" text,
    "lsad" text,
    "classfp" text,
    "mtfcc" text,
    "csafp" text,
    "cbsafp" text,
    "metdivfp" text,
    "funcstat" text,
    "aland" bigint,
    "awater" bigint,
    "intptlat" text,
    "intptlon" text,
    "vintage" integer not null,
    "geom" geometry(MultiPolygon, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_counties
    add constraint uq_tiger_counties_entity_hash
    unique ("geoid", "vintage", "record_hash");
create index ix_tiger_counties_current
    on raw_data.tiger_counties ("geoid", "vintage")
    where "valid_to" is null;
create index ix_tiger_counties_geom
    on raw_data.tiger_counties using gist ("geom");


create table raw_data.tiger_zcta (
    "zcta5ce20" text,
    "geoid20" text,
    "geoidfq20" text,
    "classfp20" text,
    "mtfcc20" text,
    "funcstat20" text,
    "aland20" bigint,
    "awater20" bigint,
    "intptlat20" text,
    "intptlon20" text,
    "vintage" integer not null,
    "geom" geometry(MultiPolygon, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_zcta
    add constraint uq_tiger_zcta_entity_hash
    unique ("geoid20", "vintage", "record_hash");
create index ix_tiger_zcta_current
    on raw_data.tiger_zcta ("geoid20", "vintage")
    where "valid_to" is null;
create index ix_tiger_zcta_geom
    on raw_data.tiger_zcta using gist ("geom");



create table raw_data.tiger_tracts (
    "statefp" text,
    "countyfp" text,
    "tractce" text,
    "geoid" text,
    "geoidfq" text,
    "name" text,
    "namelsad" text,
    "mtfcc" text,
    "funcstat" text,
    "aland" bigint,
    "awater" bigint,
    "intptlat" text,
    "intptlon" text,
    "vintage" integer not null,
    "geom" geometry(MultiPolygon, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_tracts
    add constraint uq_tiger_tracts_entity_hash
    unique ("geoid", "vintage", "record_hash");
create index ix_tiger_tracts_current
    on raw_data.tiger_tracts ("geoid", "vintage")
    where "valid_to" is null;
create index ix_tiger_tracts_geom
    on raw_data.tiger_tracts using gist ("geom");


create table raw_data.tiger_block_groups (
    "statefp" text,
    "countyfp" text,
    "tractce" text,
    "blkgrpce" text,
    "geoid" text,
    "geoidfq" text,
    "namelsad" text,
    "mtfcc" text,
    "funcstat" text,
    "aland" bigint,
    "awater" bigint,
    "intptlat" text,
    "intptlon" text,
    "vintage" integer not null,
    "geom" geometry(MultiPolygon, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_block_groups
    add constraint uq_tiger_block_groups_entity_hash
    unique ("geoid", "vintage", "record_hash");
create index ix_tiger_block_groups_current
    on raw_data.tiger_block_groups ("geoid", "vintage")
    where "valid_to" is null;
create index ix_tiger_block_groups_geom
    on raw_data.tiger_block_groups using gist ("geom");


create table raw_data.tiger_coastline (
    "name" text,
    "mtfcc" text,
    "vintage" integer not null,
    "geom" geometry(MultiLineString, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);
create index ix_tiger_coastline_geom
    on raw_data.tiger_coastline using gist ("geom");


create table raw_data.tiger_areawater (
    "ansicode" text,
    "hydroid" text,
    "fullname" text,
    "mtfcc" text,
    "aland" bigint,
    "awater" bigint,
    "intptlat" text,
    "intptlon" text,
    "statefp" text,
    "countyfp" text,
    "vintage" integer not null,
    "geom" geometry(MultiPolygon, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_areawater
    add constraint uq_tiger_areawater_entity_hash
    unique ("hydroid", "vintage", "record_hash");
create index ix_tiger_areawater_current
    on raw_data.tiger_areawater ("hydroid", "vintage")
    where "valid_to" is null;
create index ix_tiger_areawater_geom
    on raw_data.tiger_areawater using gist ("geom");


create table raw_data.tiger_linear_water (
    "ansicode" text,
    "linearid" text,
    "fullname" text,
    "artpath" text,
    "mtfcc" text,
    "statefp" text,
    "countyfp" text,
    "vintage" integer not null,
    "geom" geometry(MultiLineString, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_linear_water
    add constraint uq_tiger_linear_water_entity_hash
    unique ("linearid", "vintage", "record_hash");
create index ix_tiger_linear_water_current
    on raw_data.tiger_linear_water ("linearid", "vintage")
    where "valid_to" is null;
create index ix_tiger_linear_water_geom
    on raw_data.tiger_linear_water using gist ("geom");


create table raw_data.tiger_addrs (
    "tlid" bigint,
    "fromhn" text,
    "tohn" text,
    "side" text,
    "zip" text,
    "plus4" text,
    "fromtyp" text,
    "totyp" text,
    "arid" text,
    "mtfcc" text,
    "statefp" text,
    "countyfp" text,
    "vintage" integer not null,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_addrs
    add constraint uq_tiger_addrs_entity_hash
    unique ("tlid", "vintage", "record_hash");
create index ix_tiger_addrs_current
    on raw_data.tiger_addrs ("tlid", "vintage")
    where "valid_to" is null;


create table raw_data.tiger_railroads (
    "linearid" text,
    "fullname" text,
    "mtfcc" text,
    "vintage" integer not null,
    "geom" geometry(MultiLineString, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_railroads
    add constraint uq_tiger_railroads_entity_hash
    unique ("linearid", "vintage", "record_hash");
create index ix_tiger_railroads_current
    on raw_data.tiger_railroads ("linearid", "vintage")
    where "valid_to" is null;
create index ix_tiger_railroads_geom
    on raw_data.tiger_railroads using gist ("geom");


create table raw_data.tiger_all_roads (
    "linearid" text,
    "fullname" text,
    "rttyp" text,
    "mtfcc" text,
    "statefp" text,
    "countyfp" text,
    "vintage" integer not null,
    "geom" geometry(MultiLineString, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_all_roads
    add constraint uq_tiger_all_roads_entity_hash
    unique ("linearid", "vintage", "record_hash");
create index ix_tiger_all_roads_current
    on raw_data.tiger_all_roads ("linearid", "vintage")
    where "valid_to" is null;
create index ix_tiger_all_roads_geom
    on raw_data.tiger_all_roads using gist ("geom");


create table raw_data.tiger_primary_roads (
    "linearid" text,
    "fullname" text,
    "rttyp" text,
    "mtfcc" text,
    "vintage" integer not null,
    "geom" geometry(MultiLineString, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_primary_roads
    add constraint uq_tiger_primary_roads_entity_hash
    unique ("linearid", "vintage", "record_hash");
create index ix_tiger_primary_roads_current
    on raw_data.tiger_primary_roads ("linearid", "vintage")
    where "valid_to" is null;
create index ix_tiger_primary_roads_geom
    on raw_data.tiger_primary_roads using gist ("geom");


create table raw_data.tiger_primary_secondary_roads (
    "linearid" text,
    "fullname" text,
    "rttyp" text,
    "mtfcc" text,
    "vintage" integer not null,
    "geom" geometry(MultiLineString, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.tiger_primary_secondary_roads
    add constraint uq_tiger_primary_secondary_roads_entity_hash
    unique ("linearid", "vintage", "record_hash");
create index ix_tiger_primary_secondary_roads_current
    on raw_data.tiger_primary_secondary_roads ("linearid", "vintage")
    where "valid_to" is null;
create index ix_tiger_primary_secondary_roads_geom
    on raw_data.tiger_primary_secondary_roads using gist ("geom");
