create table if not exists raw_data.boston_osm_bike_parking (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
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


create table if not exists raw_data.boston_osm_transit (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "public_transport" text,
    "railway" text,
    "highway" text,
    "amenity" text,
    "network" text,
    "operator" text,
    "ref" text,
    "wheelchair" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.boston_osm_transit
    add constraint uq_boston_osm_transit_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_boston_osm_transit_current
    on raw_data.boston_osm_transit ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_boston_osm_transit_geom
    on raw_data.boston_osm_transit using gist (geom)
    where valid_to is null;





create table if not exists raw_data.chicago_osm_bars (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "amenity" text,
    "brand" text,
    "cuisine" text,
    "microbrewery" text,
    "craft_beer" text,
    "food" text,
    "outdoor_seating" text,
    "smoking" text,
    "opening_hours" text,
    "website" text,
    "phone" text,
    "addr_housenumber" text,
    "addr_street" text,
    "addr_city" text,
    "addr_postcode" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_bars
    add constraint uq_chicago_osm_bars_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_bars_current
    on raw_data.chicago_osm_bars ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_bars_geom
    on raw_data.chicago_osm_bars using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_bike_parking (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
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


create table if not exists raw_data.chicago_osm_cafes (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "brand" text,
    "cuisine" text,
    "takeaway" text,
    "outdoor_seating" text,
    "internet_access" text,
    "opening_hours" text,
    "website" text,
    "phone" text,
    "addr_housenumber" text,
    "addr_street" text,
    "addr_city" text,
    "addr_postcode" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_cafes
    add constraint uq_chicago_osm_cafes_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_cafes_current
    on raw_data.chicago_osm_cafes ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_cafes_geom
    on raw_data.chicago_osm_cafes using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_clothing (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "shop" text,
    "brand" text,
    "second_hand" text,
    "clothes" text,
    "opening_hours" text,
    "website" text,
    "phone" text,
    "addr_housenumber" text,
    "addr_street" text,
    "addr_city" text,
    "addr_postcode" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_clothing
    add constraint uq_chicago_osm_clothing_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_clothing_current
    on raw_data.chicago_osm_clothing ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_clothing_geom
    on raw_data.chicago_osm_clothing using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_culture (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "amenity" text,
    "tourism" text,
    "shop" text,
    "craft" text,
    "fee" text,
    "opening_hours" text,
    "website" text,
    "phone" text,
    "wheelchair" text,
    "addr_housenumber" text,
    "addr_street" text,
    "addr_city" text,
    "addr_postcode" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_culture
    add constraint uq_chicago_osm_culture_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_culture_current
    on raw_data.chicago_osm_culture ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_culture_geom
    on raw_data.chicago_osm_culture using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_fitness (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "leisure" text,
    "sport" text,
    "fitness_station" text,
    "access" text,
    "fee" text,
    "opening_hours" text,
    "website" text,
    "phone" text,
    "addr_housenumber" text,
    "addr_street" text,
    "addr_city" text,
    "addr_postcode" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_fitness
    add constraint uq_chicago_osm_fitness_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_fitness_current
    on raw_data.chicago_osm_fitness ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_fitness_geom
    on raw_data.chicago_osm_fitness using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_food_and_drink (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "amenity" text,
    "brand" text,
    "cuisine" text,
    "takeaway" text,
    "delivery" text,
    "outdoor_seating" text,
    "internet_access" text,
    "opening_hours" text,
    "website" text,
    "phone" text,
    "microbrewery" text,
    "craft_beer" text,
    "food" text,
    "addr_housenumber" text,
    "addr_street" text,
    "addr_city" text,
    "addr_postcode" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_food_and_drink
    add constraint uq_chicago_osm_food_and_drink_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_food_and_drink_current
    on raw_data.chicago_osm_food_and_drink ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_food_and_drink_geom
    on raw_data.chicago_osm_food_and_drink using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_food_retail (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "shop" text,
    "amenity" text,
    "brand" text,
    "organic" text,
    "opening_hours" text,
    "website" text,
    "phone" text,
    "addr_housenumber" text,
    "addr_street" text,
    "addr_city" text,
    "addr_postcode" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_food_retail
    add constraint uq_chicago_osm_food_retail_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_food_retail_current
    on raw_data.chicago_osm_food_retail ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_food_retail_geom
    on raw_data.chicago_osm_food_retail using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_transit (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "public_transport" text,
    "railway" text,
    "highway" text,
    "amenity" text,
    "network" text,
    "operator" text,
    "ref" text,
    "wheelchair" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_transit
    add constraint uq_chicago_osm_transit_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_transit_current
    on raw_data.chicago_osm_transit ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_transit_geom
    on raw_data.chicago_osm_transit using gist (geom)
    where valid_to is null;


create table if not exists raw_data.chicago_osm_trees (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "species" text,
    "species_en" text,
    "genus" text,
    "genus_en" text,
    "leaf_type" text,
    "leaf_cycle" text,
    "height" text,
    "circumference" text,
    "diameter_crown" text,
    "denotation" text,
    "ref" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_osm_trees
    add constraint uq_chicago_osm_trees_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_chicago_osm_trees_current
    on raw_data.chicago_osm_trees ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_chicago_osm_trees_geom
    on raw_data.chicago_osm_trees using gist (geom)
    where valid_to is null;


create table if not exists raw_data.detroit_osm_bike_parking (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
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


create table if not exists raw_data.detroit_osm_transit (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "public_transport" text,
    "railway" text,
    "highway" text,
    "amenity" text,
    "network" text,
    "operator" text,
    "ref" text,
    "wheelchair" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.detroit_osm_transit
    add constraint uq_detroit_osm_transit_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_detroit_osm_transit_current
    on raw_data.detroit_osm_transit ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_detroit_osm_transit_geom
    on raw_data.detroit_osm_transit using gist (geom)
    where valid_to is null;


create table if not exists raw_data.madison_osm_bike_parking (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
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


create table if not exists raw_data.madison_osm_transit (
    "osm_type" text not null,
    "osm_id" bigint not null,
    "osm_version" integer,
    "osm_timestamp" timestamptz,
    "geom" geometry(Geometry, 4326),
    "tags" jsonb,
    "name" text,
    "public_transport" text,
    "railway" text,
    "highway" text,
    "amenity" text,
    "network" text,
    "operator" text,
    "ref" text,
    "wheelchair" text,
    "ingested_at" timestamptz not null,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.madison_osm_transit
    add constraint uq_madison_osm_transit_entity_hash
    unique ("osm_type", "osm_id", "record_hash");
create index if not exists ix_madison_osm_transit_current
    on raw_data.madison_osm_transit ("osm_type", "osm_id")
    where valid_to is null;
create index if not exists ix_madison_osm_transit_geom
    on raw_data.madison_osm_transit using gist (geom)
    where valid_to is null;
