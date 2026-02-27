create table raw_data.osm_nodes (
    "osm_id" bigint not null,
    "region_id" text not null,
    "tags" jsonb,
    "geom" geometry(Point, 4326),
    "osm_timestamp" timestamptz,
    "osm_version" integer,
    "osm_changeset" bigint,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.osm_nodes
    add constraint uq_osm_nodes_entity_hash
    unique ("osm_id", "region_id", "record_hash");
create index ix_osm_nodes_current
    on raw_data.osm_nodes ("osm_id", "region_id")
    where "valid_to" is null;
create index ix_osm_nodes_geom
    on raw_data.osm_nodes using gist ("geom");


create table raw_data.osm_ways (
    "osm_id" bigint not null,
    "region_id" text not null,
    "tags" jsonb,
    "geom" geometry(Geometry, 4326),
    "osm_timestamp" timestamptz,
    "osm_version" integer,
    "osm_changeset" bigint,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.osm_ways
    add constraint uq_osm_ways_entity_hash
    unique ("osm_id", "region_id", "record_hash");
create index ix_osm_ways_current
    on raw_data.osm_ways ("osm_id", "region_id")
    where "valid_to" is null;
create index ix_osm_ways_geom
    on raw_data.osm_ways using gist ("geom");


create table raw_data.osm_relations (
    "osm_id" bigint not null,
    "region_id" text not null,
    "tags" jsonb,
    "members" jsonb,
    "osm_timestamp" timestamptz,
    "osm_version" integer,
    "osm_changeset" bigint,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.osm_relations
    add constraint uq_osm_relations_entity_hash
    unique ("osm_id", "region_id", "record_hash");
create index ix_osm_relations_current
    on raw_data.osm_relations ("osm_id", "region_id")
    where "valid_to" is null;
