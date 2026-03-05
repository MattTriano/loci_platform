create table raw_data.bikeindex_chicago_stolen_bikes (
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
    "latitude" double precision,
    "longitude" double precision,
    "stolen_location" text,
    "theft_description" text,
    "locking_description" text,
    "lock_defeat_description" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.bikeindex_chicago_stolen_bikes
    add constraint uq_bikeindex_chicago_stolen_bikes_entity_hash
    unique ("id", "record_hash");
create index ix_bikeindex_chicago_stolen_bikes_current
    on raw_data.bikeindex_chicago_stolen_bikes ("id")
    where "valid_to" is null;
