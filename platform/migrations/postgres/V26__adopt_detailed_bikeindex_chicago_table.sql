drop table raw_data.bikeindex_chicago_stolen_bikes;

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
alter table raw_data.bikeindex_chicago_stolen_bikes
    add constraint uq_bikeindex_chicago_stolen_bikes_entity_hash
    unique ("id", "record_hash");
create index ix_bikeindex_chicago_stolen_bikes_current
    on raw_data.bikeindex_chicago_stolen_bikes ("id")
    where "valid_to" is null;
