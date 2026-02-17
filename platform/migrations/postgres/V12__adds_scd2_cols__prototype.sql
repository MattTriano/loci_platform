-- 1. add scd2 columns
alter table raw_data.cook_county_parcel_addresses
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;

-- 2. unique constraint: same entity + same hash = same version, skip it
alter table raw_data.cook_county_parcel_addresses
    add constraint uq_cook_county_parcel_addresses_entity_hash
    unique (row_id, record_hash);

-- 3. index for efficient "get current version" queries
create index if not exists ix_cook_county_parcel_addresses_current
    on raw_data.cook_county_parcel_addresses (row_id)
    where valid_to is null;

-- 4. truncate and reload
truncate table raw_data.cook_county_parcel_addresses;
