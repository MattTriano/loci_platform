-- 1. truncate and reload
truncate table raw_data.chicago_homicide_and_non_fatal_shooting_victimizations;

-- 2. add scd2 columns
alter table raw_data.chicago_homicide_and_non_fatal_shooting_victimizations
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;

-- 3. unique constraint: same entity + same hash = same version, skip it
alter table raw_data.chicago_homicide_and_non_fatal_shooting_victimizations
    add constraint uq_chicago_homicide_and_non_fatal_shooting_victimizations_entity_hash
    unique (unique_id, record_hash);

-- 4. index for efficient "get current version" queries
create index if not exists ix_chicago_homicide_and_non_fatal_shooting_victimizations_current
    on raw_data.chicago_homicide_and_non_fatal_shooting_victimizations (unique_id)
    where valid_to is null;
