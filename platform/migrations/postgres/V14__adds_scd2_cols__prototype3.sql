-- 1. truncate and reload
truncate table raw_data.chicago_311_service_requests;

-- 2. add scd2 columns
alter table raw_data.chicago_311_service_requests
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;

-- 3. unique constraint: same entity + same hash = same version, skip it
alter table raw_data.chicago_311_service_requests
    add constraint uq_chicago_311_service_requests_entity_hash
    unique (sr_number, record_hash);

-- 4. index for efficient "get current version" queries
create index if not exists ix_chicago_311_service_requests_current
    on raw_data.chicago_311_service_requests (sr_number)
    where valid_to is null;
