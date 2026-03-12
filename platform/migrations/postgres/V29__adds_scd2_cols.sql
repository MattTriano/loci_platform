truncate table raw_data.open_air_chicago_individual_measurements;
alter table raw_data.open_air_chicago_individual_measurements
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.open_air_chicago_individual_measurements
    add constraint uq_open_air_chicago_individual_measurements_entity_hash
    unique (record_id, record_hash);
create index if not exists ix_open_air_chicago_individual_measurements_current
    on raw_data.open_air_chicago_individual_measurements (record_id)
    where valid_to is null;
