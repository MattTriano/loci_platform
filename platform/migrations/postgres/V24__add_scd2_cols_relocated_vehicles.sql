truncate table raw_data.chicago_relocated_vehicles;
alter table raw_data.chicago_relocated_vehicles
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_relocated_vehicles
    add constraint uq_chicago_relocated_vehicles_entity_hash
    unique (service_request_number, record_hash);
create index if not exists ix_chicago_relocated_vehicles_current
    on raw_data.chicago_relocated_vehicles (service_request_number)
    where valid_to is null;
