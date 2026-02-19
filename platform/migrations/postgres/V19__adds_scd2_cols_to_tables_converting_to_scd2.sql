truncate table raw_data.chicago_traffic_crashes_people;
alter table raw_data.chicago_traffic_crashes_people
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_traffic_crashes_people
    add constraint uq_chicago_traffic_crashes_people_entity_hash
    unique (person_id, record_hash);
create index if not exists ix_chicago_traffic_crashes_people_current
    on raw_data.chicago_traffic_crashes_people (person_id)
    where valid_to is null;

truncate table raw_data.chicago_traffic_crashes_vehicles;
alter table raw_data.chicago_traffic_crashes_vehicles
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_traffic_crashes_vehicles
    add constraint uq_chicago_traffic_crashes_vehicles_entity_hash
    unique (crash_unit_id, record_hash);
create index if not exists ix_chicago_traffic_crashes_vehicles_current
    on raw_data.chicago_traffic_crashes_vehicles (crash_unit_id)
    where valid_to is null;
