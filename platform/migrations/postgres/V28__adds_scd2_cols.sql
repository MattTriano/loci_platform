truncate table raw_data.chicago_bike_racks;
alter table raw_data.chicago_bike_racks
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_bike_racks
    add constraint uq_chicago_bike_racks_entity_hash
    unique (socrata_id, record_hash);
create index if not exists ix_chicago_bike_racks_current
    on raw_data.chicago_bike_racks (socrata_id)
    where valid_to is null;
