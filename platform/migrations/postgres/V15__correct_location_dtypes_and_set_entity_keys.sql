alter table raw_data.chicago_311_service_requests
  drop column location,
  add column location geometry(Point, 4326);

alter table raw_data.chicago_relocated_vehicles
  drop column relocated_from_location,
  add column relocated_from_location geometry(Point, 4326);

alter table raw_data.chicago_crimes
  drop column location,
  add column location geometry(Point, 4326);

alter table raw_data.chicago_red_light_camera_locations
  drop column location,
  add column location geometry(Point, 4326);

alter table raw_data.chicago_red_light_camera_violations
  drop column location,
  add column location geometry(Point, 4326);

alter table raw_data.chicago_speed_camera_locations
  drop column location,
  add column location geometry(Point, 4326);

alter table raw_data.chicago_speed_camera_violations
  drop column location,
  add column location geometry(Point, 4326);

alter table raw_data.chicago_business_licenses
  drop column location,
  add column location geometry(Point, 4326);

alter table raw_data.chicago_sidewalk_cafe_permits
  drop column location,
  add column location geometry(Point, 4326);

alter table raw_data.chicago_food_inspections
  drop column location,
  add column location geometry(Point, 4326);


truncate table raw_data.chicago_crimes;
alter table raw_data.chicago_crimes
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_crimes
    add constraint uq_chicago_crimes_entity_hash
    unique (id, record_hash);
create index if not exists ix_chicago_crimes_current
    on raw_data.chicago_crimes (id)
    where valid_to is null;

truncate table raw_data.chicago_sidewalk_cafe_permits;
alter table raw_data.chicago_sidewalk_cafe_permits
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_sidewalk_cafe_permits
    add constraint uq_chicago_sidewalk_cafe_permits_entity_hash
    unique (permit_number, record_hash);
create index if not exists ix_chicago_sidewalk_cafe_permits_current
    on raw_data.chicago_sidewalk_cafe_permits (permit_number)
    where valid_to is null;

truncate table raw_data.chicago_traffic_crashes_crashes;
alter table raw_data.chicago_traffic_crashes_crashes
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_traffic_crashes_crashes
    add constraint uq_chicago_traffic_crashes_crashes_entity_hash
    unique (crash_record_id, record_hash);
create index if not exists ix_chicago_traffic_crashes_crashes_current
    on raw_data.chicago_traffic_crashes_crashes (crash_record_id)
    where valid_to is null;

truncate table raw_data.cook_county_single_and_multi_family_improvement_characteristics;
alter table raw_data.cook_county_single_and_multi_family_improvement_characteristics
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.cook_county_single_and_multi_family_improvement_characteristics
    add constraint uq_cook_county_single_and_multi_family_improvement_characteristics_entity_hash
    unique (row_id, record_hash);
create index if not exists ix_cook_county_single_and_multi_family_improvement_characteristics_current
    on raw_data.cook_county_single_and_multi_family_improvement_characteristics (row_id)
    where valid_to is null;

truncate table raw_data.cook_county_residential_condominium_unit_characteristics;
alter table raw_data.cook_county_residential_condominium_unit_characteristics
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.cook_county_residential_condominium_unit_characteristics
    add constraint uq_cook_county_residential_condominium_unit_characteristics_entity_hash
    unique (row_id, record_hash);
create index if not exists ix_cook_county_residential_condominium_unit_characteristics_current
    on raw_data.cook_county_residential_condominium_unit_characteristics (row_id)
    where valid_to is null;

truncate table raw_data.cook_county_parcel_sales;
alter table raw_data.cook_county_parcel_sales
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.cook_county_parcel_sales
    add constraint uq_cook_county_parcel_sales_entity_hash
    unique (row_id, record_hash);
create index if not exists ix_cook_county_parcel_sales_current
    on raw_data.cook_county_parcel_sales (row_id)
    where valid_to is null;

truncate table raw_data.cook_county_assessed_parcel_values;
alter table raw_data.cook_county_assessed_parcel_values
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.cook_county_assessed_parcel_values
    add constraint uq_cook_county_assessed_parcel_values_entity_hash
    unique (row_id, record_hash);
create index if not exists ix_cook_county_assessed_parcel_values_current
    on raw_data.cook_county_assessed_parcel_values (row_id)
    where valid_to is null;

truncate table raw_data.chicago_building_permits;
alter table raw_data.chicago_building_permits
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_building_permits
    add constraint uq_chicago_building_permits_entity_hash
    unique (permit_, record_hash);
create index if not exists ix_chicago_building_permits_current
    on raw_data.chicago_building_permits (permit_)
    where valid_to is null;

truncate table raw_data.chicago_food_inspections;
alter table raw_data.chicago_food_inspections
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_food_inspections
    add constraint uq_chicago_food_inspections_entity_hash
    unique (inspection_id, record_hash);
create index if not exists ix_chicago_food_inspections_current
    on raw_data.chicago_food_inspections (inspection_id)
    where valid_to is null;

truncate table raw_data.chicago_additional_dwelling_unit_preapproval_applications;
alter table raw_data.chicago_additional_dwelling_unit_preapproval_applications
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_additional_dwelling_unit_preapproval_applications
    add constraint uq_chicago_additional_dwelling_unit_preapproval_applications_entity_hash
    unique (id, record_hash);
create index if not exists ix_chicago_additional_dwelling_unit_preapproval_applications_current
    on raw_data.chicago_additional_dwelling_unit_preapproval_applications (id)
    where valid_to is null;

truncate table raw_data.chicago_arrests;
alter table raw_data.chicago_arrests
    add column if not exists record_hash text not null,
    add column if not exists valid_from timestamptz not null default (now() at time zone 'utc'),
    add column if not exists valid_to timestamptz;
alter table raw_data.chicago_arrests
    add constraint uq_chicago_arrests_entity_hash
    unique (cb_no, record_hash);
create index if not exists ix_chicago_arrests_current
    on raw_data.chicago_arrests (cb_no)
    where valid_to is null;
