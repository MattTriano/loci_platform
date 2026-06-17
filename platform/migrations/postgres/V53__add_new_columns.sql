alter table raw_data.cook_county_parcel_addresses
    add column if not exists "owner_address_name"      text,
    add column if not exists "owner_address_full"      text,
    add column if not exists "owner_address_city_name" text,
    add column if not exists "owner_address_state"     text,
    add column if not exists "owner_address_zipcode_1" text;


alter table raw_data.chicago_traffic_crashes_crashes
    add column if not exists "idot_control_no" text;
