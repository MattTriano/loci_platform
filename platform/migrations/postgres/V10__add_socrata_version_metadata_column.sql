alter table raw_data.chicago_towed_vehicles
    add column if not exists socrata_version text;

alter table raw_data.chicago_traffic_crashes_crashes
    add column if not exists socrata_version text;

alter table raw_data.chicago_traffic_crashes_people
    add column if not exists socrata_version text;

alter table raw_data.chicago_traffic_crashes_vehicles
    add column if not exists socrata_version text;

alter table raw_data.chicago_relocated_vehicles
    add column if not exists socrata_version text;

alter table raw_data.chicago_311_service_requests
    add column if not exists socrata_version text;

alter table raw_data.open_air_chicago_individual_measurements
    add column if not exists socrata_version text;

alter table raw_data.chicago_park_district_activities
    add column if not exists socrata_version text;

alter table raw_data.chicago_boundaries_zip_codes
    add column if not exists socrata_version text;

alter table raw_data.chicago_homicide_and_non_fatal_shooting_victimizations
    add column if not exists socrata_version text;

alter table raw_data.chicago_arrests
    add column if not exists socrata_version text;

alter table raw_data.cook_county_parcel_sales
    add column if not exists socrata_version text;

alter table raw_data.cook_county_parcel_addresses
    add column if not exists socrata_version text;

alter table raw_data.cook_county_commercial_valuation_data
    add column if not exists socrata_version text;

alter table raw_data.cook_county_neighborhood_boundaries
    add column if not exists socrata_version text;

alter table raw_data.cook_county_residential_condominium_unit_characteristics
    add column if not exists socrata_version text;

alter table raw_data.cook_county_assessed_parcel_values
    add column if not exists socrata_version text;

alter table raw_data.chicago_crimes
    add column if not exists socrata_version text;

alter table raw_data.chicago_divvy_bicycle_stations
    add column if not exists socrata_version text;

alter table raw_data.chicago_speed_camera_locations
    add column if not exists socrata_version text;

alter table raw_data.chicago_red_light_camera_locations
    add column if not exists socrata_version text;

alter table raw_data.chicago_red_light_camera_violations
    add column if not exists socrata_version text;

alter table raw_data.chicago_speed_camera_violations
    add column if not exists socrata_version text;

alter table raw_data.chicago_business_owners
    add column if not exists socrata_version text;

alter table raw_data.chicago_potholes_patched
    add column if not exists socrata_version text;

alter table raw_data.chicago_business_licenses
    add column if not exists socrata_version text;

alter table raw_data.chicago_sidewalk_cafe_permits
    add column if not exists socrata_version text;

alter table raw_data.chicago_food_inspections
    add column if not exists socrata_version text;

alter table raw_data.chicago_additional_dwelling_unit_preapproval_applications
    add column if not exists socrata_version text;

alter table raw_data.chicago_lending_equity_residential_lending
    add column if not exists socrata_version text;

alter table raw_data.cta_ridership_daily_boarding_totals
    add column if not exists socrata_version text;
