create index if not exists idx_ingest_log_hwm_lookup
    on meta.ingest_log (source, dataset, status, completed_at desc)
    where  status = 'success';

create table if not exists raw_data.chicago_park_district_activities (
    "activity_id" text,
    "type" text,
    "title" text,
    "description" text,
    "start_date" timestamptz,
    "end_date" timestamptz,
    "date_notes" text,
    "season" text,
    "zone" text,
    "location_facility" text,
    "location_notes" text,
    "age_range" text,
    "activity_type" text,
    "category" text,
    "fee" numeric,
    "image_link" text,
    "movie_rating" text,
    "movie_title" text,
    "restrictions" text,
    "information_link" text,
    "registration_link" text,
    "registration_date" timestamptz,
    "event_cancelled" text,
    "address" text,
    "zip" text,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_8hcu_yrd4" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_park_district_activities."activity_id" is 'A unique ID for the record.';
comment on column raw_data.chicago_park_district_activities."type" is 'Whether the activity is an event or a program. Generally, events are single-day and programs are ongoing for multiple days.';
comment on column raw_data.chicago_park_district_activities."image_link" is 'A link to the image, if any, associated with the activity.';
comment on column raw_data.chicago_park_district_activities."movie_rating" is 'The MPA rating if the activity is a movie.';
comment on column raw_data.chicago_park_district_activities."information_link" is 'A link for more information about the activity, sometimes including a registration link.';
comment on column raw_data.chicago_park_district_activities."registration_link" is 'A link to register for the activity if there is not one on the Information page.';
comment on column raw_data.chicago_park_district_activities."registration_date" is 'The date registration opens. This column will be blank for all events.';
comment on column raw_data.chicago_park_district_activities."event_cancelled" is 'Whether an event has been cancelled. This column will be blank for all programs.';
comment on column raw_data.chicago_park_district_activities."location" is 'The location in a format that allows for mapping and other geographic functions on this data portal.';
comment on column raw_data.chicago_park_district_activities.":@computed_region_rpca_8um6" is 'This column was automatically created in order to record in what polygon from the dataset ''Boundaries - ZIP Codes'' (rpca-8um6) the point in column ''geo_location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_park_district_activities.":@computed_region_vrxf_vc4k" is 'This column was automatically created in order to record in what polygon from the dataset ''Community Areas'' (vrxf-vc4k) the point in column ''geo_location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_park_district_activities.":@computed_region_6mkv_f3dw" is 'This column was automatically created in order to record in what polygon from the dataset ''Zip Codes'' (6mkv-f3dw) the point in column ''geo_location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_park_district_activities.":@computed_region_bdys_3d7i" is 'This column was automatically created in order to record in what polygon from the dataset ''Census Tracts'' (bdys-3d7i) the point in column ''geo_location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_park_district_activities.":@computed_region_8hcu_yrd4" is 'This column was automatically created in order to record in what polygon from the dataset ''Wards 2023-'' (8hcu-yrd4) the point in column ''geo_location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';


create table if not exists raw_data.chicago_boundaries_zip_codes (
    "the_geom" geometry(MultiPolygon, 4326),
    "objectid" numeric,
    "zip" text,
    "shape_area" numeric,
    "shape_len" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);


create table if not exists raw_data.chicago_traffic_crashes_people (
    "person_id" text,
    "person_type" text,
    "crash_record_id" text,
    "vehicle_id" text,
    "crash_date" timestamptz,
    "seat_no" text,
    "city" text,
    "state" text,
    "zipcode" text,
    "sex" text,
    "age" numeric,
    "drivers_license_state" text,
    "drivers_license_class" text,
    "safety_equipment" text,
    "airbag_deployed" text,
    "ejection" text,
    "injury_classification" text,
    "hospital" text,
    "ems_agency" text,
    "ems_run_no" text,
    "driver_action" text,
    "driver_vision" text,
    "physical_condition" text,
    "pedpedal_action" text,
    "pedpedal_visibility" text,
    "pedpedal_location" text,
    "bac_result" text,
    "bac_result_value" numeric,
    "cell_phone_use" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_traffic_crashes_people."person_id" is 'A unique identifier for each person record. IDs starting with P indicate passengers. IDs starting with O indicate a person who was not a passenger in the vehicle (e.g., driver, pedestrian, cyclist, etc.).';
comment on column raw_data.chicago_traffic_crashes_people."person_type" is 'Type of roadway user involved in crash';
comment on column raw_data.chicago_traffic_crashes_people."crash_record_id" is 'This number can be used to link to the same crash in the Crashes and Vehicles datasets. This number also serves as a unique ID in the Crashes dataset.';
comment on column raw_data.chicago_traffic_crashes_people."vehicle_id" is 'The corresponding CRASH_UNIT_ID from the Vehicles dataset.';
comment on column raw_data.chicago_traffic_crashes_people."crash_date" is 'Date and time of crash as entered by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_people."seat_no" is 'Code for seating position of motor vehicle occupant:

1= driver, 2= center front, 3 = front passenger, 4 = second row left, 5 = second row center, 6 = second row right, 7 = enclosed passengers, 8 = exposed passengers, 9= unknown position, 10 = third row left, 11 = third row center, 12 = third row right';
comment on column raw_data.chicago_traffic_crashes_people."city" is 'City of residence of person involved in crash';
comment on column raw_data.chicago_traffic_crashes_people."state" is 'State of residence of person involved in crash';
comment on column raw_data.chicago_traffic_crashes_people."zipcode" is 'ZIP Code of residence of person involved in crash';
comment on column raw_data.chicago_traffic_crashes_people."sex" is 'Gender of person involved in crash, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_people."age" is 'Age of person involved in crash';
comment on column raw_data.chicago_traffic_crashes_people."drivers_license_state" is 'State issuing driver''s license of person involved in crash';
comment on column raw_data.chicago_traffic_crashes_people."drivers_license_class" is 'Class of driver''s license of person involved in crash';
comment on column raw_data.chicago_traffic_crashes_people."safety_equipment" is 'Safety equipment used by vehicle occupant in crash, if any';
comment on column raw_data.chicago_traffic_crashes_people."airbag_deployed" is 'Whether vehicle occupant airbag deployed as result of crash';
comment on column raw_data.chicago_traffic_crashes_people."ejection" is 'Whether vehicle occupant was ejected or extricated from the vehicle as a result of crash';
comment on column raw_data.chicago_traffic_crashes_people."injury_classification" is 'Severity of injury person sustained in the crash';
comment on column raw_data.chicago_traffic_crashes_people."hospital" is 'Hospital to which person injured in the crash was taken';
comment on column raw_data.chicago_traffic_crashes_people."ems_agency" is 'EMS agency who transported person injured in crash to the hospital';
comment on column raw_data.chicago_traffic_crashes_people."ems_run_no" is 'EMS agency run number';
comment on column raw_data.chicago_traffic_crashes_people."driver_action" is 'Driver action that contributed to the crash, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_people."driver_vision" is 'What, if any, objects obscured the driver’s vision at time of crash';
comment on column raw_data.chicago_traffic_crashes_people."physical_condition" is 'Driver’s apparent physical condition at time of crash, as observed by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_people."pedpedal_action" is 'Action of pedestrian or cyclist at the time of crash';
comment on column raw_data.chicago_traffic_crashes_people."pedpedal_visibility" is 'Visibility of pedestrian of cyclist safety equipment in use at time of crash';
comment on column raw_data.chicago_traffic_crashes_people."pedpedal_location" is 'Location of pedestrian or cyclist at the time of crash';
comment on column raw_data.chicago_traffic_crashes_people."bac_result" is 'Status of blood alcohol concentration testing for driver or other person involved in crash';
comment on column raw_data.chicago_traffic_crashes_people."bac_result_value" is 'Driver’s blood alcohol concentration test result (fatal crashes may include pedestrian or cyclist results)';
comment on column raw_data.chicago_traffic_crashes_people."cell_phone_use" is 'Whether person was/was not using cellphone at the time of the crash, as determined by the reporting officer';


create table if not exists raw_data.chicago_traffic_crashes_vehicles (
    "crash_unit_id" numeric,
    "crash_record_id" text,
    "crash_date" timestamptz,
    "unit_no" numeric,
    "unit_type" text,
    "num_passengers" numeric,
    "vehicle_id" numeric,
    "cmrc_veh_i" text,
    "make" text,
    "model" text,
    "lic_plate_state" text,
    "vehicle_year" numeric,
    "vehicle_defect" text,
    "vehicle_type" text,
    "vehicle_use" text,
    "travel_direction" text,
    "maneuver" text,
    "towed_i" text,
    "fire_i" text,
    "occupant_cnt" numeric,
    "exceed_speed_limit_i" text,
    "towed_by" text,
    "towed_to" text,
    "area_00_i" text,
    "area_01_i" text,
    "area_02_i" text,
    "area_03_i" text,
    "area_04_i" text,
    "area_05_i" text,
    "area_06_i" text,
    "area_07_i" text,
    "area_08_i" text,
    "area_09_i" text,
    "area_10_i" text,
    "area_11_i" text,
    "area_12_i" text,
    "area_99_i" text,
    "first_contact_point" text,
    "cmv_id" numeric,
    "usdot_no" text,
    "ccmc_no" text,
    "ilcc_no" text,
    "commercial_src" text,
    "gvwr" text,
    "carrier_name" text,
    "carrier_state" text,
    "carrier_city" text,
    "hazmat_placards_i" text,
    "hazmat_name" text,
    "un_no" text,
    "hazmat_present_i" text,
    "hazmat_report_i" text,
    "hazmat_report_no" text,
    "mcs_report_i" text,
    "mcs_report_no" text,
    "hazmat_vio_cause_crash_i" text,
    "mcs_vio_cause_crash_i" text,
    "idot_permit_no" text,
    "wide_load_i" text,
    "trailer1_width" text,
    "trailer2_width" text,
    "trailer1_length" numeric,
    "trailer2_length" numeric,
    "total_vehicle_length" numeric,
    "axle_cnt" numeric,
    "vehicle_config" text,
    "cargo_body_type" text,
    "load_type" text,
    "hazmat_out_of_service_i" text,
    "mcs_out_of_service_i" text,
    "hazmat_class" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_traffic_crashes_vehicles."crash_unit_id" is 'A unique identifier for each vehicle record.';
comment on column raw_data.chicago_traffic_crashes_vehicles."crash_record_id" is 'This number can be used to link to the same crash in the Crashes and People datasets. This number also serves as a unique ID in the Crashes dataset.';
comment on column raw_data.chicago_traffic_crashes_vehicles."crash_date" is 'Date and time of crash as entered by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_vehicles."unit_no" is 'A unique ID for each unit within a specific crash report.';
comment on column raw_data.chicago_traffic_crashes_vehicles."unit_type" is 'The type of unit';
comment on column raw_data.chicago_traffic_crashes_vehicles."num_passengers" is 'Number of passengers in the vehicle. The driver is not included. More information on passengers is in the People dataset.';
comment on column raw_data.chicago_traffic_crashes_vehicles."make" is 'The make (brand) of the vehicle, if relevant';
comment on column raw_data.chicago_traffic_crashes_vehicles."model" is 'The model of the vehicle, if relevant';
comment on column raw_data.chicago_traffic_crashes_vehicles."lic_plate_state" is 'The state issuing the license plate of the vehicle, if relevant';
comment on column raw_data.chicago_traffic_crashes_vehicles."vehicle_year" is 'The model year of the vehicle, if relevant';
comment on column raw_data.chicago_traffic_crashes_vehicles."vehicle_type" is 'The type of vehicle, if relevant';
comment on column raw_data.chicago_traffic_crashes_vehicles."vehicle_use" is 'The normal use of the vehicle, if relevant';
comment on column raw_data.chicago_traffic_crashes_vehicles."travel_direction" is 'The direction in which the unit was traveling prior to the crash, as determined by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_vehicles."maneuver" is 'The action the unit was taking prior to the crash, as determined by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_vehicles."towed_i" is 'Indicator of whether the vehicle was towed';
comment on column raw_data.chicago_traffic_crashes_vehicles."occupant_cnt" is 'The number of people in the unit, as determined by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_vehicles."exceed_speed_limit_i" is 'Indicator of whether the unit was speeding, as determined by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_vehicles."towed_by" is 'Entity that towed the unit, if relevant';
comment on column raw_data.chicago_traffic_crashes_vehicles."towed_to" is 'Location to which the unit was towed, if relevant';


create table if not exists raw_data.chicago_311_service_requests (
    "sr_number" text,
    "sr_type" text,
    "sr_short_code" text,
    "created_department" text,
    "owner_department" text,
    "status" text,
    "origin" text,
    "created_date" timestamptz,
    "last_modified_date" timestamptz,
    "closed_date" timestamptz,
    "street_address" text,
    "city" text,
    "state" text,
    "zip_code" text,
    "street_number" text,
    "street_direction" text,
    "street_name" text,
    "street_type" text,
    "duplicate" boolean,
    "legacy_record" boolean,
    "legacy_sr_number" text,
    "parent_sr_number" text,
    "community_area" numeric,
    "ward" numeric,
    "electrical_district" text,
    "electricity_grid" text,
    "police_sector" text,
    "police_district" text,
    "police_beat" text,
    "precinct" text,
    "sanitation_division_days" text,
    "created_hour" numeric,
    "created_day_of_week" numeric,
    "created_month" numeric,
    "x_coordinate" numeric,
    "y_coordinate" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" jsonb,
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    ":@computed_region_du4m_ji7t" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_311_service_requests."sr_short_code" is 'An internal code corresponding to the Service Request Type. This code allows for searching and filtering more easily than using the full SR_TYPE value.';
comment on column raw_data.chicago_311_service_requests."created_department" is 'The department, if any, that created the service request.';
comment on column raw_data.chicago_311_service_requests."owner_department" is 'The department with initial responsibility for the service request.';
comment on column raw_data.chicago_311_service_requests."origin" is 'How the request was opened. Some values, such as Generated In House and Mass Entry result from the City''s own operations.';
comment on column raw_data.chicago_311_service_requests."duplicate" is 'Is this request a duplicate of another request?';
comment on column raw_data.chicago_311_service_requests."legacy_record" is 'Did this request originate in the previous 311 system?';
comment on column raw_data.chicago_311_service_requests."legacy_sr_number" is 'If this request originated in the previous 311 system, the original Service Request Number.';
comment on column raw_data.chicago_311_service_requests."parent_sr_number" is 'Parent Service Request of the record if applicable. If the current Service Request record has been identified as a duplicate request, the record will be created as a child of the original request.';
comment on column raw_data.chicago_311_service_requests."created_hour" is 'The hour of the day component of CREATED_DATE.';
comment on column raw_data.chicago_311_service_requests."created_day_of_week" is 'The day of the week component of CREATED_DATE. Sunday=1';
comment on column raw_data.chicago_311_service_requests."created_month" is 'The month component of CREATED_DATE';
comment on column raw_data.chicago_311_service_requests."x_coordinate" is 'The x coordinate of the location in State Plane Illinois East NAD 1983 projection.';
comment on column raw_data.chicago_311_service_requests."y_coordinate" is 'The y coordinate of the location in State Plane Illinois East NAD 1983 projection.';
comment on column raw_data.chicago_311_service_requests."latitude" is 'The latitude of the location.';
comment on column raw_data.chicago_311_service_requests."longitude" is 'The longitude of the location.';
comment on column raw_data.chicago_311_service_requests."location" is 'The location in a format that allows for creation of maps and other geographic operations on this data portal.';
comment on column raw_data.chicago_311_service_requests.":@computed_region_du4m_ji7t" is 'This column was automatically created in order to record in what polygon from the dataset ''Boundaries - Wards (2023-)'' (du4m-ji7t) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';


create table if not exists raw_data.chicago_relocated_vehicles (
    "relocated_date" timestamptz,
    "make" text,
    "color" text,
    "plate" text,
    "state" text,
    "relocated_from_address_number" text,
    "relocated_from_street_direction" text,
    "relocated_from_street_name" text,
    "relocated_from_suffix" text,
    "relocated_to_address_number" text,
    "relocated_to_direction" text,
    "relocated_to_street_name" text,
    "relocated_to_suffix" text,
    "relocated_reason" text,
    "service_request_number" text,
    "relocated_from_x_coordinate" numeric,
    "relocated_from_y_coordinate" numeric,
    "relocated_from_latitude" numeric,
    "relocated_from_longitude" numeric,
    "relocated_from_location" jsonb,
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    ":@computed_region_awaf_s7ux" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_relocated_vehicles."relocated_from_x_coordinate" is 'The x coordinate of the location from which the vehicle was relocated, in State Plane Illinois East NAD 1983 projection.';
comment on column raw_data.chicago_relocated_vehicles."relocated_from_y_coordinate" is 'The y coordinate of the location from which the vehicle was relocated, in State Plane Illinois East NAD 1983 projection.';
comment on column raw_data.chicago_relocated_vehicles."relocated_from_latitude" is 'The latitude of the location from which the vehicle was relocated.';
comment on column raw_data.chicago_relocated_vehicles."relocated_from_longitude" is 'The longitude of the location from which the vehicle was relocated';
comment on column raw_data.chicago_relocated_vehicles."relocated_from_location" is 'The location from which the vehicle was relocated in a special format that allows for mapping on this data portal.';


create table if not exists raw_data.open_air_chicago_individual_measurements (
    "datasourceid" text,
    "time" timestamptz,
    "sensor_name" text,
    "atmpressureindividual_raw" numeric,
    "atmpressureindividual_value" numeric,
    "bcallsourcesindividual_raw" numeric,
    "bcallsourcesindividual_value" numeric,
    "bcbiomassindividual_raw" numeric,
    "bcbiomassindividual_value" numeric,
    "bcfossilfuelindividual_raw" numeric,
    "bcfossilfuelindividual_value" numeric,
    "bcspectralbindividual_raw" numeric,
    "bcspectralbindividual_value" numeric,
    "bcspectralgindividual_raw" numeric,
    "bcspectralgindividual_value" numeric,
    "bcspectralirindividual_raw" numeric,
    "bcspectralirindividual_value" numeric,
    "bcspectralrindividual_raw" numeric,
    "bcspectralrindividual_value" numeric,
    "bcspectraluvindividual_raw" numeric,
    "bcspectraluvindividual_value" numeric,
    "no2concindividual_raw" numeric,
    "no2concindividual_value" numeric,
    "pm2_5concmassindividual_raw" numeric,
    "pm2_5concmassindividual_value" numeric,
    "pm2_5concnumindividual_raw" numeric,
    "pm2_5concnumindividual_value" numeric,
    "relhumidambientindividual" numeric,
    "relhumidambientindividual_1" numeric,
    "relhumidinternalindividual" numeric,
    "relhumidinternalindividual_1" numeric,
    "temperatureambientindividual" numeric,
    "temperatureambientindividual_1" numeric,
    "temperatureinternalindividual" numeric,
    "temperatureinternalindividual_1" numeric,
    "winddirectionindividual_raw" numeric,
    "winddirectionindividual_value" numeric,
    "windspeedindividual_raw" numeric,
    "windspeedindividual_value" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    "record_id" text,
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_8hcu_yrd4" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.open_air_chicago_individual_measurements."datasourceid" is 'Sensor identification number';
comment on column raw_data.open_air_chicago_individual_measurements."time" is 'Date and time of sample, in UTC (not Chicago time)';
comment on column raw_data.open_air_chicago_individual_measurements."sensor_name" is 'Community area name and number of sensor in community area';
comment on column raw_data.open_air_chicago_individual_measurements."atmpressureindividual_raw" is 'Atmospheric pressure, Individual sample (atm), raw';
comment on column raw_data.open_air_chicago_individual_measurements."atmpressureindividual_value" is 'Atmospheric pressure, Individual sample (atm), value';
comment on column raw_data.open_air_chicago_individual_measurements."bcallsourcesindividual_raw" is 'Black carbon All sources mass concentration, Individual sample (ng/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."bcallsourcesindividual_value" is 'Black carbon All sources mass concentration, Individual sample (ng/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."bcbiomassindividual_raw" is 'Black carbon Biomass mass concentration, Individual sample (ng/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."bcbiomassindividual_value" is 'Black carbon Biomass mass concentration, Individual sample (ng/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."bcfossilfuelindividual_raw" is 'Black carbon Fossil fuel mass concentration, Individual sample (ng/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."bcfossilfuelindividual_value" is 'Black carbon Fossil fuel mass concentration, Individual sample (ng/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectralbindividual_raw" is 'Black carbon Spectral B mass concentration, Individual sample (ng/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectralbindividual_value" is 'Black carbon Spectral B mass concentration, Individual sample (ng/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectralgindividual_raw" is 'Black carbon Spectral G mass concentration, Individual sample (ng/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectralgindividual_value" is 'Black carbon Spectral G mass concentration, Individual sample (ng/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectralirindividual_raw" is 'Black carbon Spectral IR mass concentration, Individual sample (ng/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectralirindividual_value" is 'Black carbon Spectral IR mass concentration, Individual sample (ng/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectralrindividual_raw" is 'Black carbon Spectral R mass concentration, Individual sample (ng/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectralrindividual_value" is 'Black carbon Spectral R mass concentration, Individual sample (ng/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectraluvindividual_raw" is 'Black carbon Spectral UV mass concentration, Individual sample (ng/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."bcspectraluvindividual_value" is 'Black carbon Spectral UV mass concentration, Individual sample (ng/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."no2concindividual_raw" is 'NO2 concentration, Individual sample (ppb), raw';
comment on column raw_data.open_air_chicago_individual_measurements."no2concindividual_value" is 'NO2 concentration, Individual sample (ppb), value';
comment on column raw_data.open_air_chicago_individual_measurements."pm2_5concmassindividual_raw" is 'PM2.5 mass concentration, Individual sample (μg/m³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."pm2_5concmassindividual_value" is 'PM2.5 mass concentration, Individual sample (μg/m³), value';
comment on column raw_data.open_air_chicago_individual_measurements."pm2_5concnumindividual_raw" is 'PM2.5 number concentration, Individual sample (#/cm³), raw';
comment on column raw_data.open_air_chicago_individual_measurements."pm2_5concnumindividual_value" is 'PM2.5 number concentration, Individual sample (#/cm³), value';
comment on column raw_data.open_air_chicago_individual_measurements."relhumidambientindividual" is 'Relative humidity ambient, Individual sample (%), raw';
comment on column raw_data.open_air_chicago_individual_measurements."relhumidambientindividual_1" is 'Relative humidity ambient, Individual sample (%), value';
comment on column raw_data.open_air_chicago_individual_measurements."relhumidinternalindividual" is 'Relative humidity internal, Individual sample (%), raw';
comment on column raw_data.open_air_chicago_individual_measurements."relhumidinternalindividual_1" is 'Relative humidity internal, Individual sample (%), value';
comment on column raw_data.open_air_chicago_individual_measurements."temperatureambientindividual" is 'Temperature ambient, Individual sample (°C), raw';
comment on column raw_data.open_air_chicago_individual_measurements."temperatureambientindividual_1" is 'Temperature ambient, Individual sample (°C), value';
comment on column raw_data.open_air_chicago_individual_measurements."temperatureinternalindividual" is 'Temperature internal, Individual sample (°C), raw';
comment on column raw_data.open_air_chicago_individual_measurements."temperatureinternalindividual_1" is 'Temperature internal, Individual sample (°C), value';
comment on column raw_data.open_air_chicago_individual_measurements."winddirectionindividual_raw" is 'Wind direction, Individual sample (°), raw';
comment on column raw_data.open_air_chicago_individual_measurements."winddirectionindividual_value" is 'Wind direction, Individual sample (°), value';
comment on column raw_data.open_air_chicago_individual_measurements."windspeedindividual_raw" is 'Wind speed, Individual sample (m/sec), raw';
comment on column raw_data.open_air_chicago_individual_measurements."windspeedindividual_value" is 'Wind speed, Individual sample (m/sec), value';
comment on column raw_data.open_air_chicago_individual_measurements."latitude" is 'Latitude of the sensor location';
comment on column raw_data.open_air_chicago_individual_measurements."longitude" is 'Longitude of the sensor location';
comment on column raw_data.open_air_chicago_individual_measurements."location" is 'Location of the sensor in a format that allows for creation of maps and other geographic operations on this data portal';
comment on column raw_data.open_air_chicago_individual_measurements."record_id" is 'A unique ID for the record.';
comment on column raw_data.open_air_chicago_individual_measurements.":@computed_region_rpca_8um6" is 'This column was automatically created in order to record in what polygon from the dataset ''Boundaries - ZIP Codes'' (rpca-8um6) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.open_air_chicago_individual_measurements.":@computed_region_vrxf_vc4k" is 'This column was automatically created in order to record in what polygon from the dataset ''Community Areas'' (vrxf-vc4k) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.open_air_chicago_individual_measurements.":@computed_region_6mkv_f3dw" is 'This column was automatically created in order to record in what polygon from the dataset ''Zip Codes'' (6mkv-f3dw) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.open_air_chicago_individual_measurements.":@computed_region_bdys_3d7i" is 'This column was automatically created in order to record in what polygon from the dataset ''Census Tracts'' (bdys-3d7i) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.open_air_chicago_individual_measurements.":@computed_region_8hcu_yrd4" is 'This column was automatically created in order to record in what polygon from the dataset ''Wards 2023-'' (8hcu-yrd4) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';


create table if not exists raw_data.chicago_homicide_and_non_fatal_shooting_victimizations (
    "case_number" text,
    "date" timestamptz,
    "block" text,
    "victimization_primary" text,
    "incident_primary" text,
    "gunshot_injury_i" text,
    "unique_id" text,
    "zip_code" text,
    "ward" numeric,
    "community_area" text,
    "street_outreach_organization" text,
    "area" numeric,
    "district" numeric,
    "beat" text,
    "age" text,
    "sex" text,
    "race" text,
    "victimization_fbi_cd" text,
    "incident_fbi_cd" text,
    "victimization_fbi_descr" text,
    "incident_fbi_descr" text,
    "victimization_iucr_cd" text,
    "incident_iucr_cd" text,
    "victimization_iucr_secondary" text,
    "incident_iucr_secondary" text,
    "homicide_victim_first_name" text,
    "homicide_victim_mi" text,
    "homicide_victim_last_name" text,
    "month" numeric,
    "day_of_week" numeric,
    "hour" numeric,
    "location_description" text,
    "state_house_district" text,
    "state_senate_district" text,
    "updated" timestamptz,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    ":@computed_region_d3ds_rm58" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    ":@computed_region_d9mm_jgwp" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."case_number" is 'The Chicago Police Department RD Number (Records Division Number), which is a unique ID assigned to each incident. Due to an incident sometimes involving multiple victimizations, this number is repeated in this dataset for some incidents.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."date" is 'Date when the victimization occurred. This is sometimes a best estimate.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."block" is 'The partially redacted address where the victimization occurred, placing it on the same block as the actual address.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."victimization_primary" is 'Text description of the IUCR Code that describes the major crime category it falls into. VICTIMIZATION_PRIMARY represents only the crime that this specific victim within the incident experienced. In rare instances when we were not able to establish whether a non-fatal shooting victimization was an aggravated battery, robbery, or criminal sexual assault, we set this field equal to “NON-FATAL.”';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."incident_primary" is 'Text description of the IUCR Code that describes the major crime category it falls into. INCIDENT_PRIMARY represents the incident as a whole by conveying the most serious victimization according to IUCR hierarchy guidelines. In rare instances when we were not able to establish whether a non-fatal shooting victimization was an aggravated battery, robbery, or criminal sexual assault, we set this field equal to “NON-FATAL.”';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."gunshot_injury_i" is 'Indicator field describing whether or not a victim was injured by gunfire. Non-fatal shooting data is not available before 2010 so all non-homicide victimizations will be recorded as “UNKNOWN.”';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."unique_id" is 'ID unique to each victimization.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."zip_code" is 'ZIP code where the victimization occurred, using the boundaries in place as of 11/1/2021.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."ward" is 'The City Council district in which the victimization occurred, using the boundaries in place as of 11/1/2021. Please refer to the city of Chicago’s Data Portal for a map of the 50 wards.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."community_area" is 'The community area where the victimization occurred. Chicago has 77 community areas. See the full list of Chicago’s community areas at https://data.cityofchicago.org/d/cauq-8yn6.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."street_outreach_organization" is 'Street outreach organization refers to organizations that actively work in “the streets” to engage individuals who are at immediate or high risk of being either victims or perpetrators of violence. Each organization has specific geographic boundaries where they focus their activities. This indicates which organization(s) is (are) currently active in the location where the victimization occurred, using the boundaries in place as of 11/1/2021, and may contain more than one organization as several outreach organizations’ boundaries overlap.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."area" is 'The CPD area where the victimization occurred, using the boundaries in place as of 11/1/2021. There are 5 CPD areas. Each area includes 3-4 police districts.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."district" is 'The current CPD district where the victimization occurred, using the boundaries in place as of 11/1/2021. Please refer to the City of Chicago’s Data Portal for a map of districts.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."beat" is 'The CPD beat where the victimization occurred, using the boundaries in place as of 11/1/2021. A beat is the smallest CPD geographic area. Each beat has a dedicated police beat car. Three to five beats make up a police sector, and three sectors make up a police district. The Chicago Police Department has 22 police districts. Please refer to the City of Chicago’s Data Portal for a map of beats.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."age" is 'Age of the victims grouped by decade. ';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."sex" is 'Sex of the victims. Victimization demographic data shown here are captured by CPD and limited to information included in the police report, which may not often be supported by individual self-identification including for sex/gender. In particular, CPD has historically recorded a victim’s sex rather than gender although has added a field for collecting gender as of January 2021. ';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."race" is 'Crime classification as outlined in the FBI''s Uniform Crime Reporting (UCR). See the Chicago Police Department listing of these classifications at http://gis.chicagopolice.org/clearmap_crime_sums/crime_type.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."victimization_fbi_cd" is 'Crime classification as outlined in the FBI''s Uniform Crime Reporting (UCR). See the Chicago Police Department listing of these classifications at http://gis.chicagopolice.org/clearmap_crime_sums/crime_type.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."incident_fbi_cd" is 'Crime classification as outlined in the FBI''s Uniform Crime Reporting (UCR), but only pertains to the most serious victimization in the incident. See the Chicago Police Department listing of these classifications at http://gis.chicagopolice.org/clearmap_crime_sums/crime_type.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."victimization_fbi_descr" is 'FBI Description connects a text description of the category to FBI Code.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."incident_fbi_descr" is 'FBI Description connects a text description of the category to FBI Code, but only pertains to the most serious victimization in the incident.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."victimization_iucr_cd" is 'Based on the Illinois Uniform Crime Reporting code. This is directly linked to the Primary Type and Description. See the list of IUCR codes at https://data.cityofchicago.org/d/c7ck-438e.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."incident_iucr_cd" is 'The IUCR code that represents the most serious victimization experienced in this incident according to IUCR hierarchy guidelines. When we were not able to establish whether a homicide victimization was in the first or second degree, we set this field to “01XX.” When we were not able to establish whether a non-fatal shooting victimization was an aggravated battery, robbery, or criminal sexual assault, we set this field equal to “UNK.”';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."victimization_iucr_secondary" is 'IUCR Secondary adds more description to the Primary category.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."incident_iucr_secondary" is 'Text description of the IUCR code that describes the sub category of the primary category. The column represents the incident as whole by conveying the most serious victimization according to IUCR hierarchy guidelines.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."homicide_victim_first_name" is '(For homicide victims only) Victim''s first name.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."homicide_victim_mi" is '(For homicide victims only) Victim''s middle initial.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."homicide_victim_last_name" is '(For homicide victims only) Victim''s last name.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."month" is 'Month when the victimization occurred.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."day_of_week" is 'Day of the week when the victimization occurred. Sunday=1';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."hour" is 'Hour of the day when the victimization occurred.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."location_description" is 'Describes the location where a crime occurred, such as alley, sidewalk, etc.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."state_house_district" is 'Illinois House of Representatives Legislative District where the victimization occurred, using the boundaries in place as of 11/1/2021, represented by members elected to the Illinois House of Representatives.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."state_senate_district" is 'Illinois State Senate Legislative Districts (SLDs) where the victimization occurred, using the boundaries in place as of 11/1/2021, represented by members elected to the Illinois State Senate.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."updated" is 'Records the most recent date the source table was updated.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."latitude" is 'The latitude of the victimization location. In order to preserve anonymity, the given coordinates are not the actual location of the crime. To produce slightly altered coordinates, a circle roughly the size of an average city block was drawn around the original point location, and a new location was picked randomly from a spot around the circumference of that circle.';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."longitude" is 'The longitude of the victimization location. This has been slightly altered to preserve anonymity. (see details under LATITUDE).';
comment on column raw_data.chicago_homicide_and_non_fatal_shooting_victimizations."location" is 'A slightly altered location of the victimization in a form that allows for mapping and other geographic analysis on this data portal.';


create table if not exists raw_data.chicago_arrests (
    "cb_no" numeric,
    "case_number" text,
    "arrest_date" timestamptz,
    "race" text,
    "charge_1_statute" text,
    "charge_1_description" text,
    "charge_1_type" text,
    "charge_1_class" text,
    "charge_2_statute" text,
    "charge_2_description" text,
    "charge_2_type" text,
    "charge_2_class" text,
    "charge_3_statute" text,
    "charge_3_description" text,
    "charge_3_type" text,
    "charge_3_class" text,
    "charge_4_statute" text,
    "charge_4_description" text,
    "charge_4_type" text,
    "charge_4_class" text,
    "charges_statute" text,
    "charges_description" text,
    "charges_type" text,
    "charges_class" text,
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_arrests."cb_no" is 'Central Booking Number – uniquely identifies the arrest report';
comment on column raw_data.chicago_arrests."case_number" is 'Records Division Number – uniquely identifies the criminal incident report associated with the arrest . This column, if populated, corresponds to Case Number in https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2 and can be used to link the datasets.';
comment on column raw_data.chicago_arrests."arrest_date" is 'Date and time of arrest.';
comment on column raw_data.chicago_arrests."race" is 'The race/ethnicity of the person arrested.';
comment on column raw_data.chicago_arrests."charge_1_statute" is 'Statute for most serious charge (based on class and type, then arrest report order in case of tie). ';
comment on column raw_data.chicago_arrests."charge_1_description" is 'Statute description for most serious charge (based on class and type, then arrest report order in case of tie).';
comment on column raw_data.chicago_arrests."charge_1_type" is 'Charge type for most serious charge (F = felony; M = misdemeanor; Null = local ordinance, traffic arrest, other) ';
comment on column raw_data.chicago_arrests."charge_1_class" is 'Charge class for most serious charge (If F then X, 1, 2, 3, or 4; If M then A, B, or C; L = local ordinance violation; P = petty offense; U = business offense; Z = used for warrant arrests, criminal registration violations, and violations of court orders)';
comment on column raw_data.chicago_arrests."charge_2_statute" is 'Statute for 2nd most serious charge (based on class and type, then arrest report order in case of tie).';
comment on column raw_data.chicago_arrests."charge_2_description" is 'Statute description for 2nd most serious charge (based on class and type, then arrest report order in case of tie).';
comment on column raw_data.chicago_arrests."charge_2_type" is 'Charge type for 2nd most serious charge (F = felony; M = misdemeanor; Null = local ordinance, traffic arrest, other) ';
comment on column raw_data.chicago_arrests."charge_2_class" is 'Charge class for 2nd most serious charge (If F then X, 1, 2, 3, or 4; If M then A, B, or C; L = local ordinance violation; P = petty offense; U = business offense; Z = used for warrant arrests, criminal registration violations, and violations of court orders)';
comment on column raw_data.chicago_arrests."charge_3_statute" is 'Statute for 3rd most serious charge (based on class and type, then arrest report order in case of tie).';
comment on column raw_data.chicago_arrests."charge_3_description" is 'Statute description for 3rd most serious charge (based on class and type, then arrest report order in case of tie).';
comment on column raw_data.chicago_arrests."charge_3_type" is 'Charge type for 3rd most serious charge (F = felony; M = misdemeanor; Null = local ordinance, traffic arrest, other) ';
comment on column raw_data.chicago_arrests."charge_3_class" is 'Charge class for 3rd most serious charge (If F then X, 1, 2, 3, or 4; If M then A, B, or C; L = local ordinance violation; P = petty offense; U = business offense; Z = used for warrant arrests, criminal registration violations, and violations of court orders)';
comment on column raw_data.chicago_arrests."charge_4_statute" is 'Statute for 4th most serious charge (based on class and type, then arrest report order in case of tie).';
comment on column raw_data.chicago_arrests."charge_4_description" is 'Statute description for 4th most serious charge (based on class and type, then arrest report order in case of tie).';
comment on column raw_data.chicago_arrests."charge_4_type" is 'Charge type for 4th most serious charge (F = felony; M = misdemeanor; Null = local ordinance, traffic arrest, other) ';
comment on column raw_data.chicago_arrests."charge_4_class" is 'Charge class for 4th most serious charge (If F then X, 1, 2, 3, or 4; If M then A, B, or C; L = local ordinance violation; P = petty offense; U = business offense; Z = used for warrant arrests, criminal registration violations, and violations of court orders)';
comment on column raw_data.chicago_arrests."charges_statute" is 'The statutes of the first four charges filed, separated by the | character with a space on each side. Values are always listed in the same order. In other words, the nth value in each list will refer to the same charge. The "Contains" operator can be used in a filter to search for presence of a value in any of the charges.';
comment on column raw_data.chicago_arrests."charges_description" is 'The descriptions of the first four charges filed, separated by the | character with a space on each side. Values are always listed in the same order. In other words, the nth value in each list will refer to the same charge. The "Contains" operator can be used in a filter to search for presence of a value in any of the charges.';
comment on column raw_data.chicago_arrests."charges_type" is 'The types of the first four charges filed, separated by the | character with a space on each side. Values are always listed in the same order. In other words, the nth value in each list will refer to the same charge. The "Contains" operator can be used in a filter to search for presence of a value in any of the charges.';
comment on column raw_data.chicago_arrests."charges_class" is 'The classes of the first four charges filed, separated by the | character with a space on each side. Values are always listed in the same order. In other words, the nth value in each list will refer to the same charge. The "Contains" operator can be used in a filter to search for presence of a value in any of the charges.';


create table if not exists raw_data.cook_county_parcel_sales (
    "pin" text,
    "year" numeric,
    "township_code" text,
    "nbhd" text,
    "class" text,
    "sale_date" timestamptz,
    "is_mydec_date" boolean,
    "sale_price" numeric,
    "doc_no" text,
    "deed_type" text,
    "mydec_deed_type" text,
    "seller_name" text,
    "is_multisale" boolean,
    "num_parcels_sale" numeric,
    "buyer_name" text,
    "sale_type" text,
    "sale_filter_same_sale_within_365" boolean,
    "sale_filter_less_than_10k" boolean,
    "sale_filter_deed_type" boolean,
    "row_id" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.cook_county_parcel_sales."pin" is 'Parcel Identification Number (PIN)';
comment on column raw_data.cook_county_parcel_sales."year" is 'Year';
comment on column raw_data.cook_county_parcel_sales."township_code" is 'Township code';
comment on column raw_data.cook_county_parcel_sales."nbhd" is 'Assessor neighborhood code, first two digits are township, last three are neighborhood';
comment on column raw_data.cook_county_parcel_sales."class" is 'Property class';
comment on column raw_data.cook_county_parcel_sales."sale_date" is 'Sale date (recorded, not executed)';
comment on column raw_data.cook_county_parcel_sales."is_mydec_date" is 'Indicates whether the sale date has been overwritten with a more precise value from IDOR (Illinois Department of Revenue). In the past the Assessor''s ingest process truncated sale dates to the first of the month. Not all sales can be updated with dates from IDOR.';
comment on column raw_data.cook_county_parcel_sales."sale_price" is 'Sale price';
comment on column raw_data.cook_county_parcel_sales."doc_no" is 'Sale document number. Corresponds to Clerk''s document number';
comment on column raw_data.cook_county_parcel_sales."deed_type" is 'Sale deed type';
comment on column raw_data.cook_county_parcel_sales."mydec_deed_type" is 'Deed type from MyDec, more granular than CCAO deed type.';
comment on column raw_data.cook_county_parcel_sales."seller_name" is 'Sale seller name';
comment on column raw_data.cook_county_parcel_sales."is_multisale" is 'Indicates whether a parcel was sold individually or as part of a larger group of PINs';
comment on column raw_data.cook_county_parcel_sales."num_parcels_sale" is 'The number of parcels that were part of the sale';
comment on column raw_data.cook_county_parcel_sales."buyer_name" is 'Sale buyer name';
comment on column raw_data.cook_county_parcel_sales."sale_type" is 'Sale type';
comment on column raw_data.cook_county_parcel_sales."sale_filter_same_sale_within_365" is 'Remove sale with the same value (for the same PIN) within 365 days';
comment on column raw_data.cook_county_parcel_sales."sale_filter_less_than_10k" is 'Indicator for whether sale is less than $10K FMW';
comment on column raw_data.cook_county_parcel_sales."sale_filter_deed_type" is 'Indicator for quit claim, executor, beneficiary and missing deed types';
comment on column raw_data.cook_county_parcel_sales."row_id" is 'Unique row key for API';


create table if not exists raw_data.cook_county_parcel_addresses (
    "pin" text,
    "pin10" text,
    "year" numeric,
    "prop_address_full" text,
    "prop_address_city_name" text,
    "prop_address_state" text,
    "prop_address_zipcode_1" text,
    "mail_address_name" text,
    "mail_address_full" text,
    "mail_address_city_name" text,
    "mail_address_state" text,
    "mail_address_zipcode_1" text,
    "row_id" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.cook_county_parcel_addresses."pin" is 'Parcel Identification Number (PIN)';
comment on column raw_data.cook_county_parcel_addresses."pin10" is 'Parcel Identification Number (10-digit)';
comment on column raw_data.cook_county_parcel_addresses."year" is 'Tax year';
comment on column raw_data.cook_county_parcel_addresses."prop_address_full" is 'Property street address';
comment on column raw_data.cook_county_parcel_addresses."prop_address_city_name" is 'Property city';
comment on column raw_data.cook_county_parcel_addresses."prop_address_state" is 'Property state';
comment on column raw_data.cook_county_parcel_addresses."prop_address_zipcode_1" is 'Property zip code';
comment on column raw_data.cook_county_parcel_addresses."mail_address_name" is 'Taxpayer mailing name';
comment on column raw_data.cook_county_parcel_addresses."mail_address_full" is 'Taxpayer mailing street address';
comment on column raw_data.cook_county_parcel_addresses."mail_address_city_name" is 'Taxpayer mailing city';
comment on column raw_data.cook_county_parcel_addresses."mail_address_state" is 'Taxpayer mailing state';
comment on column raw_data.cook_county_parcel_addresses."mail_address_zipcode_1" is 'Taxpayer mailing zip code';
comment on column raw_data.cook_county_parcel_addresses."row_id" is 'Unique row key for API';


create table if not exists raw_data.cook_county_commercial_valuation_data (
    "keypin" text,
    "pins" text,
    "year" numeric,
    "township" text,
    "sheet" text,
    "class_es" text,
    "studiounits" numeric,
    "_1brunits" numeric,
    "_2brunits" numeric,
    "_3brunits" numeric,
    "_4brunits" numeric,
    "tot_units" numeric,
    "address" text,
    "adj_rent_sf" numeric,
    "aprx_comm_sf" numeric,
    "apt" numeric,
    "avgdailyrate" numeric,
    "bldgsf" numeric,
    "gross_building_area" numeric,
    "caprate" numeric,
    "carwash" text,
    "category" numeric,
    "ceilingheight" text,
    "cost_day_bed" numeric,
    "costapproach_sf" numeric,
    "covidadjvacancy" numeric,
    "ebitda" numeric,
    "egi" numeric,
    "excesslandarea" numeric,
    "excesslandval" numeric,
    "exp" numeric,
    "f_r" text,
    "finalmarketvalue" numeric,
    "finalmarketvalue_bed" numeric,
    "finalmarketvalue_key" numeric,
    "finalmarketvalue_sf" numeric,
    "finalmarketvalue_unit" numeric,
    "idphlicense" numeric,
    "incomemarketvalue" numeric,
    "incomemarketvalue_sf" numeric,
    "investmentrating" text,
    "land_bldg" numeric,
    "landsf" numeric,
    "model" text,
    "nbhd" numeric,
    "netrentablesf" numeric,
    "noi" numeric,
    "oiltankvalue_atypicaloby" numeric,
    "owner" numeric,
    "parking" numeric,
    "parkingsf" numeric,
    "pctownerinterest" numeric,
    "permit_partial_demovalue" text,
    "permit_partial_demovalue_reason" text,
    "pgi" numeric,
    "property_name_description" text,
    "property_type_use" text,
    "reportedoccupancy" numeric,
    "revenuebed_day" numeric,
    "revpar" numeric,
    "roomrev" numeric,
    "salecompmarketvalue_sf" numeric,
    "sap" numeric,
    "sapdeduction" numeric,
    "saptier" numeric,
    "stories" numeric,
    "subclass2" text,
    "taxdist" text,
    "taxpayer" text,
    "totalrevreported" numeric,
    "totalexp" numeric,
    "totallandval" numeric,
    "totalrev" numeric,
    "townregion" text,
    "vacancy" numeric,
    "yearbuilt" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.cook_county_commercial_valuation_data."keypin" is 'Parcel identification number (PIN). Primary PIN used for valuing properties associated with multiple PINs.';
comment on column raw_data.cook_county_commercial_valuation_data."pins" is 'All PINs associated with primary PIN that should be valued collectively';
comment on column raw_data.cook_county_commercial_valuation_data."year" is 'Year';
comment on column raw_data.cook_county_commercial_valuation_data."township" is 'Township name';
comment on column raw_data.cook_county_commercial_valuation_data."sheet" is 'Sheet from associated excel workbook that contains row on assessor''s website';
comment on column raw_data.cook_county_commercial_valuation_data."class_es" is 'Class of key PIN or classes of key PIN and some or all associated PINs';
comment on column raw_data.cook_county_commercial_valuation_data."studiounits" is 'Number of studio apartments';
comment on column raw_data.cook_county_commercial_valuation_data."_1brunits" is 'Number of 1 bedroom apartments';
comment on column raw_data.cook_county_commercial_valuation_data."_2brunits" is 'Number of 2 bedroom apartments';
comment on column raw_data.cook_county_commercial_valuation_data."_3brunits" is 'Number of 3 bedroom apartments';
comment on column raw_data.cook_county_commercial_valuation_data."_4brunits" is 'Number of 4 bedroom apartments';
comment on column raw_data.cook_county_commercial_valuation_data."tot_units" is 'Total number of units (can include hotel rooms, nursing home rooms, residential apartments, commercial units etc.)';
comment on column raw_data.cook_county_commercial_valuation_data."address" is 'Address for keypin';
comment on column raw_data.cook_county_commercial_valuation_data."adj_rent_sf" is 'Adjusted rent per square foot';
comment on column raw_data.cook_county_commercial_valuation_data."aprx_comm_sf" is 'Approximate commercial square footage for key PIN and all associated PINs';
comment on column raw_data.cook_county_commercial_valuation_data."apt" is 'Total number of apartments';
comment on column raw_data.cook_county_commercial_valuation_data."avgdailyrate" is 'Average daily rate (hotel specific)';
comment on column raw_data.cook_county_commercial_valuation_data."bldgsf" is 'Building square footage for key PIN and all associated PINs';
comment on column raw_data.cook_county_commercial_valuation_data."gross_building_area" is 'Gross building area';
comment on column raw_data.cook_county_commercial_valuation_data."caprate" is 'Capitalization rate';
comment on column raw_data.cook_county_commercial_valuation_data."carwash" is 'Car wash at location (gas station/convenience store specific) - A = "Yes", B = "No"';
comment on column raw_data.cook_county_commercial_valuation_data."category" is 'Hotel category (hotel specific)';
comment on column raw_data.cook_county_commercial_valuation_data."ceilingheight" is 'Ceiling height';
comment on column raw_data.cook_county_commercial_valuation_data."cost_day_bed" is 'Cost per day per bed (nursing home specific)';
comment on column raw_data.cook_county_commercial_valuation_data."costapproach_sf" is 'Cost approach value per square foot';
comment on column raw_data.cook_county_commercial_valuation_data."covidadjvacancy" is 'Covid adjusted vacancy';
comment on column raw_data.cook_county_commercial_valuation_data."ebitda" is 'Earnings before interest taxes depreciation and amortization %';
comment on column raw_data.cook_county_commercial_valuation_data."egi" is 'Effective gross income';
comment on column raw_data.cook_county_commercial_valuation_data."excesslandarea" is 'Excess land area (land area above 4:1 land to building)';
comment on column raw_data.cook_county_commercial_valuation_data."excesslandval" is 'Value of excess land area';
comment on column raw_data.cook_county_commercial_valuation_data."exp" is 'Expenses';
comment on column raw_data.cook_county_commercial_valuation_data."f_r" is 'Fast food operation / rental space (gas station/convenience store specific)';
comment on column raw_data.cook_county_commercial_valuation_data."finalmarketvalue" is 'Final market value';
comment on column raw_data.cook_county_commercial_valuation_data."finalmarketvalue_bed" is 'Final market value per bed (nursing home specific)';
comment on column raw_data.cook_county_commercial_valuation_data."finalmarketvalue_key" is 'Final market value per key (529 class specific)';
comment on column raw_data.cook_county_commercial_valuation_data."finalmarketvalue_sf" is 'Final market value per square foot';
comment on column raw_data.cook_county_commercial_valuation_data."finalmarketvalue_unit" is 'Final market value per unit (multifamily specific)';
comment on column raw_data.cook_county_commercial_valuation_data."idphlicense" is 'Illinois department of health license number';
comment on column raw_data.cook_county_commercial_valuation_data."incomemarketvalue" is 'Value derived from income approach';
comment on column raw_data.cook_county_commercial_valuation_data."incomemarketvalue_sf" is 'Value per square foot derived from income approach';
comment on column raw_data.cook_county_commercial_valuation_data."investmentrating" is 'Invesment rating';
comment on column raw_data.cook_county_commercial_valuation_data."land_bldg" is 'Land square footage to building square footage ratio for key PIN and all associated PINs';
comment on column raw_data.cook_county_commercial_valuation_data."landsf" is 'Land square footage for key PIN and all associated PINs';
comment on column raw_data.cook_county_commercial_valuation_data."model" is 'Commercial valuation modeling group';
comment on column raw_data.cook_county_commercial_valuation_data."nbhd" is 'Assessor neighborhood number';
comment on column raw_data.cook_county_commercial_valuation_data."netrentablesf" is 'Net rentable square footage';
comment on column raw_data.cook_county_commercial_valuation_data."noi" is 'Net operating income';
comment on column raw_data.cook_county_commercial_valuation_data."oiltankvalue_atypicaloby" is 'Oil tank value / atypical outbuilding';
comment on column raw_data.cook_county_commercial_valuation_data."owner" is 'Owner name';
comment on column raw_data.cook_county_commercial_valuation_data."parking" is 'Number of parking spaces';
comment on column raw_data.cook_county_commercial_valuation_data."parkingsf" is 'Parking square footage';
comment on column raw_data.cook_county_commercial_valuation_data."pctownerinterest" is 'Percent owner interest (condominium specific)';
comment on column raw_data.cook_county_commercial_valuation_data."permit_partial_demovalue" is 'Partial value due to uncompleted new construction or demolition';
comment on column raw_data.cook_county_commercial_valuation_data."permit_partial_demovalue_reason" is 'Reason for partial value due to uncompleted new construction or demolition';
comment on column raw_data.cook_county_commercial_valuation_data."pgi" is 'Potential gross income';
comment on column raw_data.cook_county_commercial_valuation_data."property_name_description" is 'Property name/description';
comment on column raw_data.cook_county_commercial_valuation_data."property_type_use" is 'Property type/use (2023 and before)';
comment on column raw_data.cook_county_commercial_valuation_data."reportedoccupancy" is 'Reported occupancy';
comment on column raw_data.cook_county_commercial_valuation_data."revenuebed_day" is 'Revenue per bed per day (nursing home specific)';
comment on column raw_data.cook_county_commercial_valuation_data."revpar" is 'Revenue per available room (hotel specific)';
comment on column raw_data.cook_county_commercial_valuation_data."roomrev" is 'Percentage of revenue from room rentals (hotel specific)';
comment on column raw_data.cook_county_commercial_valuation_data."salecompmarketvalue_sf" is 'Value per square foot derived from comparables approach';
comment on column raw_data.cook_county_commercial_valuation_data."sap" is 'In affordable housing special assessment program';
comment on column raw_data.cook_county_commercial_valuation_data."sapdeduction" is 'Reduction from affordable housing special assessment program';
comment on column raw_data.cook_county_commercial_valuation_data."saptier" is 'Tier of affordable housing special assessment program';
comment on column raw_data.cook_county_commercial_valuation_data."stories" is 'Number of stories';
comment on column raw_data.cook_county_commercial_valuation_data."subclass2" is 'Property type/use (2024 and later)';
comment on column raw_data.cook_county_commercial_valuation_data."taxdist" is 'Taxing district';
comment on column raw_data.cook_county_commercial_valuation_data."taxpayer" is 'Owner';
comment on column raw_data.cook_county_commercial_valuation_data."totalrevreported" is 'Total revenue reported';
comment on column raw_data.cook_county_commercial_valuation_data."totalexp" is 'Total expenses as a value';
comment on column raw_data.cook_county_commercial_valuation_data."totallandval" is 'Total land value';
comment on column raw_data.cook_county_commercial_valuation_data."totalrev" is 'Total revenue';
comment on column raw_data.cook_county_commercial_valuation_data."townregion" is 'Township region';
comment on column raw_data.cook_county_commercial_valuation_data."vacancy" is 'Vacancy';
comment on column raw_data.cook_county_commercial_valuation_data."yearbuilt" is 'Year built';


create table if not exists raw_data.cook_county_neighborhood_boundaries (
    "multipolygon" geometry(MultiPolygon, 4326),
    "nbhd" text,
    "town_nbhd" text,
    "township_code" text,
    "township_name" text,
    "triad_code" text,
    "triad_name" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.cook_county_neighborhood_boundaries."multipolygon" is 'Geometry (neighborhood boundary)';
comment on column raw_data.cook_county_neighborhood_boundaries."nbhd" is 'Neighborhood number';
comment on column raw_data.cook_county_neighborhood_boundaries."town_nbhd" is 'Township and neighborhood number. First 2 digits are town, last 3 are neighborhood';
comment on column raw_data.cook_county_neighborhood_boundaries."township_code" is 'Township number';
comment on column raw_data.cook_county_neighborhood_boundaries."township_name" is 'Township name';
comment on column raw_data.cook_county_neighborhood_boundaries."triad_code" is 'Triad code. Reassessment of property in Cook County is done within a triennial cycle, meaning it occurs every three years. The Cook County Assessor''s Office alternates reassessments between triads: the north and west suburbs, the south and west suburbs and the City of Chicago.';
comment on column raw_data.cook_county_neighborhood_boundaries."triad_name" is 'Triad name. Reassessment of property in Cook County is done within a triennial cycle, meaning it occurs every three years. The Cook County Assessor''s Office alternates reassessments between triads: the north and west suburbs, the south and west suburbs and the City of Chicago.';


create table if not exists raw_data.cook_county_single_and_multi_family_improvement_characteristics (
    "pin" text,
    "year" numeric,
    "card" numeric,
    "class" text,
    "township_code" text,
    "tieback_key_pin" text,
    "tieback_proration_rate" numeric,
    "card_proration_rate" numeric,
    "cdu" text,
    "pin_is_multicard" boolean,
    "pin_num_cards" numeric,
    "pin_is_multiland" boolean,
    "pin_num_landlines" numeric,
    "char_yrblt" numeric,
    "char_bldg_sf" numeric,
    "char_land_sf" numeric,
    "char_beds" numeric,
    "char_rooms" numeric,
    "char_fbath" numeric,
    "char_hbath" numeric,
    "char_frpl" numeric,
    "char_type_resd" text,
    "char_cnst_qlty" text,
    "char_apts" text,
    "char_attic_fnsh" text,
    "char_gar1_att" text,
    "char_gar1_area" text,
    "char_gar1_size" text,
    "char_gar1_cnst" text,
    "char_attic_type" text,
    "char_bsmt" text,
    "char_ext_wall" text,
    "char_heat" text,
    "char_repair_cnd" text,
    "char_bsmt_fin" text,
    "char_roof_cnst" text,
    "char_use" text,
    "char_site" text,
    "char_ncu" text,
    "char_renovation" text,
    "char_porch" text,
    "char_air" text,
    "char_tp_plan" text,
    "row_id" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."pin" is 'Parcel Identification Number (PIN)';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."year" is 'Tax year';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."card" is 'Card number. Each card is an improvement/building on the parcel';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."class" is 'Property class';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."township_code" is 'Township code';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."tieback_key_pin" is 'Tieback key PIN. Prorated properties (whose value is split across multiple PINs) have a "main" or key PIN';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."tieback_proration_rate" is 'Tieback proration rate. Prorated properties (whose value is split across multiple PINs) pay taxes on the proportion of value on their PIN. In other words, assessed value is multiplied by proration rate to determine taxable assessed value';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."card_proration_rate" is 'Card proration rate. Prorated parcels (whose value is split across multiple cards) pay taxes on the proportion of value on their card. In other words, assessed value is multiplied by proration rate to determine taxable assessed value. Cards are divisions within parcels, such as one of multiple buildings on a single parcel. ';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."cdu" is 'Condition, Desirability, and Utility code. Not well maintained.';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."pin_is_multicard" is 'Multicard PIN. Indicates whether the parcel contains more than one building (ADU, coach house, etc.)';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."pin_num_cards" is 'Number of cards on this parcel. Each card is an improvement/building';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."pin_is_multiland" is 'Multiland PIN. Indicates whether parcel has more than one landline';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."pin_num_landlines" is 'Number of landlines on a parcel. The sum of all landline square footage should be equal to the total square footage of the parcel. Each landline can correspond to a different land price/rate';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_yrblt" is 'Year built';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_bldg_sf" is 'Building square feet. Square footage of the building, as measured from the exterior';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_land_sf" is 'Land square feet. Square footage of the land (not just the building) of the property. Note that a single PIN can have multiple landlines, meaning it can be associated with more than one land price/rate';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_beds" is 'Number of bedrooms';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_rooms" is 'Rooms. Number of total rooms in the building (excluding baths). Not to be confused with bedrooms';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_fbath" is 'Full baths. Defined as having a bath or shower. If this value is missing, the default value is set to 1';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_hbath" is 'Half baths. Defined as bathrooms without a shower or bathtub';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_frpl" is 'Fireplaces. Counted as the number of flues one can see from the outside of the building';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_type_resd" is 'Type of residence';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_cnst_qlty" is 'Construction quality';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_apts" is 'Apartments. Number of apartments for class 211 and 212 properties';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_attic_fnsh" is 'Attic finish';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_gar1_att" is 'Garage 1 attached';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_gar1_area" is 'Is Garage 1 physically included within the building area? If yes, the garage area is subtracted from the building square feet calculation by the field agent';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_gar1_size" is 'Garage 1 size';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_gar1_cnst" is 'Garage 1 exterior wall material';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_attic_type" is 'Attic type';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_bsmt" is 'Basement type';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_ext_wall" is 'Exterior wall material';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_heat" is 'Central heating';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_repair_cnd" is 'Repair condition';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_bsmt_fin" is 'Basement finish';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_roof_cnst" is 'Roof material';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_use" is 'Single vs. multi-family use';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_site" is 'Site desirability';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_ncu" is 'Number of commercial units on the parcel (the vast majority are for properties with class 212)';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_renovation" is 'Renovation';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_porch" is 'Porch';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_air" is 'Central air conditioning';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."char_tp_plan" is 'Design plan';
comment on column raw_data.cook_county_single_and_multi_family_improvement_characteristics."row_id" is 'Unique row key for API';


create table if not exists raw_data.cook_county_residential_condominium_unit_characteristics (
    "pin" text,
    "pin10" text,
    "card" numeric,
    "year" numeric,
    "class" text,
    "township_code" text,
    "tieback_key_pin" text,
    "tieback_proration_rate" numeric,
    "card_proration_rate" numeric,
    "char_yrblt" numeric,
    "char_building_sf" numeric,
    "char_unit_sf" numeric,
    "char_bedrooms" numeric,
    "char_full_baths" numeric,
    "char_half_baths" numeric,
    "char_building_non_units" numeric,
    "char_building_pins" numeric,
    "char_land_sf" numeric,
    "cdu" text,
    "pin_is_multiland" boolean,
    "pin_num_landlines" numeric,
    "bldg_is_mixed_use" boolean,
    "is_parking_space" boolean,
    "is_common_area" boolean,
    "row_id" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.cook_county_residential_condominium_unit_characteristics."pin" is 'Parcel Identification Number (PIN). The last 4 digits identify the unit within the building';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."pin10" is 'Parcel Identification Number (10-digit). Identifies the condominium building';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."card" is 'Card number. Generally not applicable to condominiums';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."year" is 'Tax year';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."class" is 'Property class';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."township_code" is 'Township code';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."tieback_key_pin" is 'Tieback key PIN. Prorated properties (whose value is split across multiple PINs) have a "main" or key PIN';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."tieback_proration_rate" is 'Tieback proration rate. Prorated properties (whose value is split across multiple PINs) pay taxes on the proportion of value on their PIN. In other words, assessed value is multiplied by proration rate to determine taxable assessed value';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."card_proration_rate" is 'Card proration rate. Prorated parcels (whose value is split across multiple cards) pay taxes on the proportion of value on their card. In other words, assessed value is multiplied by proration rate to determine taxable assessed value. Cards are divisions within parcels, such as one of multiple buildings on a single parcel.';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_yrblt" is 'Year built';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_building_sf" is 'Condominium building total square feet. Sourced from listings and other sources';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_unit_sf" is 'Condominium unit square feet. Sourced from listings and other sources';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_bedrooms" is 'Condominium unit bedrooms. Sourced from listings and other sources';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_full_baths" is 'Full baths. Defined as having a bath or shower. If this value is missing, the default value is set to 1';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_half_baths" is 'Half baths. Defined as bathrooms without a shower or bathtub';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_building_non_units" is 'Total condominium building non-livable parcel count. Includes parking, storage areas, and common areas';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_building_pins" is 'Total condominium building parcel count. Includes both livable and non-livable parcels';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."char_land_sf" is 'Land square feet. Same for all units in a building';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."cdu" is 'Condition, Desirability, and Utility code. Not well maintained.';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."pin_is_multiland" is 'Multiland PIN indicator. Generally not applicable to condominiums';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."pin_num_landlines" is 'Number of landlines on PIN. Generally not applicable to condominiums';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."bldg_is_mixed_use" is 'Condominium building contains non-residential units';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."is_parking_space" is 'Parking/garage space or storage unit indicator';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."is_common_area" is 'Building common area indicator';
comment on column raw_data.cook_county_residential_condominium_unit_characteristics."row_id" is 'Unique row key for API';


create table if not exists raw_data.cook_county_assessed_parcel_values (
    "pin" text,
    "year" numeric,
    "class" text,
    "township_code" text,
    "township_name" text,
    "nbhd" text,
    "mailed_bldg" numeric,
    "mailed_land" numeric,
    "mailed_tot" numeric,
    "mailed_hie" numeric,
    "certified_bldg" numeric,
    "certified_land" numeric,
    "certified_tot" numeric,
    "certified_hie" numeric,
    "board_bldg" numeric,
    "board_land" numeric,
    "board_tot" numeric,
    "board_hie" numeric,
    "row_id" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.cook_county_assessed_parcel_values."pin" is 'Parcel Identification Number (PIN)';
comment on column raw_data.cook_county_assessed_parcel_values."year" is 'Tax year';
comment on column raw_data.cook_county_assessed_parcel_values."class" is 'Property class';
comment on column raw_data.cook_county_assessed_parcel_values."township_code" is 'Township code';
comment on column raw_data.cook_county_assessed_parcel_values."township_name" is 'Township name';
comment on column raw_data.cook_county_assessed_parcel_values."nbhd" is 'Assessor neighborhood code, first two digits are township, last three are neighborhood';
comment on column raw_data.cook_county_assessed_parcel_values."mailed_bldg" is 'Assessor mailed building value';
comment on column raw_data.cook_county_assessed_parcel_values."mailed_land" is 'Assessor mailed land value';
comment on column raw_data.cook_county_assessed_parcel_values."mailed_tot" is 'Assessor mailed total value';
comment on column raw_data.cook_county_assessed_parcel_values."mailed_hie" is 'Assessor mailed home improvement exemption value';
comment on column raw_data.cook_county_assessed_parcel_values."certified_bldg" is 'Assessor certified building value';
comment on column raw_data.cook_county_assessed_parcel_values."certified_land" is 'Assessor certified land value';
comment on column raw_data.cook_county_assessed_parcel_values."certified_tot" is 'Assessor certified total value';
comment on column raw_data.cook_county_assessed_parcel_values."certified_hie" is 'Assessor certified home improvement exemption value';
comment on column raw_data.cook_county_assessed_parcel_values."board_bldg" is 'Board of Review certified building value';
comment on column raw_data.cook_county_assessed_parcel_values."board_land" is 'Board of Review certified land value';
comment on column raw_data.cook_county_assessed_parcel_values."board_tot" is 'Board of Review certified total value';
comment on column raw_data.cook_county_assessed_parcel_values."board_hie" is 'Board of Review certified home improvement exemption value';
comment on column raw_data.cook_county_assessed_parcel_values."row_id" is 'Unique row key for API';


create table if not exists raw_data.chicago_crimes (
    "id" numeric,
    "case_number" text,
    "date" timestamptz,
    "block" text,
    "iucr" text,
    "primary_type" text,
    "description" text,
    "location_description" text,
    "arrest" boolean,
    "domestic" boolean,
    "beat" text,
    "district" text,
    "ward" numeric,
    "community_area" text,
    "fbi_code" text,
    "x_coordinate" numeric,
    "y_coordinate" numeric,
    "year" numeric,
    "updated_on" timestamptz,
    "latitude" numeric,
    "longitude" numeric,
    "location" jsonb,
    ":@computed_region_awaf_s7ux" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_d9mm_jgwp" numeric,
    ":@computed_region_d3ds_rm58" numeric,
    ":@computed_region_8hcu_yrd4" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_crimes."id" is 'Unique identifier for the record.';
comment on column raw_data.chicago_crimes."case_number" is 'The Chicago Police Department RD Number (Records Division Number), which is unique to the incident. ';
comment on column raw_data.chicago_crimes."date" is 'Date when the incident occurred. this is sometimes a best estimate. ';
comment on column raw_data.chicago_crimes."block" is 'The partially redacted address where the incident occurred, placing it on the same block as the actual address. ';
comment on column raw_data.chicago_crimes."iucr" is 'The Illinois Uniform Crime Reporting code. This is directly linked to the Primary Type and Description. See the list of IUCR codes at https://data.cityofchicago.org/d/c7ck-438e.';
comment on column raw_data.chicago_crimes."primary_type" is 'The primary description of the IUCR code.';
comment on column raw_data.chicago_crimes."description" is 'The secondary description of the IUCR code, a subcategory of the primary description.';
comment on column raw_data.chicago_crimes."location_description" is 'Description of the location where the incident occurred.';
comment on column raw_data.chicago_crimes."arrest" is 'Indicates whether an arrest was made.';
comment on column raw_data.chicago_crimes."domestic" is 'Indicates whether the incident was domestic-related as defined by the Illinois Domestic Violence Act.';
comment on column raw_data.chicago_crimes."beat" is 'Indicates the beat where the incident occurred.  A beat is the smallest police geographic area – each beat has a dedicated police beat car.  Three to five beats make up a police sector, and three sectors make up a police district.  The Chicago Police Department has 22 police districts.  

See the beats at https://data.cityofchicago.org/d/aerh-rz74.
';
comment on column raw_data.chicago_crimes."district" is 'Indicates the police district where the incident occurred.  See the districts at https://data.cityofchicago.org/d/fthy-xz3r.';
comment on column raw_data.chicago_crimes."ward" is 'The ward (City Council district) where the incident occurred.  See the wards at https://data.cityofchicago.org/d/sp34-6z76.';
comment on column raw_data.chicago_crimes."community_area" is 'Indicates the community area where the incident occurred.  Chicago has 77 community areas. 

See the community areas at https://data.cityofchicago.org/d/cauq-8yn6.
';
comment on column raw_data.chicago_crimes."fbi_code" is 'Indicates the crime classification as outlined in the FBI''s National Incident-Based Reporting System (NIBRS).See the Chicago Police Department listing of these classifications at https://gis.chicagopolice.org/pages/crime_details.';
comment on column raw_data.chicago_crimes."x_coordinate" is 'The x coordinate of the location where the incident occurred in State Plane Illinois East NAD 1983 projection.  This location is shifted from the actual location for partial redaction but falls on the same block.';
comment on column raw_data.chicago_crimes."y_coordinate" is 'The y coordinate of the location where the incident occurred in State Plane Illinois East NAD 1983 projection. This location is shifted from the actual location for partial redaction but falls on the same block.';
comment on column raw_data.chicago_crimes."year" is 'Year the incident occurred.';
comment on column raw_data.chicago_crimes."updated_on" is 'Date and time the record was last updated.';
comment on column raw_data.chicago_crimes."latitude" is 'The latitude of the location where the incident occurred. This location is shifted from the actual location for partial redaction but falls on the same block.';
comment on column raw_data.chicago_crimes."longitude" is 'The longitude of the location where the incident occurred. This location is shifted from the actual location for partial redaction but falls on the same block.';
comment on column raw_data.chicago_crimes."location" is 'The location where the incident occurred in a format that allows for creation of maps and other geographic operations on this data portal. This location is shifted from the actual location for partial redaction but falls on the same block.';
comment on column raw_data.chicago_crimes.":@computed_region_8hcu_yrd4" is 'This column was automatically created in order to record in what polygon from the dataset ''Wards 2023-'' (8hcu-yrd4) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';


create table if not exists raw_data.chicago_divvy_bicycle_stations (
    "id" text,
    "station_name" text,
    "short_name" text,
    "total_docks" numeric,
    "docks_in_service" numeric,
    "status" text,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    ":@computed_region_awaf_s7ux" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_8hcu_yrd4" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_divvy_bicycle_stations."id" is 'A unique station identifier. NOTE: Extremely long IDs may not display correctly on this page but should be correct if this dataset is exported or accessed through the API.';
comment on column raw_data.chicago_divvy_bicycle_stations."short_name" is 'An alternative identifier used for some purposes.';
comment on column raw_data.chicago_divvy_bicycle_stations."total_docks" is 'Total docks in the station.  Each dock can accommodate one bicycle.';
comment on column raw_data.chicago_divvy_bicycle_stations."docks_in_service" is 'Excludes docks taken out of service.  Calculated as the number of docks reported as containing available bicycles or available to receive a returned bicycle, as of the time this dataset was refreshed.';
comment on column raw_data.chicago_divvy_bicycle_stations."status" is 'The status of the station.  Stations "In Service" are available for use.';
comment on column raw_data.chicago_divvy_bicycle_stations.":@computed_region_awaf_s7ux" is 'This column was automatically created in order to record in what polygon from the dataset ''Historical Wards 2003-2015'' (awaf-s7ux) the point in column ''location_1'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_divvy_bicycle_stations.":@computed_region_6mkv_f3dw" is 'This column was automatically created in order to record in what polygon from the dataset ''Zip Codes'' (6mkv-f3dw) the point in column ''location_1'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_divvy_bicycle_stations.":@computed_region_vrxf_vc4k" is 'This column was automatically created in order to record in what polygon from the dataset ''Community Areas'' (vrxf-vc4k) the point in column ''location_1'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_divvy_bicycle_stations.":@computed_region_bdys_3d7i" is 'This column was automatically created in order to record in what polygon from the dataset ''Census Tracts'' (bdys-3d7i) the point in column ''location_1'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_divvy_bicycle_stations.":@computed_region_43wa_7qmu" is 'This column was automatically created in order to record in what polygon from the dataset ''Wards'' (43wa-7qmu) the point in column ''location_1'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_divvy_bicycle_stations.":@computed_region_rpca_8um6" is 'This column was automatically created in order to record in what polygon from the dataset ''Boundaries - ZIP Codes'' (rpca-8um6) the point in column ''location_1'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_divvy_bicycle_stations.":@computed_region_8hcu_yrd4" is 'This column was automatically created in order to record in what polygon from the dataset ''Wards 2023-'' (8hcu-yrd4) the point in column ''location_1'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';


create table if not exists raw_data.chicago_speed_camera_locations (
    "id" numeric,
    "location_id" text,
    "address" text,
    "first_approach" text,
    "second_approach" text,
    "go_live_date" timestamptz,
    "latitude" text,
    "longitude" text,
    "location" jsonb,
    ":@computed_region_awaf_s7ux" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_speed_camera_locations."id" is 'An internal-only unique ID for the speed camera. This ID will not match any other data source.';
comment on column raw_data.chicago_speed_camera_locations."location_id" is 'An ID for the speed camera that may match other data sources.';


create table if not exists raw_data.chicago_red_light_camera_locations (
    "intersection" text,
    "first_approach" text,
    "second_approach" text,
    "third_approach" text,
    "go_live_date" timestamptz,
    "latitude" numeric,
    "longitude" numeric,
    "location" jsonb,
    ":@computed_region_awaf_s7ux" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);


create table if not exists raw_data.chicago_red_light_camera_violations (
    "intersection" text,
    "camera_id" text,
    "address" text,
    "violation_date" timestamptz,
    "violations" numeric,
    "x_coordinate" numeric,
    "y_coordinate" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" jsonb,
    ":@computed_region_awaf_s7ux" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_red_light_camera_violations."intersection" is 'Intersection of the location of the red light enforcement camera(s). There may be more than one camera at each intersection.';
comment on column raw_data.chicago_red_light_camera_violations."camera_id" is 'A unique ID for each physical camera at an intersection, which may contain more than one camera.';
comment on column raw_data.chicago_red_light_camera_violations."address" is 'The address of the physical camera (CAMERA ID). The address may be the same for all cameras or different, based on the physical installation of each camera.';
comment on column raw_data.chicago_red_light_camera_violations."violation_date" is 'The date of when the violations occurred. NOTE: The citation may be issued on a different date.';
comment on column raw_data.chicago_red_light_camera_violations."violations" is 'Number of violations for each camera on a particular day.';
comment on column raw_data.chicago_red_light_camera_violations."x_coordinate" is 'The X Coordinate, measured in feet, of the location of the camera. Geocoded using Illinois State Plane East (ESRI:102671).';
comment on column raw_data.chicago_red_light_camera_violations."y_coordinate" is 'The Y Coordinate, measured in feet, of the location of the camera. Geocoded using Illinois State Plane East (ESRI:102671).';
comment on column raw_data.chicago_red_light_camera_violations."latitude" is 'The latitude of the physical location of the camera(s) based on the ADDRESS column. Geocoded using the WGS84.';
comment on column raw_data.chicago_red_light_camera_violations."longitude" is 'The longitude of the physical location of the camera(s) based on the ADDRESS column. Geocoded using the WGS84.';
comment on column raw_data.chicago_red_light_camera_violations."location" is 'The coordinates of the camera(s) based on the LATITUDE and LONGITUDE columns. Geocoded using the WGS84.';


create table if not exists raw_data.chicago_speed_camera_violations (
    "address" text,
    "camera_id" text,
    "violation_date" timestamptz,
    "violations" numeric,
    "x_coordinate" numeric,
    "y_coordinate" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" jsonb,
    ":@computed_region_awaf_s7ux" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_speed_camera_violations."address" is 'Address of the location of the speed enforcement camera(s). There may be more than one camera at each address.';
comment on column raw_data.chicago_speed_camera_violations."camera_id" is 'A unique ID associated with the physical camera at each location. There may be more than one camera at a physical address.';
comment on column raw_data.chicago_speed_camera_violations."violation_date" is 'The date of when the violations occurred. NOTE: The citation may be issued on a different date.';
comment on column raw_data.chicago_speed_camera_violations."violations" is 'Number of violations for each camera on a particular day.';
comment on column raw_data.chicago_speed_camera_violations."x_coordinate" is 'The X Coordinate, measured in feet, of the location of the camera. Geocoded using Illinois State Plane East (ESRI:102671).';
comment on column raw_data.chicago_speed_camera_violations."y_coordinate" is 'The Y Coordinate, measured in feet, of the location of the camera. Geocoded using Illinois State Plane East (ESRI:102671).';
comment on column raw_data.chicago_speed_camera_violations."latitude" is 'The latitude of the physical location of the camera(s) based on the ADDRESS column. Geocoded using the WGS84.';
comment on column raw_data.chicago_speed_camera_violations."longitude" is 'The longitude of the physical location of the camera(s) based on the ADDRESS column. Geocoded using the WGS84.';
comment on column raw_data.chicago_speed_camera_violations."location" is 'The coordinates of the camera(s) based on the LATITUDE and LONGITUDE columns.  Geocoded using the WGS84.';


create table if not exists raw_data.chicago_business_owners (
    "account_number" numeric,
    "doing_business_as_name" text,
    "owner_first_name" text,
    "owner_middle_initial" text,
    "owner_last_name" text,
    "owner_name_suffix" text,
    "owner_name" text,
    "owner_title" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);


create table if not exists raw_data.chicago_potholes_patched (
    "address" text,
    "request_date" timestamptz,
    "completion_date" timestamptz,
    "number_of_potholes_filled_on_block" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);


create table if not exists raw_data.chicago_business_licenses (
    "id" text,
    "license_id" numeric,
    "account_number" numeric,
    "site_number" numeric,
    "legal_name" text,
    "doing_business_as_name" text,
    "address" text,
    "city" text,
    "state" text,
    "zip_code" text,
    "ward" numeric,
    "precinct" numeric,
    "ward_precinct" text,
    "police_district" numeric,
    "community_area" numeric,
    "community_area_name" text,
    "neighborhood" text,
    "license_code" numeric,
    "license_description" text,
    "business_activity_id" text,
    "business_activity" text,
    "license_number" numeric,
    "application_type" text,
    "application_created_date" timestamptz,
    "application_requirements_complete" timestamptz,
    "payment_date" timestamptz,
    "conditional_approval" text,
    "license_start_date" timestamptz,
    "expiration_date" timestamptz,
    "license_approved_for_issuance" timestamptz,
    "date_issued" timestamptz,
    "license_status" text,
    "license_status_change_date" timestamptz,
    "ssa" text,
    "latitude" numeric,
    "longitude" numeric,
    "location" jsonb,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_awaf_s7ux" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_business_licenses."id" is 'A calculated ID for each record.';
comment on column raw_data.chicago_business_licenses."license_id" is 'An internal database ID for each record.  Each license can have multiple records as it goes through renewals and other transactions.  See the LICENSE NUMBER field for the number generally known to the public and used in most other data sources that refer to the license.';
comment on column raw_data.chicago_business_licenses."account_number" is 'The account number of the business owner, which will stay consistent across that owner''s licenses and can be used to find the owner in the Business Owners dataset.';
comment on column raw_data.chicago_business_licenses."site_number" is 'An internal database ID indicating the location of this licensed business to account for business owners with more than one location.';
comment on column raw_data.chicago_business_licenses."ward" is 'The ward where the business is located.';
comment on column raw_data.chicago_business_licenses."precinct" is 'The precinct within the ward where the business is located. Note the the same precinct numbers exist in multiple wards.';
comment on column raw_data.chicago_business_licenses."ward_precinct" is 'The ward and precinct where the business is located. This column can be used to filter by precinct more easily across multiple wards. ';
comment on column raw_data.chicago_business_licenses."community_area" is 'Community Area Number';
comment on column raw_data.chicago_business_licenses."community_area_name" is 'Community Area Name';
comment on column raw_data.chicago_business_licenses."license_code" is 'A code for the type of license. Each code value corresponds to a specific LICENSE DESCRIPTION value.';
comment on column raw_data.chicago_business_licenses."business_activity_id" is 'A code for the business activity. Each ID value corresponds to a specific BUSINESS ACTIVITY value.';
comment on column raw_data.chicago_business_licenses."license_number" is 'The license number known to the public and generally used in other data sources that refer to the license. This is the field most users will want for most purposes. Each license has a single license number that stays consistent throughout the lifetime of the license. By contrast, the LICENSE ID field is an internal database ID and not generally useful to external users.';
comment on column raw_data.chicago_business_licenses."application_created_date" is 'The date the business license application was created.  RENEW type records do not have an application.';
comment on column raw_data.chicago_business_licenses."application_requirements_complete" is 'For all application types except RENEW, this is the date all required application documents were received.  For RENEW type records, this is the date the record was created.';
comment on column raw_data.chicago_business_licenses."conditional_approval" is 'This pertains to applications that contain liquor licenses. Customers may request a conditional approval prior to building out the space.';
comment on column raw_data.chicago_business_licenses."license_approved_for_issuance" is 'This is the date the license was ready for issuance. Licenses may not be issued if the customer owes debt to the City. ';
comment on column raw_data.chicago_business_licenses."license_status" is 'See the dataset description for the codes used.';
comment on column raw_data.chicago_business_licenses."ssa" is 'Special Service Areas  are local tax districts that fund expanded services and programs, to foster commercial and economic development, through a localized property tax. In other cities these areas are sometimes called Business Improvement Districts (BIDs). This portal contains a map of all Chicago SSAs';


create table if not exists raw_data.chicago_sidewalk_cafe_permits (
    "permit_number" numeric,
    "account_number" numeric,
    "site_number" numeric,
    "legal_name" text,
    "doing_business_as_name" text,
    "issued_date" timestamptz,
    "expiration_date" timestamptz,
    "payment_date" timestamptz,
    "address" text,
    "address_number_start" text,
    "address_number" text,
    "street_direction" text,
    "street" text,
    "street_type" text,
    "city" text,
    "state" text,
    "zip_code" text,
    "ward" numeric,
    "police_district" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" jsonb,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_sidewalk_cafe_permits."account_number" is 'The account number of the business owner, which will stay consistent across that owner''s licenses and can be used to find the owner in the Business Owners and Business Licenses datasets.';
comment on column raw_data.chicago_sidewalk_cafe_permits."site_number" is 'An internal database ID indicating the location of this licensed business to account for business owners with more than one location.';
comment on column raw_data.chicago_sidewalk_cafe_permits."issued_date" is 'The date the permit was issued.';
comment on column raw_data.chicago_sidewalk_cafe_permits."expiration_date" is 'The date the permit expires.';
comment on column raw_data.chicago_sidewalk_cafe_permits."payment_date" is 'The date the City of Chicago received payment for the permit.';
comment on column raw_data.chicago_sidewalk_cafe_permits."address_number_start" is 'If the permit covers an address range, this is the beginning of the range.  It is the address used for the LATITUDE, LONGITUDE, and LOCATION columns.';
comment on column raw_data.chicago_sidewalk_cafe_permits."address_number" is 'The address number or address number range covered by the permit.';
comment on column raw_data.chicago_sidewalk_cafe_permits."ward" is 'The ward where the business is located.';


create table if not exists raw_data.chicago_food_inspections (
    "inspection_id" numeric,
    "dba_name" text,
    "aka_name" text,
    "license_" numeric,
    "facility_type" text,
    "risk" text,
    "address" text,
    "city" text,
    "state" text,
    "zip" numeric,
    "inspection_date" timestamptz,
    "inspection_type" text,
    "results" text,
    "violations" text,
    "latitude" numeric,
    "longitude" numeric,
    "location" jsonb,
    ":@computed_region_awaf_s7ux" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_food_inspections."dba_name" is 'Doing Business As';
comment on column raw_data.chicago_food_inspections."aka_name" is 'Also Known As';


create table if not exists raw_data.chicago_additional_dwelling_unit_preapproval_applications (
    "id" numeric,
    "submission_date" timestamptz,
    "status" text,
    "status_updated_date" timestamptz,
    "applicant_name" text,
    "owner_name" text,
    "street_number" numeric,
    "street_direction" text,
    "street_name" text,
    "address" text,
    "comm_area" numeric,
    "ward" numeric,
    "adu_zone" text,
    "zoning" text,
    "owner_occ" boolean,
    "adu_type" text,
    "existing_bldg" boolean,
    "year_built" numeric,
    "current_dus" numeric,
    "max_adus" numeric,
    "new_adus" numeric,
    "aff_adus" numeric,
    "prev_adus" boolean,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."id" is 'A unique identifier for the record';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."submission_date" is 'Date when ADU preapplication was electronically submitted';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."status" is 'Status of ADU preapplication';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."status_updated_date" is 'Date when ADU preapplication status was last updated';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."applicant_name" is 'Name of ADU preapproval applicant';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."owner_name" is 'Name of property owner (if different than  applicant)';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."street_number" is 'Number portion of the property address';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."street_direction" is 'Direction portion of the property address';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."street_name" is 'Street name portion of the property address';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."address" is 'Property address';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."comm_area" is 'The Community Area of the property';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."ward" is 'The Ward (City Council district) of the property';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."adu_zone" is 'The Additional Dwelling Unit-Allowed Area that the property is located in (See Section 17-7-0570 of the Zoning Ordinance)';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."zoning" is 'The zoning district of the property';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."owner_occ" is 'Whether the property is owner occupied';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."adu_type" is 'The type of ADU';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."existing_bldg" is 'Whether the preapproval is for an existing building on the lot';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."year_built" is 'Year when the existing building on the lot (if any) was built';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."current_dus" is 'Number of legally-established dwelling units on lot';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."max_adus" is 'Maximum number of ADUs allowed';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."new_adus" is 'Proposed number of ADUs';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."aff_adus" is 'Minimum number of affordable ADUs';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."prev_adus" is 'Whether there is a previous ADU application at the address';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."latitude" is 'The latitude of the property';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."longitude" is 'The longitude of the property';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications."location" is 'The location of the property in a format that allows for mapping and geographic analysis on this data portal.';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications.":@computed_region_rpca_8um6" is 'This column was automatically created in order to record in what polygon from the dataset ''Boundaries - ZIP Codes'' (rpca-8um6) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications.":@computed_region_vrxf_vc4k" is 'This column was automatically created in order to record in what polygon from the dataset ''Community Areas'' (vrxf-vc4k) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications.":@computed_region_6mkv_f3dw" is 'This column was automatically created in order to record in what polygon from the dataset ''Zip Codes'' (6mkv-f3dw) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications.":@computed_region_bdys_3d7i" is 'This column was automatically created in order to record in what polygon from the dataset ''Census Tracts'' (bdys-3d7i) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
comment on column raw_data.chicago_additional_dwelling_unit_preapproval_applications.":@computed_region_43wa_7qmu" is 'This column was automatically created in order to record in what polygon from the dataset ''Wards'' (43wa-7qmu) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';


create table if not exists raw_data.chicago_lending_equity_residential_lending (
    "reporting_year" numeric,
    "bank" text,
    "rfp_source" text,
    "data_description" text,
    "census_tract" text,
    "action_taken" text,
    "total_units" numeric,
    "open_end_line_of_credit" text,
    "loan_type" text,
    "loan_purpose" text,
    "loan_amount" numeric,
    "application_date" timestamptz,
    "interest_rate" numeric,
    "loan_term_months" numeric,
    "total_points_and_fees" numeric,
    "lien_status" text,
    "property_value" numeric,
    "combined_loan_value_ratio" numeric,
    "purchase_price" numeric,
    "down_payment_amount" numeric,
    "reason_for_denial_1" text,
    "reason_for_denial_2" text,
    "reason_for_denial_3" text,
    "reason_for_denial_4" text,
    "race_of_applicant_or_borrower_1" text,
    "race_of_applicant_or_borrower_2" text,
    "race_of_applicant_or_borrower_3" text,
    "race_of_applicant_or_borrower_4" text,
    "race_of_applicant_or_borrower_5" text,
    "race_of_co_applicant_or_borrower_1" text,
    "race_of_co_applicant_or_borrower_2" text,
    "race_of_co_applicant_or_borrower_3" text,
    "race_of_co_applicant_or_borrower_4" text,
    "race_of_co_applicant_or_borrower_5" text,
    "ethnicity_of_applicant_or_borrower_1" text,
    "ethnicity_of_applicant_or_borrower_2" text,
    "ethnicity_of_applicant_or_borrower_3" text,
    "ethnicity_of_applicant_or_borrower_4" text,
    "ethnicity_of_applicant_or_borrower_5" text,
    "ethnicity_of_co_applicant_or_borrower_1" text,
    "ethnicity_of_co_applicant_or_borrower_2" text,
    "ethnicity_of_co_applicant_or_borrower_3" text,
    "ethnicity_of_co_applicant_or_borrower_4" text,
    "ethnicity_of_co_applicant_or_borrower_5" text,
    "sex_of_applicant_or_borrower" text,
    "sex_of_co_applicant_or_borrower" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_lending_equity_residential_lending."reporting_year" is 'The year in which the record was reported, covering bank operations in the previous year.';
comment on column raw_data.chicago_lending_equity_residential_lending."bank" is 'Name of bank submitting data for municipal depository RFP';
comment on column raw_data.chicago_lending_equity_residential_lending."rfp_source" is 'RFP submission item data were drawn from.';
comment on column raw_data.chicago_lending_equity_residential_lending."data_description" is 'The specific data that the bank were required to submit.';
comment on column raw_data.chicago_lending_equity_residential_lending."census_tract" is 'The Census Tract of the property whose loan application is described by the record.';
comment on column raw_data.chicago_lending_equity_residential_lending."action_taken" is 'Indicates action taken on the  loan application by bank. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."total_units" is 'Number of individual dwelling units related to the property secured by the  loan application.';
comment on column raw_data.chicago_lending_equity_residential_lending."open_end_line_of_credit" is 'Indicate whether loan application is for an open line of credit. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."loan_type" is 'Wheter the loan or application is insured by the FHA,VA, RSA, or FSA.  See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."loan_purpose" is 'Purpose of  loan application. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."loan_amount" is 'Amount of loan or amount applied for.';
comment on column raw_data.chicago_lending_equity_residential_lending."application_date" is 'Date of the loan application.';
comment on column raw_data.chicago_lending_equity_residential_lending."interest_rate" is 'Interest rate on the approved application or loan.';
comment on column raw_data.chicago_lending_equity_residential_lending."loan_term_months" is 'Number of months after which the legal obligation will mature or terminate.';
comment on column raw_data.chicago_lending_equity_residential_lending."total_points_and_fees" is 'Total points and fees charges in connection with the loan.';
comment on column raw_data.chicago_lending_equity_residential_lending."lien_status" is 'Whether the property is a first or subordinate lien. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."property_value" is 'Value of property relied on that secured the loan.';
comment on column raw_data.chicago_lending_equity_residential_lending."combined_loan_value_ratio" is 'Ratio of the total amount of debt that is secured by the property to value that was relied on.';
comment on column raw_data.chicago_lending_equity_residential_lending."purchase_price" is 'Purchase price of the property.';
comment on column raw_data.chicago_lending_equity_residential_lending."down_payment_amount" is 'Indicates the down payment amount of the property.';
comment on column raw_data.chicago_lending_equity_residential_lending."reason_for_denial_1" is 'Reason for denial of the application. There may be up to four reasons. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."reason_for_denial_2" is 'Reason for denial of the application. There may be up to four reasons. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."reason_for_denial_3" is 'Reason for denial of the application. There may be up to four reasons. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."reason_for_denial_4" is 'Reason for denial of the application. There may be up to four reasons. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_applicant_or_borrower_1" is 'Race of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_applicant_or_borrower_2" is 'Race of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_applicant_or_borrower_3" is 'Race of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_applicant_or_borrower_4" is 'Race of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_applicant_or_borrower_5" is 'Race of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_co_applicant_or_borrower_1" is 'Race of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_co_applicant_or_borrower_2" is 'Race of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_co_applicant_or_borrower_3" is 'Race of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_co_applicant_or_borrower_4" is 'Race of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."race_of_co_applicant_or_borrower_5" is 'Race of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_applicant_or_borrower_1" is 'Ethnicity of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_applicant_or_borrower_2" is 'Ethnicity of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_applicant_or_borrower_3" is 'Ethnicity of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_applicant_or_borrower_4" is 'Ethnicity of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_applicant_or_borrower_5" is 'Ethnicity of applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_co_applicant_or_borrower_1" is 'Ethnicity of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_co_applicant_or_borrower_2" is 'Ethnicity of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_co_applicant_or_borrower_3" is 'Ethnicity of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_co_applicant_or_borrower_4" is 'Ethnicity of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."ethnicity_of_co_applicant_or_borrower_5" is 'Ethnicity of co-applicant or borrower. The person may list up to five values. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."sex_of_applicant_or_borrower" is 'Sex of applicant or borrower. See the dataset description for a link to the codes used.';
comment on column raw_data.chicago_lending_equity_residential_lending."sex_of_co_applicant_or_borrower" is 'Sex of co-applicant or borrower. See the dataset description for a link to the codes used.';


create table if not exists raw_data.cta_ridership_daily_boarding_totals (
    "service_date" timestamptz,
    "day_type" text,
    "bus" numeric,
    "rail_boardings" numeric,
    "total_rides" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);
