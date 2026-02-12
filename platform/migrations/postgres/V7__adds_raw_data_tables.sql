create table if not exists raw_data.chicago_towed_vehicles (
    "tow_date" timestamptz,
    "make" text,
    "style" text,
    "model" text,
    "color" text,
    "plate" text,
    "state" text,
    "towed_to_address" text,
    "tow_facility_phone" text,
    "inventory_number" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);


create table if not exists raw_data.chicago_traffic_crashes_crashes (
    "crash_record_id" text,
    "crash_date_est_i" text,
    "crash_date" timestamptz,
    "posted_speed_limit" numeric,
    "traffic_control_device" text,
    "device_condition" text,
    "weather_condition" text,
    "lighting_condition" text,
    "first_crash_type" text,
    "trafficway_type" text,
    "lane_cnt" numeric,
    "alignment" text,
    "roadway_surface_cond" text,
    "road_defect" text,
    "report_type" text,
    "crash_type" text,
    "intersection_related_i" text,
    "private_property_i" text,
    "hit_and_run_i" text,
    "damage" text,
    "date_police_notified" timestamptz,
    "prim_contributory_cause" text,
    "sec_contributory_cause" text,
    "street_no" numeric,
    "street_direction" text,
    "street_name" text,
    "beat_of_occurrence" numeric,
    "photos_taken_i" text,
    "statements_taken_i" text,
    "dooring_i" text,
    "work_zone_i" text,
    "work_zone_type" text,
    "workers_present_i" text,
    "num_units" numeric,
    "most_severe_injury" text,
    "injuries_total" numeric,
    "injuries_fatal" numeric,
    "injuries_incapacitating" numeric,
    "injuries_non_incapacitating" numeric,
    "injuries_reported_not_evident" numeric,
    "injuries_no_indication" numeric,
    "injuries_unknown" numeric,
    "crash_hour" numeric,
    "crash_day_of_week" numeric,
    "crash_month" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    ":@computed_region_rpca_8um6" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_traffic_crashes_crashes."crash_record_id" is 'This number can be used to link to the same crash in the Vehicles and People datasets. This number also serves as a unique ID in this dataset.';
comment on column raw_data.chicago_traffic_crashes_crashes."crash_date_est_i" is 'Crash date estimated by desk officer or reporting party (only used in cases where crash is reported at police station days after the crash)';
comment on column raw_data.chicago_traffic_crashes_crashes."crash_date" is 'Date and time of crash as entered by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."posted_speed_limit" is 'Posted speed limit, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."traffic_control_device" is 'Traffic control device present at crash location, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."device_condition" is 'Condition of traffic control device, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."weather_condition" is 'Weather condition at time of crash, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."lighting_condition" is 'Light condition at time of crash, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."first_crash_type" is 'Type of first collision in crash';
comment on column raw_data.chicago_traffic_crashes_crashes."trafficway_type" is 'Trafficway type, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."lane_cnt" is 'Total number of through lanes in either direction, excluding turn lanes, as determined by reporting officer (0 = intersection)';
comment on column raw_data.chicago_traffic_crashes_crashes."alignment" is 'Street alignment at crash location, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."roadway_surface_cond" is 'Road surface condition, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."road_defect" is 'Road defects, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."report_type" is 'Administrative report type (at scene, at desk, amended)';
comment on column raw_data.chicago_traffic_crashes_crashes."crash_type" is 'A general severity classification for the crash. Can be either Injury and/or Tow Due to Crash or No Injury / Drive Away ';
comment on column raw_data.chicago_traffic_crashes_crashes."intersection_related_i" is 'A field observation by the police officer whether an intersection played a role in the crash. Does not represent whether or not the crash occurred within the intersection.';
comment on column raw_data.chicago_traffic_crashes_crashes."private_property_i" is 'Whether the crash begun or first contact was made outside of the public right-of-way.';
comment on column raw_data.chicago_traffic_crashes_crashes."hit_and_run_i" is 'Crash did/did not involve a driver who caused the crash and fled the scene without exchanging information and/or rendering aid';
comment on column raw_data.chicago_traffic_crashes_crashes."damage" is 'A field observation of estimated damage.';
comment on column raw_data.chicago_traffic_crashes_crashes."date_police_notified" is 'Calendar date on which police were notified of the crash';
comment on column raw_data.chicago_traffic_crashes_crashes."prim_contributory_cause" is 'The factor which was most significant in causing the crash, as determined by officer judgment';
comment on column raw_data.chicago_traffic_crashes_crashes."sec_contributory_cause" is 'The factor which was second most significant in causing the crash, as determined by officer judgment';
comment on column raw_data.chicago_traffic_crashes_crashes."street_no" is 'Street address number of crash location, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."street_direction" is 'Street address direction (N,E,S,W) of crash location, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."street_name" is 'Street address name of crash location, as determined by reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."beat_of_occurrence" is 'Chicago Police Department Beat ID. Boundaries available at https://data.cityofchicago.org/d/aerh-rz74';
comment on column raw_data.chicago_traffic_crashes_crashes."photos_taken_i" is 'Whether the Chicago Police Department took photos at the location of the crash';
comment on column raw_data.chicago_traffic_crashes_crashes."statements_taken_i" is 'Whether statements were taken from unit(s) involved in crash';
comment on column raw_data.chicago_traffic_crashes_crashes."dooring_i" is 'Whether crash involved a motor vehicle occupant opening a door into the travel path of a bicyclist, causing a crash';
comment on column raw_data.chicago_traffic_crashes_crashes."work_zone_i" is 'Whether the crash occurred in an active work zone';
comment on column raw_data.chicago_traffic_crashes_crashes."work_zone_type" is 'The type of work zone, if any';
comment on column raw_data.chicago_traffic_crashes_crashes."workers_present_i" is 'Whether construction workers were present in an active work zone at crash location';
comment on column raw_data.chicago_traffic_crashes_crashes."num_units" is 'Number of units involved in the crash. A unit can be a motor vehicle, a pedestrian, a bicyclist, or another non-passenger roadway user. Each unit represents a mode of traffic with an independent trajectory. ';
comment on column raw_data.chicago_traffic_crashes_crashes."most_severe_injury" is 'Most severe injury sustained by any person involved in the crash';
comment on column raw_data.chicago_traffic_crashes_crashes."injuries_total" is 'Total persons sustaining fatal, incapacitating, non-incapacitating, and possible injuries as determined by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."injuries_fatal" is 'Total persons sustaining fatal injuries in the crash';
comment on column raw_data.chicago_traffic_crashes_crashes."injuries_incapacitating" is 'Total persons sustaining incapacitating/serious injuries in the crash as determined by the reporting officer. Any injury other than fatal injury, which prevents the injured person from walking, driving, or normally continuing the activities they were capable of performing before the injury occurred. Includes severe lacerations, broken limbs, skull or chest injuries, and abdominal injuries.';
comment on column raw_data.chicago_traffic_crashes_crashes."injuries_non_incapacitating" is 'Total persons sustaining non-incapacitating injuries in the crash as determined by the reporting officer. Any injury, other than fatal or incapacitating injury, which is evident to observers at the scene of the crash. Includes lump on head, abrasions, bruises, and minor lacerations.';
comment on column raw_data.chicago_traffic_crashes_crashes."injuries_reported_not_evident" is 'Total persons sustaining possible injuries in the crash as determined by the reporting officer. Includes momentary unconsciousness, claims of injuries not evident, limping, complaint of pain, nausea, and hysteria.';
comment on column raw_data.chicago_traffic_crashes_crashes."injuries_no_indication" is 'Total persons sustaining no injuries in the crash as determined by the reporting officer';
comment on column raw_data.chicago_traffic_crashes_crashes."injuries_unknown" is 'Total persons for whom injuries sustained, if any, are unknown';
comment on column raw_data.chicago_traffic_crashes_crashes."crash_hour" is 'The hour of the day component of CRASH_DATE.';
comment on column raw_data.chicago_traffic_crashes_crashes."crash_day_of_week" is 'The day of the week component of CRASH_DATE. Sunday=1

';
comment on column raw_data.chicago_traffic_crashes_crashes."crash_month" is 'The month component of CRASH_DATE.';
comment on column raw_data.chicago_traffic_crashes_crashes."latitude" is 'The latitude of the crash location, as determined by reporting officer, as derived from the reported address of crash';
comment on column raw_data.chicago_traffic_crashes_crashes."longitude" is 'The longitude of the crash location, as determined by reporting officer, as derived from the reported address of crash';
comment on column raw_data.chicago_traffic_crashes_crashes."location" is 'The crash location, as determined by reporting officer, as derived from the reported address of crash, in a column type that allows for mapping and other geographic analysis in the data portal software';
comment on column raw_data.chicago_traffic_crashes_crashes.":@computed_region_rpca_8um6" is 'This column was automatically created in order to record in what polygon from the dataset ''Boundaries - ZIP Codes'' (rpca-8um6) the point in column ''location'' is located.  This enables the creation of region maps (choropleths) in the visualization canvas and data lens.';
