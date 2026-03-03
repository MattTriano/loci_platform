create table if not exists raw_data.chicago_city_boundary (
    "shape_area" numeric,
    "the_geom" geometry(MultiPolygon, 4326),
    "name" text,
    "objectid" numeric,
    "shape_len" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_city_boundary
    add constraint uq_chicago_city_boundary_entity_hash
    unique (objectid, record_hash);
create index if not exists ix_chicago_city_boundary_current
    on raw_data.chicago_city_boundary (objectid)
    where valid_to is null;


create table if not exists raw_data.chicago_bike_racks (
    "the_geom" geometry(Point, 4326),
    "latitude" numeric,
    "longitude" numeric,
    "location" text,
    "name" text,
    "quantity" numeric,
    "type" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text
);
comment on column raw_data.chicago_bike_racks."quantity" is 'Number of racks at the location.';


create table if not exists raw_data.chicago_community_areas (
    "the_geom" geometry(MultiPolygon, 4326),
    "area_numbe" numeric,
    "community" text,
    "area_num_1" text,
    "shape_area" numeric,
    "shape_len" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_community_areas
    add constraint uq_chicago_community_areas_entity_hash
    unique (area_numbe, record_hash);
create index if not exists ix_chicago_community_areas_current
    on raw_data.chicago_community_areas (area_numbe)
    where valid_to is null;


create table if not exists raw_data.chicago_police_districts (
    "the_geom" geometry(MultiPolygon, 4326),
    "dist_label" text,
    "dist_num" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_police_districts
    add constraint uq_chicago_police_districts_entity_hash
    unique (dist_num, record_hash);
create index if not exists ix_chicago_police_districts_current
    on raw_data.chicago_police_districts (dist_num)
    where valid_to is null;


create table if not exists raw_data.chicago_ward_precincts (
    "the_geom" geometry(MultiPolygon, 4326),
    "shape_leng" numeric,
    "shape_area" numeric,
    "ward_precinct" text,
    "ward" numeric,
    "precinct" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_ward_precincts
    add constraint uq_chicago_ward_precincts_entity_hash
    unique (ward_precinct, record_hash);
create index if not exists ix_chicago_ward_precincts_current
    on raw_data.chicago_ward_precincts (ward_precinct)
    where valid_to is null;
comment on column raw_data.chicago_ward_precincts."shape_leng" is 'Miles';
comment on column raw_data.chicago_ward_precincts."shape_area" is 'Square Miles';
comment on column raw_data.chicago_ward_precincts."ward_precinct" is 'The combination of ward (first two characters) and precinct within that ward (last three characters).that uniquely identifies the precinct.';
comment on column raw_data.chicago_ward_precincts."ward" is 'The ward (City Council district) in which the precinct falls.';
comment on column raw_data.chicago_ward_precincts."precinct" is 'The precinct number within the ward.';


create table if not exists raw_data.chicago_pedway_routes (
    "the_geom" geometry(MultiLineString, 4326),
    "objectid" numeric,
    "ped_route" text,
    "shape_len" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_pedway_routes
    add constraint uq_chicago_pedway_routes_entity_hash
    unique (objectid, record_hash);
create index if not exists ix_chicago_pedway_routes_current
    on raw_data.chicago_pedway_routes (objectid)
    where valid_to is null;


create table if not exists raw_data.chicago_libraries (
    "branch_" text,
    "service_hours" text,
    "address" text,
    "city" text,
    "state" text,
    "zip" text,
    "phone" text,
    "website" text,
    "branch_email" text,
    "location" geometry(Point, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_libraries
    add constraint uq_chicago_libraries_entity_hash
    unique (branch_, record_hash);
create index if not exists ix_chicago_libraries_current
    on raw_data.chicago_libraries (branch_)
    where valid_to is null;


create table if not exists raw_data.chicago_bike_routes (
    "the_geom" geometry(MultiLineString, 4326),
    "street" text,
    "st_name" text,
    "oneway_dir" text,
    "f_street" text,
    "t_street" text,
    "displayrou" text,
    "mi_ctrline" numeric,
    "br_oneway" text,
    "br_ow_dir" text,
    "contraflow" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text
);


create table if not exists raw_data.chicago_building_footprints (
    "the_geom" geometry(MultiPolygon, 4326),
    "bldg_id" numeric,
    "cdb_city_i" text,
    "bldg_statu" text,
    "f_add1" numeric,
    "t_add1" numeric,
    "pre_dir1" text,
    "st_name1" text,
    "st_type1" text,
    "unit_name" text,
    "non_standa" text,
    "bldg_name1" text,
    "bldg_name2" text,
    "comments" text,
    "stories" numeric,
    "orig_bldg_" numeric,
    "footprint_" text,
    "create_use" text,
    "bldg_creat" timestamptz,
    "bldg_activ" timestamptz,
    "bldg_end_d" timestamptz,
    "demolished" timestamptz,
    "edit_date" timestamptz,
    "edit_useri" text,
    "edit_sourc" text,
    "qc_date" timestamptz,
    "qc_userid" text,
    "qc_source" text,
    "x_coord" numeric,
    "y_coord" numeric,
    "z_coord" numeric,
    "harris_str" text,
    "no_of_unit" numeric,
    "no_stories" numeric,
    "year_built" numeric,
    "bldg_sq_fo" numeric,
    "bldg_condi" text,
    "condition_" timestamptz,
    "vacancy_st" text,
    "label_hous" text,
    "suf_dir1" text,
    "shape_area" numeric,
    "shape_len" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_building_footprints
    add constraint uq_chicago_building_footprints_entity_hash
    unique (bldg_id, record_hash);
create index if not exists ix_chicago_building_footprints_current
    on raw_data.chicago_building_footprints (bldg_id)
    where valid_to is null;


create table if not exists raw_data.cta_bus_stops (
    "the_geom" geometry(Point, 4326),
    "systemstop" numeric,
    "street" text,
    "cross_st" text,
    "dir" text,
    "pos" text,
    "routesstpg" text,
    "owlroutes" text,
    "city" text,
    "public_nam" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.cta_bus_stops
    add constraint uq_cta_bus_stops_entity_hash
    unique (systemstop, record_hash);
create index if not exists ix_cta_bus_stops_current
    on raw_data.cta_bus_stops (systemstop)
    where valid_to is null;


create table if not exists raw_data.cta_bus_routes (
    "the_geom" geometry(MultiLineString, 4326),
    "route" text,
    "name" text,
    "wkday" boolean,
    "sat" boolean,
    "sun" boolean,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.cta_bus_routes
    add constraint uq_cta_bus_routes_entity_hash
    unique (route, record_hash);
create index if not exists ix_cta_bus_routes_current
    on raw_data.cta_bus_routes (route)
    where valid_to is null;
comment on column raw_data.cta_bus_routes."route" is 'Route Number';
comment on column raw_data.cta_bus_routes."name" is 'Route Name';
comment on column raw_data.cta_bus_routes."wkday" is 'Does the route run on weekdays?';
comment on column raw_data.cta_bus_routes."sat" is 'Does the route run on Saturday?';
comment on column raw_data.cta_bus_routes."sun" is 'Does the route run on Sunday?';


create table if not exists raw_data.cta_stations (
    "the_geom" geometry(Point, 4326),
    "station_id" numeric,
    "longname" text,
    "lines" text,
    "address" text,
    "ada" boolean,
    "pknrd" boolean,
    "point_x" numeric,
    "point_y" numeric,
    "legend" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);

alter table raw_data.cta_stations
    add constraint uq_cta_stations_entity_hash
    unique (station_id, record_hash);

create index if not exists ix_cta_stations_current
    on raw_data.cta_stations (station_id)
    where valid_to is null;

comment on column raw_data.cta_stations."pknrd" is 'Park and Ride';
comment on column raw_data.cta_stations."legend" is 'Corresponds to the LINES column, for mapping.';


create table if not exists raw_data.chicago_parks (
    "the_geom" geometry(MultiPolygon, 4326),
    "objectid_1" numeric,
    "park_no" numeric,
    "park" text,
    "location" text,
    "zip" text,
    "acres" numeric,
    "ward" numeric,
    "park_class" text,
    "label" text,
    "gisobjid" numeric,
    "wheelchr_a" numeric,
    "archery_ra" numeric,
    "artificial" numeric,
    "band_shell" numeric,
    "baseball_b" numeric,
    "basketball" numeric,
    "basketba_1" numeric,
    "beach" numeric,
    "boat_launc" numeric,
    "boat_lau_1" numeric,
    "boat_slips" numeric,
    "bocce_cour" numeric,
    "bowling_gr" numeric,
    "casting_pi" numeric,
    "football_s" numeric,
    "community_" numeric,
    "conservato" numeric,
    "cultural_c" numeric,
    "dog_friend" numeric,
    "fitness_ce" numeric,
    "fitness_co" numeric,
    "gallery" numeric,
    "garden" numeric,
    "golf_cours" numeric,
    "golf_drivi" numeric,
    "golf_putti" numeric,
    "gymnasium" numeric,
    "gymnastic_" numeric,
    "handball_r" numeric,
    "horseshoe_" numeric,
    "iceskating" numeric,
    "pool_indoo" numeric,
    "baseball_j" numeric,
    "mountain_b" numeric,
    "nature_cen" numeric,
    "pool_outdo" numeric,
    "zoo" numeric,
    "playground" numeric,
    "playgrou_1" numeric,
    "rowing_clu" numeric,
    "volleyball" numeric,
    "senior_cen" numeric,
    "shuffleboa" numeric,
    "skate_park" numeric,
    "sled_hill" numeric,
    "sport_roll" numeric,
    "spray_feat" numeric,
    "baseball_s" numeric,
    "tennis_cou" numeric,
    "track" numeric,
    "volleyba_1" numeric,
    "water_play" numeric,
    "water_slid" numeric,
    "boxing_cen" numeric,
    "wetland_ar" numeric,
    "lagoon" numeric,
    "cricket_fi" numeric,
    "climbing_w" numeric,
    "game_table" numeric,
    "carousel" numeric,
    "croquet" numeric,
    "handball_i" numeric,
    "harbor" numeric,
    "modeltrain" numeric,
    "modelyacht" numeric,
    "nature_bir" numeric,
    "minigolf" numeric,
    "perimeter" numeric,
    "shape_leng" numeric,
    "shape_area" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_parks
    add constraint uq_chicago_parks_entity_hash
    unique (park_no, record_hash);
create index if not exists ix_chicago_parks_current
    on raw_data.chicago_parks (park_no)
    where valid_to is null;


create table if not exists raw_data.chicago_mural_registry (
    "mural_registration_id" text,
    "artist_credit" text,
    "artwork_title" text,
    "media" text,
    "year_installed" text,
    "year_restored" text,
    "location_description" text,
    "street_address" text,
    "zip" text,
    "ward" numeric,
    "affiliated_or_commissioning" text,
    "description_of_artwork" text,
    "community_areas" text,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);

alter table raw_data.chicago_mural_registry
    add constraint uq_chicago_mural_registry_entity_hash
    unique (mural_registration_id, record_hash);

create index if not exists ix_chicago_mural_registry_current
    on raw_data.chicago_mural_registry (mural_registration_id)
    where valid_to is null;

comment on column raw_data.chicago_mural_registry."mural_registration_id" is 'Unique number assigned to each mural registered with the City of Chicago';


create table if not exists raw_data.chicago_landmarks (
    "name" text,
    "id" text,
    "address" text,
    "date_built" text,
    "architect" text,
    "landmark" timestamptz,
    "the_geom" geometry(MultiPolygon, 4326),
    "valid_date" timestamptz,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_landmarks
    add constraint uq_chicago_landmarks_entity_hash
    unique (id, record_hash);
create index if not exists ix_chicago_landmarks_current
    on raw_data.chicago_landmarks (id)
    where valid_to is null;
comment on column raw_data.chicago_landmarks."valid_date" is 'The date on which the records in this dataset were current. It corresponds to the Time Period. All records in the dataset have the same value at any given time. The exact date sometimes is an estimate.';


create table if not exists raw_data.chicago_vacant_abandoned_buildings (
    "docket_number" text,
    "violation_number" text,
    "issued_date" timestamptz,
    "issuing_department" text,
    "last_hearing_date" timestamptz,
    "property_address" text,
    "violation_type" text,
    "entity_or_person_s_" text,
    "disposition_description" text,
    "total_fines" numeric,
    "total_administrative_costs" numeric,
    "interest_amount" numeric,
    "collection_costs_or_attorney_fees" numeric,
    "court_cost" numeric,
    "original_total_amount_due" numeric,
    "total_paid" numeric,
    "current_amount_due" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_vacant_abandoned_buildings
    add constraint uq_chicago_vacant_abandoned_buildings_entity_hash
    unique (violation_number, record_hash);
create index if not exists ix_chicago_vacant_abandoned_buildings_current
    on raw_data.chicago_vacant_abandoned_buildings (violation_number)
    where valid_to is null;


create table if not exists raw_data.chicago_building_scofflaw_list (
    "defendant_owner" text,
    "address" text,
    "secondary_address" text,
    "tertiary_address" text,
    "circuit_court_case_number" text,
    "building_list_date" timestamptz,
    "owner_list_date" timestamptz,
    "community_area" text,
    "community_area_number" text,
    "ward" text,
    "x_coordinate" numeric,
    "y_coordinate" numeric,
    "longitude" numeric,
    "latitude" numeric,
    "location" geometry(Point, 4326),
    "record_id" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_building_scofflaw_list
    add constraint uq_chicago_building_scofflaw_list_entity_hash
    unique (record_id, record_hash);
create index if not exists ix_chicago_building_scofflaw_list_current
    on raw_data.chicago_building_scofflaw_list (record_id)
    where valid_to is null;
comment on column raw_data.chicago_building_scofflaw_list."defendant_owner" is 'A list of the building owners who are named as defendants in the code enforcement proceeding as of the owner list date. Owners are listed one month after the building list date. ';
comment on column raw_data.chicago_building_scofflaw_list."address" is 'Primary address of building (used for geocoding and other geographic information).';
comment on column raw_data.chicago_building_scofflaw_list."secondary_address" is 'An alternative address that could be used to describe building location. This was not used for any geocoding.';
comment on column raw_data.chicago_building_scofflaw_list."tertiary_address" is 'An alternative address that could be used to describe building location. This was not used for any geocoding.';
comment on column raw_data.chicago_building_scofflaw_list."circuit_court_case_number" is 'Case number for the Cook County Circuit Court';
comment on column raw_data.chicago_building_scofflaw_list."building_list_date" is 'The date of the building list for this record.';
comment on column raw_data.chicago_building_scofflaw_list."owner_list_date" is 'The date of the owner list for this record.';
comment on column raw_data.chicago_building_scofflaw_list."community_area" is 'The name of the community area. For context, see: http://en.wikipedia.org/wiki/Community_areas_in_Chicago ';
comment on column raw_data.chicago_building_scofflaw_list."community_area_number" is 'The ID that corresponds with COMMUNITY AREA in this dataset. For context, see: http://en.wikipedia.org/wiki/Community_areas_in_Chicago';
comment on column raw_data.chicago_building_scofflaw_list."ward" is 'The number of the ward (city council district) as of the building  list date';
comment on column raw_data.chicago_building_scofflaw_list."x_coordinate" is 'The east-west coordinate defined in feet, based on NAD 83 - State Plane Eastern IL ';
comment on column raw_data.chicago_building_scofflaw_list."y_coordinate" is 'The north-south coordinate defined in feet, based on NAD 83 - State Plane Eastern IL ';
comment on column raw_data.chicago_building_scofflaw_list."longitude" is 'The longitude of the building (based on the ADDRESS column).';
comment on column raw_data.chicago_building_scofflaw_list."latitude" is 'The latitude of the building (based on the ADDRESS column).';
comment on column raw_data.chicago_building_scofflaw_list."location" is 'The location of the building (based on the ADDRESS column) in a format that allows for mapping and other geographic analysis on this portal.';
comment on column raw_data.chicago_building_scofflaw_list."record_id" is 'A unique identifier for the record.  A record is considered an updated version of a previously existing record if it has the same CIRCUIT COURT CASE NUMBER and BUILDING LIST DATE. This will most commonly happen when the DEFENDANT OWNER value is added a month after the record is first published.';


create table if not exists raw_data.chicago_library_events (
    "title" text,
    "description" text,
    "event_types" text,
    "event_audiences" text,
    "event_languages" text,
    "event_page" text,
    "location_name" text,
    "location_details" text,
    "start" timestamptz,
    "end" timestamptz,
    "featured" boolean,
    "cancelled" boolean,
    "recurring" boolean,
    "registration_starts" timestamptz,
    "registration_ends" timestamptz,
    "registration_closed" boolean,
    "registration_status" text,
    "location_address" text,
    "location_zip" text,
    "location" geometry(Point, 4326),
    "day_of_the_week" text,
    "event_id" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);
alter table raw_data.chicago_library_events
    add constraint uq_chicago_library_events_entity_hash
    unique (event_id, record_hash);
create index if not exists ix_chicago_library_events_current
    on raw_data.chicago_library_events (event_id)
    where valid_to is null;
comment on column raw_data.chicago_library_events."event_types" is 'If the event falls under multiple types, they are all listed, separated by the | (pipe) character.';
comment on column raw_data.chicago_library_events."event_audiences" is 'If the event is for multiple audiences, they are all listed, separated by the | (pipe) character.';
comment on column raw_data.chicago_library_events."event_languages" is 'If the event is in multiple languages, they are all listed, separated by the | (pipe) character.';
comment on column raw_data.chicago_library_events."event_page" is 'Clickable URL to the event page on the Chicago Public Library site.';
comment on column raw_data.chicago_library_events."registration_status" is 'Initial information. Please click the Event Page link to confirm.';
comment on column raw_data.chicago_library_events."day_of_the_week" is 'Based on the Start date.';
comment on column raw_data.chicago_library_events."event_id" is 'A unique ID for the event.';


create table if not exists raw_data.chicago_house_share_restricted_zones (
    "ward" numeric,
    "precinct" numeric,
    "submission_date" timestamptz,
    "comment_period_end_date" timestamptz,
    "ordinance_introduction_date" timestamptz,
    "ordinance_effective_date" timestamptz,
    "scope_of_restriction" text,
    "enacting_ordinance" text,
    "repeal_submission_date" timestamptz,
    "repeal_comment_period_end_date" timestamptz,
    "repeal_ordinance_introduction_date" timestamptz,
    "repeal_ordinance_effective_date" timestamptz,
    "repeal_ordinance" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);

alter table raw_data.chicago_house_share_restricted_zones
    add constraint uq_chicago_house_share_restricted_zones_entity_hash
    unique (precinct, record_hash);

create index if not exists ix_chicago_house_share_restricted_zones_current
    on raw_data.chicago_house_share_restricted_zones (precinct)
    where valid_to is null;

comment on column raw_data.chicago_house_share_restricted_zones."ward" is 'The Ward (City Council district)';
comment on column raw_data.chicago_house_share_restricted_zones."precinct" is 'The Precinct within the ward. The same precinct numbers exist in different wards so this column is only meaningful in combination with the Ward column';
comment on column raw_data.chicago_house_share_restricted_zones."submission_date" is 'Date the petition was filed to prohibit house share rentals in this precinct.';
comment on column raw_data.chicago_house_share_restricted_zones."comment_period_end_date" is 'Deadline for public comments on the proposed prohibition.';
comment on column raw_data.chicago_house_share_restricted_zones."ordinance_introduction_date" is 'Date the City Council ordinance was introduced to prohibit house share rentals in this precinct.';
comment on column raw_data.chicago_house_share_restricted_zones."ordinance_effective_date" is 'Date when the prohibition on house share rentals in this precinct took effect or will take effect, at 10 am.';
comment on column raw_data.chicago_house_share_restricted_zones."scope_of_restriction" is 'The restriction placed by the ordinance or proposed if no ordinance has passed yet.';
comment on column raw_data.chicago_house_share_restricted_zones."enacting_ordinance" is 'A link to the City Council ordinance to prohibit house share rentals in this precinct.';
comment on column raw_data.chicago_house_share_restricted_zones."repeal_submission_date" is 'Date the petition was filed to repeal the prohibition on house share rentals in this precinct.';
comment on column raw_data.chicago_house_share_restricted_zones."repeal_comment_period_end_date" is 'Deadline for public comments on the proposed prohibition repeal.';
comment on column raw_data.chicago_house_share_restricted_zones."repeal_ordinance_introduction_date" is 'Date the City Council ordinance was introduced to repeal the prohibition on house share rentals in this precinct.';
comment on column raw_data.chicago_house_share_restricted_zones."repeal_ordinance_effective_date" is 'Date on which the prohibition on house share rentals in this precinct was lifted or will be lifted.';
comment on column raw_data.chicago_house_share_restricted_zones."repeal_ordinance" is 'A link to the City Council ordinance to repeal the prohibition on house share rentals in this precinct.';



