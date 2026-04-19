create table raw_data.detroit_bike_parking (
    "objectid" bigint,
    "name" text,
    "point_type" text,
    "address" text,
    "city" text,
    "notes" text,
    "data_source" text,
    "updated" text,
    "installed" text,
    "manufacturer" text,
    "photo" text,
    "racks" text,
    "capacity" text,
    "covered" text,
    "geom" geometry(Point, 2253),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.detroit_bike_parking
    add constraint uq_detroit_bike_parking_entity_hash
    unique ("geom", "record_hash");
create index ix_detroit_bike_parking_current
    on raw_data.detroit_bike_parking ("geom")
    where "valid_to" is null;
create index ix_detroit_bike_parking_geom
    on raw_data.detroit_bike_parking using gist ("geom");


create table raw_data.detroit_bike_lanes (
    "bike_route_id" text,
    "jurisdiction" text,
    "route_name" text,
    "on_road_type" text,
    "trail_name" text,
    "protected_bike_lane_type" text,
    "trail_type" text,
    "objectid" bigint,
    "shape__length" double precision,
    "geom" geometry(MultiLineString, 3857),
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.detroit_bike_lanes
    add constraint uq_detroit_bike_lanes_entity_hash
    unique ("bike_route_id", "record_hash");
create index ix_detroit_bike_lanes_current
    on raw_data.detroit_bike_lanes ("bike_route_id")
    where "valid_to" is null;
create index ix_detroit_bike_lanes_geom
    on raw_data.detroit_bike_lanes using gist ("geom");


create table raw_data.detroit_traffic_crashes (
    "objectid" bigint,
    "crash_id" integer,
    "primary_road" text,
    "intersecting_road" text,
    "crash_date" text,
    "day" smallint,
    "month" smallint,
    "year" smallint,
    "hour" smallint,
    "weekday" smallint,
    "crash_type_code" smallint,
    "highway_classification_code" smallint,
    "community_code" text,
    "jurisdiction_code" smallint,
    "lane_departure_type_code" smallint,
    "surface_type" text,
    "road_condition_code" smallint,
    "weather_condition_code" smallint,
    "lighting_condition_code" smallint,
    "speed_limit" smallint,
    "num_lanes" smallint,
    "num_units" smallint,
    "num_occupants" smallint,
    "most_severe_injury_code" smallint,
    "num_fatal_injuries" smallint,
    "num_suspected_serious_injuries" smallint,
    "num_suspected_minor_injuries" smallint,
    "num_possible_injuries" smallint,
    "is_property_damage_only" text,
    "is_secondary_crash" text,
    "is_traffic_control_disregarded" text,
    "is_red_light_run_involved" text,
    "is_hit_and_run_involved" text,
    "is_alcohol_involved" text,
    "is_drug_involved" text,
    "is_unbelted_person_involved" text,
    "is_work_zone_involved" text,
    "is_speeding_driver_involved" text,
    "is_distracted_driver_involved" text,
    "is_driveway_involved" text,
    "is_pedestrian_involved" text,
    "is_elderly_driver_involved" text,
    "is_young_driver_involved" text,
    "is_commercial_vehicle_involved" text,
    "is_emergency_vehicle_involved" text,
    "is_train_involved" text,
    "is_school_bus_involved" text,
    "is_motorcycle_involved" text,
    "is_bicycle_involved" text,
    "is_deer_involved" text,
    "longitude" real,
    "latitude" real,
    "_orig_geom" text,
    "_orig_geog" text,
    "geom" geometry(Point, 4326),
    "source_layer" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.detroit_traffic_crashes
    add constraint uq_detroit_traffic_crashes_entity_hash
    unique ("crash_id", "record_hash");
create index ix_detroit_traffic_crashes_current
    on raw_data.detroit_traffic_crashes ("crash_id")
    where "valid_to" is null;
create index ix_detroit_traffic_crashes_geom
    on raw_data.detroit_traffic_crashes using gist ("geom");


create table raw_data.osmnx_detroit_bike_network_nodes (
    osmid bigint not null,
    latitude double precision,
    longitude double precision,
    street_count integer,
    highway text,
    ref text,
    geom geometry(Point, 4326),
    tile_id text,
    ingested_at timestamptz not null default (now() at time zone 'UTC'),
    record_hash text not null,
    valid_from timestamptz not null default (now() at time zone 'UTC'),
    valid_to timestamptz
);
alter table raw_data.osmnx_detroit_bike_network_nodes
    add constraint uq_osmnx_detroit_bike_network_nodes_entity_hash
    unique (osmid, record_hash);
create index ix_osmnx_detroit_bike_network_nodes_current
    on raw_data.osmnx_detroit_bike_network_nodes (osmid)
    where valid_to is null;
create index ix_osmnx_detroit_bike_network_nodes_geom
    on raw_data.osmnx_detroit_bike_network_nodes using gist (geom);
create index ix_osmnx_detroit_bike_network_nodes_tile_id
    on raw_data.osmnx_detroit_bike_network_nodes (tile_id)
    where valid_to is null;


create table raw_data.osmnx_detroit_bike_network_edges (
    u bigint not null,
    v bigint not null,
    key integer not null,
    osmid text,
    length_m double precision,
    name text,
    highway text,
    oneway boolean,
    reversed text,
    maxspeed text,
    surface text,
    lanes text,
    ref text,
    service text,
    width text,
    lit text,
    access text,
    bridge text,
    tunnel text,
    bicycle text,
    bicycle_lanes text,
    bicycle_lanes_backward text,
    bicycle_lanes_forward text,
    bicycle_right text,
    bicycle_road text,
    class_bicycle text,
    cyclestreet text,
    oneway_bicycle text,
    ramp_bicycle text,
    sidewalk_both_bicycle text,
    cycleway text,
    cycleway_buffer text,
    cycleway_lane text,
    cycleway_oneway text,
    cycleway_separation text,
    cycleway_shared_lane text,
    cycleway_smoothness text,
    cycleway_surface text,
    cycleway_both text,
    cycleway_both_buffer text,
    cycleway_both_colour text,
    cycleway_both_lane text,
    cycleway_both_separation text,
    cycleway_both_shared_lane text,
    cycleway_both_traffic_sign text,
    cycleway_left text,
    cycleway_left_buffer text,
    cycleway_left_lane text,
    cycleway_left_oneway text,
    cycleway_left_separation text,
    cycleway_left_shared_lane text,
    cycleway_left_traffic_sign text,
    cycleway_right text,
    cycleway_right_buffer text,
    cycleway_right_lane text,
    cycleway_right_oneway text,
    cycleway_right_separation text,
    cycleway_right_shared_lane text,
    cycleway_right_traffic_sign text,
    geom geometry(LineString, 4326),
    tile_id text,
    ingested_at timestamptz not null default (now() at time zone 'UTC'),
    record_hash text not null,
    valid_from timestamptz not null default (now() at time zone 'UTC'),
    valid_to timestamptz
);
alter table raw_data.osmnx_detroit_bike_network_edges
    add constraint uq_osmnx_detroit_bike_network_edges_entity_hash
    unique (u, v, key, record_hash);
create index ix_osmnx_detroit_bike_network_edges_current
    on raw_data.osmnx_detroit_bike_network_edges (u, v, key)
    where valid_to is null;
create index ix_osmnx_detroit_bike_network_edges_geom
    on raw_data.osmnx_detroit_bike_network_edges using gist (geom);
create index ix_osmnx_detroit_bike_network_edges_u
    on raw_data.osmnx_detroit_bike_network_edges (u)
    where valid_to is null;
create index ix_osmnx_detroit_bike_network_edges_v
    on raw_data.osmnx_detroit_bike_network_edges (v)
    where valid_to is null;
create index ix_osmnx_detroit_bike_network_edges_tile_id
    on raw_data.osmnx_detroit_bike_network_edges (tile_id)
    where valid_to is null;
