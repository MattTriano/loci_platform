create table if not exists raw_data.toronto_serious_motor_vehicle_collisions (
    "collision_id" text,
    "accdate" text,
    "stname1" text,
    "stname2" text,
    "stname3" text,
    "per_inv" text,
    "acclass" text,
    "accloc" text,
    "traffictl" text,
    "impactype" text,
    "visible" text,
    "light" text,
    "rdsfcond" text,
    "road_class" text,
    "failtorem" text,
    "longitude" text,
    "latitude" text,
    "veh_no" text,
    "vehtype" text,
    "initdir" text,
    "per_no" text,
    "invage" text,
    "injury" text,
    "safequip" text,
    "drivact" text,
    "drivcond" text,
    "pedact" text,
    "pedcond" text,
    "manoeuvre" text,
    "pedtype" text,
    "cyclistype" text,
    "cycact" text,
    "cyccond" text,
    "road_user" text,
    "fatal_no" text,
    "wardname" text,
    "division" text,
    "neighbourhood" text,
    "aggressive" text,
    "distracted" text,
    "cyclist" text,
    "motorcyclist" text,
    "other_micromobility" text,
    "older_adult" text,
    "pedestrian" text,
    "red_light" text,
    "school_child" text,
    "heavy_truck" text,
    "geom" geometry,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.toronto_serious_motor_vehicle_collisions
    add constraint uq_toronto_serious_motor_vehicle_collisions_entity_hash
    unique ("collision_id", "per_no", "record_hash");
create index if not exists ix_toronto_serious_motor_vehicle_collisions_current
    on raw_data.toronto_serious_motor_vehicle_collisions ("collision_id", "per_no")
    where valid_to is null;


create table if not exists raw_data.toronto_bicycle_parking_racks (
    "address_point_id" text,
    "address_number" text,
    "linear_name_full" text,
    "address_full" text,
    "postal_code" text,
    "municipality" text,
    "city" text,
    "centreline_id" text,
    "lo_num" text,
    "lo_num_suf" text,
    "hi_num" text,
    "hi_num_suf" text,
    "linear_name_id" text,
    "ward_name" text,
    "mi_prinx" text,
    "objectid" text,
    "capacity" text,
    "multimodal" text,
    "seasonal" text,
    "sheltered" text,
    "surface" text,
    "status" text,
    "location" text,
    "notes" text,
    "map_class" text,
    "geom" geometry,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.toronto_bicycle_parking_racks
    add constraint uq_toronto_bicycle_parking_racks_entity_hash
    unique ("objectid", "record_hash");
create index if not exists ix_toronto_bicycle_parking_racks_current
    on raw_data.toronto_bicycle_parking_racks ("objectid")
    where valid_to is null;
