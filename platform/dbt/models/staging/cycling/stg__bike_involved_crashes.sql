with bike_crash_ids as (
    select crash_record_id
    from {{ source('raw_data', 'chicago_traffic_crashes_vehicles') }}
    where unit_type = 'BICYCLE' and valid_to is null
        union
    select crash_record_id
    from {{ source('raw_data', 'chicago_traffic_crashes_people') }}
    where person_type = 'BICYCLE' and valid_to is null
        union
    select crash_record_id
    from {{ source('raw_data', 'chicago_traffic_crashes_crashes') }}
    where first_crash_type = 'PEDALCYCLIST' and valid_to is null
),
crashes as (
    select
        crash_record_id,
        crash_date,
        crash_date at time zone 'America/Chicago' as local_crash_date,
        to_char(crash_date at time zone 'America/Chicago', 'Day') as local_crash_day_of_week,
        (crash_date at time zone 'America/Chicago')::time as local_crash_time,
        -- Road context
        posted_speed_limit,
        traffic_control_device,
        trafficway_type,
        intersection_related_i,
        roadway_surface_cond,
        road_defect,
        alignment,
        lane_cnt,
        -- Conditions
        weather_condition,
        lighting_condition,
        -- Crash classification
        first_crash_type,
        crash_type,
        prim_contributory_cause,
        sec_contributory_cause,
        most_severe_injury,
        damage,
        hit_and_run_i,
        dooring_i,
        -- Injury counts
        injuries_total,
        injuries_fatal,
        injuries_incapacitating,
        injuries_non_incapacitating,
        injuries_reported_not_evident,
        -- Location
        street_no,
        street_direction,
        street_name,
        beat_of_occurrence,
        latitude,
        longitude,
        location as geom
    from {{ source('raw_data', 'chicago_traffic_crashes_crashes') }}
    inner join bike_crash_ids
    using (crash_record_id)
    where valid_to is null
),
bike_crash_people as (
    select
        person_id,
        person_type,
        crash_record_id,
        vehicle_id,
        pedpedal_action as cyclist_action,
        pedpedal_visibility as cyclist_visibility,
        pedpedal_location as cyclist_location,
        sex,
        age,
        safety_equipment,
        ejection,
        injury_classification,
        driver_action,
        driver_vision,
        physical_condition as driver_physical_condition,
        bac_result,
        bac_result_value,
        cell_phone_use
    from {{ source('raw_data', 'chicago_traffic_crashes_people') }}
    where
        valid_to is null
        and person_type = 'BICYCLE'
),
merged as (
    select *
    from crashes
    left join bike_crash_people
    using (crash_record_id)
)

select * from merged
