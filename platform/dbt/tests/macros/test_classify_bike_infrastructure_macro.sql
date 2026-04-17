-- tests/macros/test_classify_bike_infrastructure_macro.sql
--
-- Singular test for the classify_bike_infrastructure macro.
-- Defines input rows (simulating OSMnx edge tags) and expected classification
-- output, applies the macro, and selects any mismatches.
-- If this query returns any rows, the test fails.
--
-- Each test case sets only the tags relevant to that case; all other tag
-- columns default to null. The comment on each row indicates what OSM
-- tagging scenario it represents.

with test_cases as (
    select * from (
        values
        -- ---------------------------------------------------------------
        -- (highway, bicycle, bicycle_road, cyclestreet,
        --  cycleway, cycleway_left, cycleway_right, cycleway_both,
        --  cycleway_buffer, cycleway_left_buffer, cycleway_right_buffer, cycleway_both_buffer,
        --  cycleway_separation, cycleway_left_separation, cycleway_right_separation, cycleway_both_separation,
        --  cycleway_shared_lane, cycleway_left_shared_lane, cycleway_right_shared_lane, cycleway_both_shared_lane,
        --  expected_category, expected_type, expected_has_buffer, case_name)
        -- ---------------------------------------------------------------

        -- highway=cycleway alone → separated track
        ('cycleway', null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'separated', 'track', false, 'highway=cycleway'),

        -- cycleway=track → separated track
        ('residential', null, null, null,
         'track', null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'separated', 'track', false, 'cycleway=track'),

        -- cycleway:right=lane with separation=bollard → protected lane
        ('primary', null, null, null,
         null, null, 'lane', null,
         null, null, null, null,
         null, null, 'bollard', null,
         null, null, null, null,
         'separated', 'protected_lane', false, 'lane with physical separation'),

        -- cycleway:both=lane with buffer → buffered lane
        ('secondary', null, null, null,
         null, null, null, 'lane',
         null, null, null, 'painted_area',
         null, null, null, null,
         null, null, null, null,
         'on_road', 'buffered_lane', true, 'lane with buffer'),

        -- cycleway=lane, no buffer or separation → plain bike lane
        ('tertiary', null, null, null,
         'lane', null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'on_road', 'bike_lane', false, 'plain bike lane'),

        -- cycleway=shared_lane → sharrow
        ('residential', null, null, null,
         'shared_lane', null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'shared', 'sharrow', false, 'shared_lane tag'),

        -- bicycle_road=yes → bicycle road (shared)
        ('residential', null, 'yes', null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'shared', 'bicycle_road', false, 'bicycle_road'),

        -- cyclestreet=yes → bicycle road (shared)
        ('residential', null, null, 'yes',
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'shared', 'bicycle_road', false, 'cyclestreet'),

        -- highway=footway with bicycle=designated → shared_path
        ('footway', 'designated', null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'path', 'shared_path', false, 'footway with bicycle=designated'),

        -- highway=path alone → separated/shared_path
        ('path', null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'separated', 'shared_path', false, 'generic path'),

        -- cycleway=share_busway → share_busway
        ('secondary', null, null, null,
         'share_busway', null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'shared', 'share_busway', false, 'share_busway'),

        -- Left and right differ: left=lane, right=track → right wins (separated)
        ('primary', null, null, null,
         null, 'lane', 'track', null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'separated', 'track', false, 'left=lane right=track picks right'),

        -- cycleway:both takes precedence over missing cycleway
        ('secondary', null, null, null,
         null, null, null, 'lane',
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         'on_road', 'bike_lane', false, 'cycleway:both=lane'),

        -- separation=no should NOT classify as separated
        ('primary', null, null, null,
         null, null, 'lane', null,
         null, null, null, null,
         null, null, 'no', null,
         null, null, null, null,
         'on_road', 'bike_lane', false, 'separation=no does not protect'),

        -- Plain residential street with no bike tags → all null
        ('residential', null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, false, 'no bike infra'),

        -- Primary road with no bike tags → all null (road stays unclassified)
        ('primary', null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, null, null,
         null, null, false, 'arterial with no bike infra')

    ) as t(
        highway, bicycle, bicycle_road, cyclestreet,
        cycleway, cycleway_left, cycleway_right, cycleway_both,
        cycleway_buffer, cycleway_left_buffer, cycleway_right_buffer, cycleway_both_buffer,
        cycleway_separation, cycleway_left_separation, cycleway_right_separation, cycleway_both_separation,
        cycleway_shared_lane, cycleway_left_shared_lane, cycleway_right_shared_lane, cycleway_both_shared_lane,
        expected_category, expected_type, expected_has_buffer, case_name
    )
),

classified as (
    {{ classify_bike_infrastructure('test_cases') }}
)

select
    case_name,
    expected_category,
    osm_infra_category as actual_category,
    expected_type,
    osm_infra_type as actual_type,
    expected_has_buffer,
    __osm_has_buffer as actual_has_buffer
from classified
where
    expected_category is distinct from osm_infra_category
    or expected_type is distinct from osm_infra_type
    or expected_has_buffer is distinct from __osm_has_buffer
