{{ config(
    materialized='table',
    pre_hook=["SET work_mem = '256MB'"],
    post_hook=[
        "CREATE INDEX ON {{ this }} (segment_id)",
        "CREATE INDEX ON {{ this }} (start_node_id)",
        "CREATE INDEX ON {{ this }} (end_node_id)",
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "RESET work_mem"
    ]
) }}

{{ generate_city_bike_stress_weighted_segments_model('madison', include_crashes=false, include_elevation=true) }}
