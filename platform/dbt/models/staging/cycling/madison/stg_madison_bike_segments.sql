{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} (way_id, start_position, end_position)",
        "CREATE INDEX ON {{ this }} (start_node_id)",
        "CREATE INDEX ON {{ this }} (end_node_id)",
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

{{ generate_stg_city_bike_segments_model('madison') }}
