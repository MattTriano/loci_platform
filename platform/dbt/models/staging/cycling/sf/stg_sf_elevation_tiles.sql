{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} USING GIST (hull)",
        "ANALYZE {{ this }}"
    ]
) }}

{{ generate_stg_city_elevation_tiles_model('sf') }}
