{{ config(materialized='table') }}

{{ generate_city_segment_crash_costs_model(
    'chicago',
    crash_weight=24.0,
    crash_decay_lambda=0.4,
    crash_buffer_degrees=0.0002
) }}
