-- models/staging/chicago_building_permits/stg_chicago_building_permit_service_tags.sql
-- One row per (permit, service) where the permit's work_description
-- matches a service keyword pattern. Current permit versions only.
--
-- Materialized as a table: the regex join (~500k descriptions x ~14
-- patterns) is the expensive step, and downstream models reuse it.

{{ config(materialized='table') }}

with service_taxonomy (service, pattern) as (

    -- Keyword patterns over UPPER(work_description). \m/\M are word
    -- boundaries. Extend freely; coverage is measured in step 4 and
    -- unmatched descriptions tell you what to add.

    values
        ('garage',          '\m(GARAGE)\M'),
        ('roofing',         '\m(ROOF|REROOF|SHINGLE)'),
        ('porch_deck',      '\m(PORCH|DECK)\M'),
        ('demolition',      '\m(DEMOLISH|DEMOLITION|WRECK)'),
        ('masonry',         '\m(MASONRY|TUCKPOINT|BRICK|FACADE)'),
        ('electrical',      '\m(ELECTRIC|WIRING|REWIRE|PANEL)'),
        ('plumbing',        '\m(PLUMBING|SEWER|WATER HEATER|DRAIN)'),
        ('hvac',            '\m(HVAC|FURNACE|AIR CONDITION|DUCT|BOILER)'),
        ('windows_doors',   '\m(WINDOW|DOOR)\M'),
        ('fence',           '\m(FENCE)\M'),
        ('solar',           '\m(SOLAR|PHOTOVOLTAIC|PV)\M'),
        ('fire_protection', '\m(SPRINKLER|FIRE ALARM|FIRE PUMP)'),
        ('signs',           '\m(SIGN)\M'),
        ('elevator',        '\m(ELEVATOR|ESCALATOR|LIFT)\M')

),

permits as (

    select permit_, permit_type, issue_date, upper(coalesce(work_description, '')) as descr
    from {{ source('raw_data', 'chicago_building_permits') }}
    where valid_to is null

)

select p.permit_, p.permit_type, p.issue_date, t.service
from permits p
join service_taxonomy t on p.descr ~ t.pattern
