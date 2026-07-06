-- models/staging/chicago_building_permits/stg_chicago_building_permit_service_tags.sql
-- One row per (permit, service). Two channels, deduplicated by the
-- UNION: keyword patterns over work_description, and permit types that
-- directly imply a service (a wrecking permit IS demolition work,
-- whatever the description says). Current permit versions only.
--
-- Taxonomy extended 2026-07 from a word-frequency scan of untagged
-- Express descriptions, which were dominated by low-voltage electrical
-- and alarm/security maintenance work. Note 'FIRE ALARM' matches both
-- fire_protection and alarm_security: deliberate — a permit can carry
-- both tags, and firms doing fire alarm work genuinely offer both.

{{ config(materialized='table') }}

with service_taxonomy (service, pattern) as (

    values
        ('garage',          '\m(GARAGE)\M'),
        ('roofing',         '\m(ROOF|REROOF|SHINGLE)'),
        ('porch_deck',      '\m(PORCH|DECK)\M'),
        ('demolition',      '\m(DEMOLISH|DEMOLITION|WRECK)'),
        ('masonry',         '\m(MASONRY|TUCKPOINT|BRICK|FACADE)'),
        ('electrical',      '\m(ELECTRIC|WIRING|REWIRE|PANEL|VOLTAGE|OUTLET|CIRCUIT|LIGHTING|LIGHT FIXTURE|CABLING|METER)'),
        ('plumbing',        '\m(PLUMBING|SEWER|WATER HEATER|DRAIN)'),
        ('hvac',            '\m(HVAC|FURNACE|AIR CONDITION|DUCT|BOILER)'),
        ('windows_doors',   '\m(WINDOW|DOOR)\M'),
        ('fence',           '\m(FENCE)\M'),
        ('solar',           '\m(SOLAR|PHOTOVOLTAIC|PV)\M'),
        ('fire_protection', '\m(SPRINKLER|FIRE ALARM|FIRE PUMP)'),
        ('alarm_security',  '\m(BURGLAR|ALARM|SECURITY SYSTEM|SECURITY CAMERA|CCTV|ACCESS CONTROL)'),
        ('signs',           '\m(SIGN)\M'),
        ('elevator',        '\m(ELEVATOR|ESCALATOR|LIFT)\M'),
        ('scaffolding',     '\m(SCAFFOLD)'),
        ('tents',           '\m(TENT|CANOPY)\M')

),

permit_type_services (permit_type, service) as (

    values
        ('PERMIT - WRECKING/DEMOLITION', 'demolition'),
        ('PERMIT - SIGNS',               'signs'),
        ('PERMIT - ELEVATOR EQUIPMENT',  'elevator'),
        ('PERMIT - SCAFFOLDING',         'scaffolding'),
        ('PERMIT - PORCH CONSTRUCTION',  'porch_deck')

),

permits as (

    select
        permit_,
        permit_type,
        issue_date,
        upper(coalesce(work_description, '')) as descr
    from {{ source('raw_data', 'chicago_building_permits') }}
    where valid_to is null

)

select p.permit_, p.permit_type, p.issue_date, t.service
from permits p
join service_taxonomy t on p.descr ~ t.pattern

union   -- deliberate: dedupes permits tagged by both channels

select p.permit_, p.permit_type, p.issue_date, s.service
from permits p
join permit_type_services s on s.permit_type = p.permit_type
