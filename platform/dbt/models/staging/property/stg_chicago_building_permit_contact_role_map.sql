-- models/staging/chicago_building_permits/stg_chicago_building_permit_contact_role_map.sql
-- Exact-value mapping of contractor contact_type values to a role and,
-- for trade roles, a service. Derived from the full enumeration of
-- contact_type values in the dataset (23 values, 2026-07).
--
-- Roles:
--   gc    - general contractor: gets services from permit descriptions
--           and permit types (the description channel)
--   trade - trade contractor: service is declared by the role itself
--   none  - carries no usable service/firm information (self-performing
--           owners, catch-all "other" types); excluded downstream
--
-- New contact_type values from the city will be absent from this map;
-- the assert_all_contractor_contact_types_mapped test surfaces them.

{{ config(materialized='table') }}

select * from (
    values
        ('CONTRACTOR-GENERAL CONTRACTOR',        'gc',    null),
        ('GENERAL CONTRACTOR',                   'gc',    null),
        ('OWNER AS GENERAL CONTRACTOR',          'none',  null),  -- self-performing owner, not a firm
        ('ELECTRICAL CONTRACTOR',                'trade', 'electrical'),
        ('CONTRACTOR-ELECTRICAL',                'trade', 'electrical'),
        ('CONTRACTOR-PLUMBER/PLUMBING',          'trade', 'plumbing'),
        ('PLUMBING CONTRACTOR',                  'trade', 'plumbing'),
        ('CONTRACTOR-VENTILATION',               'trade', 'hvac'),
        ('CONTRACTOR-REFRIGERATION',             'trade', 'hvac'),
        ('CONTRACTOR-HEATING',                   'trade', 'hvac'),
        ('OTHER SUBCONTRACTOR (VENTILATION)',    'trade', 'hvac'),
        ('OTHER SUBCONTRACTOR (REFRIGERATION)',  'trade', 'hvac'),
        ('MASONRY CONTRACTOR',                   'trade', 'masonry'),
        ('MASON CONTRACTOR',                     'trade', 'masonry'),
        ('SIGN CONTRACTOR',                      'trade', 'signs'),
        ('CONTRACTOR-ELEVATOR',                  'trade', 'elevator'),
        ('ELEVATOR MECHANIC CONTRACTOR',         'trade', 'elevator'),
        ('CONTRACTOR-WRECKING',                  'trade', 'demolition'),
        ('WRECKING CONTRACTOR',                  'trade', 'demolition'),
        ('TENT CONTRACTOR',                      'trade', 'tents'),
        ('PRIVATE ALARM CONTRACTOR',             'trade', 'alarm_systems'),
        ('OTHER CONSTRUCTION SUBCONTRACTOR',     'none',  null),
        ('OTHER CONTRACTOR',                     'none',  null)
) as t (contact_type, role, service)
