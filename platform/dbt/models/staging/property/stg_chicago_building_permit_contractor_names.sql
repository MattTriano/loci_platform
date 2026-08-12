-- models/staging/chicago_building_permits/stg_chicago_building_permit_contractor_names.sql
-- One row per distinct normalized contractor name, with the evidence
-- profile used for entity resolution (pair matching + crosswalk).
--
-- Materialized as a table (not a view) because pair generation needs a
-- physical relation for the trigram index; the post-hook drops and
-- recreates the index (dbt's rebuild renames the old table to a backup
-- that still holds an index with this name, so create-if-not-exists
-- would no-op and the index would vanish with the backup).
--
-- permits counts DISTINCT permits, not contact rows: self-performing
-- firms commonly appear on one permit under multiple roles (e.g. solar
-- installers as GENERAL CONTRACTOR + ELECTRICAL CONTRACTOR), which
-- inflated row-based counts by ~2x for those firms.

{{ config(
    materialized='table',
    post_hook=[
        "drop index if exists {{ this.schema }}.{{ this.name }}_name_trgm",
        "create index {{ this.name }}_name_trgm on {{ this }} using gin (name_norm gin_trgm_ops)"
    ]
) }}

with generic_tokens (token) as (

    -- Tokens removed when deriving the distinct keys (the "identifying
    -- part" of a name used for pair scoring).
    --
    -- Inclusion test: a token is generic only if removing it could NOT
    -- collapse two different businesses into one key. Legal suffixes and
    -- trade words pass this test. Person first names and single-letter
    -- initials FAIL it (they are the identity of sole proprietors:
    -- 'J SMITH CONSTRUCTION' vs 'M SMITH CONSTRUCTION') and must never
    -- be added here, however frequent they are.
    --
    -- Derived from a token-frequency scan of this table, 2026-07.

    values
        ('INC'), ('INCORPORATED'), ('LLC'), ('CO'), ('CORP'),
        ('CORPORATION'), ('COMPANY'), ('LTD'), ('THE'), ('AND'), ('DBA'),
        ('CONSTRUCTION'), ('DEVELOPMENT'), ('GROUP'), ('CONTRACTING'),
        ('CONTRACTORS'), ('GENERAL'), ('BUILDERS'), ('REMODELING'),
        ('SERVICES'), ('SERVICE'), ('HEATING'), ('COOLING'), ('MECHANICAL'),
        ('ELECTRIC'), ('ELECTRICAL'), ('PLUMBING'), ('MASONRY')

),
contractor_contacts as (
    select
        permit_,
        name_norm,
        contact_name,
        contact_zip,
        contact_city,
        issue_date
    from {{ ref('stg_chicago_building_permit_contacts') }}
    where contact_type ilike '%CONTRACTOR%'
      and name_norm is not null
      and name_norm not like 'OWNER ACTING%'   -- self-performed work, not a firm
),
profiles as (
    select
        name_norm,
        count(distinct permit_) as permits,   -- not count(*): dual-role firms
        mode() within group (order by contact_zip) as modal_zip,
        mode() within group (order by contact_city) as modal_city,
        (array_agg(contact_name order by issue_date desc))[1] as display_name,
        min(issue_date) as first_seen,
        max(issue_date) as last_seen
    from contractor_contacts
    group by 1
),
keyed as (
    select
        profiles.*,

        -- Sorted + deduplicated: word-order variants and concatenation
        -- artifacts ('X CONTRACTING CONTRACTING') produce the same key.
        -- Equality here means "same bag of identifying words".
        nullif(array_to_string(array(
            select distinct u.t
            from unnest(string_to_array(name_norm, ' '))
                 with ordinality as u(t, pos)
            where u.t not in (select token from generic_tokens)
              and not (
                    u.pos = cardinality(string_to_array(name_norm, ' '))
                    and length(u.t) >= 2
                    and exists (
                        select 1 from generic_tokens g
                        where g.token like u.t || '%'
                          and g.token != u.t
                    )
              )
            order by u.t
        ), ' '), '') as distinct_key,

        -- Same tokens, original order preserved: much stronger evidence,
        -- since it cannot be produced by reordering ('AIR COMFORT' vs
        -- 'COMFORT AIR' differ here).
        nullif(array_to_string(array(
            select u.t
            from unnest(string_to_array(name_norm, ' '))
                 with ordinality as u(t, pos)
            where u.t not in (select token from generic_tokens)
              and not (
                    u.pos = cardinality(string_to_array(name_norm, ' '))
                    and length(u.t) >= 2
                    and exists (
                        select 1 from generic_tokens g
                        where g.token like u.t || '%'
                          and g.token != u.t
                    )
              )
            order by u.pos
        ), ' '), '') as distinct_key_ordered
    from profiles
)

select
    name_norm,
    distinct_key,
    distinct_key_ordered,
    permits,
    modal_zip,
    modal_city,
    display_name,
    first_seen,
    last_seen
from keyed
