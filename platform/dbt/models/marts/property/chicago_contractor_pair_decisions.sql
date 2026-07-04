-- marts/property/chicago_contractor_pair_decisions.sql
-- Candidate pairs with the v2 decision rule applied.
-- Assumes pg_trgm.similarity_threshold = 0.70 (set at database level).

{{ config(
    materialized='table',
    pre_hook="set pg_trgm.similarity_threshold = 0.70"
) }}

with pairs as (

    select
        a.name_norm as name_a,
        b.name_norm as name_b,
        a.distinct_key as key_a,
        b.distinct_key as key_b,
        a.modal_zip as zip_a,
        b.modal_zip as zip_b,
        a.modal_city as city_a,
        b.modal_city as city_b,
        similarity(a.name_norm, b.name_norm) as name_sim,
        a.distinct_key = b.distinct_key as distinct_exact,
        similarity(a.distinct_key, b.distinct_key) as distinct_sim,
        cardinality(string_to_array(a.distinct_key, ' ')) as key_tokens,
        a.modal_zip is not distinct from b.modal_zip
            and a.modal_zip is not null as zip_match,
        a.permits as permits_a,
        b.permits as permits_b
    from {{ ref('stg_chicago_building_permit_contractor_names') }} as a
    join {{ ref('stg_chicago_building_permit_contractor_names') }} as b
      on a.name_norm < b.name_norm
     and a.name_norm % b.name_norm
    where a.distinct_key is not null
      and b.distinct_key is not null

)

select *,
    case
        -- same identifying tokens; multi-token keys are distinctive enough
        -- alone, single-token keys need location corroboration
        when distinct_exact and (key_tokens >= 2 or zip_match)
            then 'auto'
        -- near-identical identifying tokens (typos, minor truncation
        -- residue) corroborated by location
        when distinct_sim >= 0.90 and zip_match
            then 'auto'
        -- real evidence, no corroboration: human decision, but only
        -- where the merge would affect the analysis
        when (distinct_sim >= 0.85 or (distinct_exact and key_tokens = 1))
             and greatest(permits_a, permits_b) >= 10
            then 'review'
        else 'no_match'
    end as decision
from pairs
