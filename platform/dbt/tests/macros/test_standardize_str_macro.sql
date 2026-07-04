-- tests/macros/test_standardize_str_macro.sql
--
-- Singular test for the standardize_str macro.
-- If this query returns any rows, the test fails.
-- NULL-safe comparison via IS DISTINCT FROM: the macro intentionally
-- returns NULL for empty/punctuation-only input, and `!=` would let
-- those mismatches slip through.

with test_cases as (
    -- casing
    select 'burling builders, inc' as raw_str, 'BURLING BUILDERS INC' as expected_str
    union all
    -- punctuation becomes a space, then collapses
    select 'BURLING BUILDERS, INC.', 'BURLING BUILDERS INC'
    union all
    -- apostrophes and ampersands are punctuation too (documents the
    -- O''BRIEN -> O BRIEN behavior so a future edit here is a conscious choice)
    select 'O''BRIEN & SONS, LLC', 'O BRIEN SONS LLC'
    union all
    select 'A.B.C. CONSTRUCTION', 'A B C CONSTRUCTION'
    union all
    -- hyphens and symbols
    select 'SMITH-JONES BUILDERS #2', 'SMITH JONES BUILDERS 2'
    union all
    -- digits preserved
    select 'GARAGE 2017 LLC', 'GARAGE 2017 LLC'
    union all
    -- interior whitespace collapse + trim
    select '  OAKK   Construction    Co  ', 'OAKK CONSTRUCTION CO'
    union all
    -- already clean
    select 'MAXWELL SERVICES INC', 'MAXWELL SERVICES INC'
    union all
    -- degenerate inputs -> NULL
    select '***', null
    union all
    select '   ', null
    union all
    select '', null
    union all
    select cast(null as text), null
),
standardized as (
    select
        raw_str,
        expected_str,
        {{ standardize_str('raw_str') }} as actual_str
    from test_cases
)

select raw_str, expected_str, actual_str
from standardized
where actual_str is distinct from expected_str
