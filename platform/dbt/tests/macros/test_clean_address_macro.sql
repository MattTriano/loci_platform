-- tests/macros/test_clean_address_macro.sql
--
-- Singular test for the clean_address macro.
-- Defines input/expected pairs, applies the macro, and selects any mismatches.
-- If this query returns any rows, the test fails.

with test_cases as (
    select '12XX w North  st' as raw_addr, '12XX W NORTH ST' as expected_addr
    union all
    select '  035XX N BROADWAY  ', '035XX N BROADWAY'
    union all
    select '001XX   E   ONTARIO ST', '001XX E ONTARIO ST'
    union all
    select '014XX W TOUHY  AV', '014XX W TOUHY AV'
    union all
    select '065XX S LANGLEY   AV', '065XX S LANGLEY AV'
    union all
    -- space before comma
    select '123 MAIN ST , APT 4', '123 MAIN ST, APT 4'
    union all
    -- multiple spaces before comma
    select '123 MAIN ST   , APT 4', '123 MAIN ST, APT 4'
    union all
    -- already clean
    select '083XX S PAULINA ST', '083XX S PAULINA ST'
    union all
    -- mixed case with extra whitespace
    select '  069xx   w grand   av  ', '069XX W GRAND AV'
),
cleaned as (
    select
        raw_addr,
        expected_addr,
        {{ clean_address('raw_addr') }} as cleaned_addr
    from test_cases
)

select
    raw_addr,
    expected_addr,
    cleaned_addr
from cleaned
where cleaned_addr != expected_addr
