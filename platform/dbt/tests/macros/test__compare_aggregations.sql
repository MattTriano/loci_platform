-- tests/macros/test__compare_aggregations.sql
--
-- Singular test for the_compare_aggregations generic test.
-- Uses inline (values ...) tables to simulate models, then calls the macro
-- and checks that it returns the expected number of rows.
-- If this query returns any rows, the test fails.

{% set passing_case %}
    {{ _compare_aggregations(
        model="(values (1), (2), (3)) as t(id)",
        other_model="(values (1), (2), (3)) as t(id)",
        this_model_agg="count(*)",
        other_model_agg="count(*)",
        assertion="this_model_agg = other_model_agg"
    ) }}
{% endset %}

{% set failing_different_counts %}
    {{_compare_aggregations(
        model="(values (1), (2), (3)) as t(id)",
        other_model="(values (1), (2)) as t(id)",
        this_model_agg="count(*)",
        other_model_agg="count(*)",
        assertion="this_model_agg = other_model_agg"
    ) }}
{% endset %}

{% set passing_threshold %}
    {{_compare_aggregations(
        model="(values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) as t(id)",
        other_model="(values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) as t(id)",
        this_model_agg="count(*)",
        other_model_agg="count(*)",
        assertion="other_model_agg::float / this_model_agg >= 0.999"
    ) }}
{% endset %}

{% set failing_threshold %}
    {{_compare_aggregations(
        model="(values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) as t(id)",
        other_model="(values (1), (2), (3), (4), (5)) as t(id)",
        this_model_agg="count(*)",
        other_model_agg="count(*)",
        assertion="other_model_agg::float / this_model_agg >= 0.999"
    ) }}
{% endset %}

select test_name from (

    select 'passing_case: expected 0 rows but got some' as test_name
    where (select count(*) from ({{ passing_case }}) as t) != 0

    union all

    select 'failing_different_counts: expected rows but got 0' as test_name
    where (select count(*) from ({{ failing_different_counts }}) as t) = 0

    union all

    select 'passing_threshold: expected 0 rows but got some' as test_name
    where (select count(*) from ({{ passing_threshold }}) as t) != 0

    union all

    select 'failing_threshold: expected rows but got 0' as test_name
    where (select count(*) from ({{ failing_threshold }}) as t) = 0

) as results