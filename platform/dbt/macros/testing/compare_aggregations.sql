{% macro _compare_aggregations(model, other_model, this_model_agg, other_model_agg, assertion) %}

with this_model as (
    select {{ this_model_agg }} as this_model_agg
    from {{ model }}
),

other_model as (
    select {{ other_model_agg }} as other_model_agg
    from {{ other_model }}
)

select
    this_model.this_model_agg,
    other_model.other_model_agg
from this_model, other_model
where not ({{ assertion }})

{% endmacro %}