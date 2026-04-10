{% test compare_aggregations(model, other_model, this_model_agg, other_model_agg, assertion) %}

    {{ _compare_aggregations(model, other_model, this_model_agg, other_model_agg, assertion) }}

{% endtest %}
