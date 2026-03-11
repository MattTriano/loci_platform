{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if node.config.get('skip_schema_prefix', false) -%}
        {{ custom_schema_name | default(default_schema, true) }}
    {%- elif target.name == 'dev' -%}
        dbt_{{ target.user }}_{{ custom_schema_name | default(default_schema, true) }}
    {%- else -%}
        {{ custom_schema_name | default(default_schema, true) }}
    {%- endif -%}
{%- endmacro %}
