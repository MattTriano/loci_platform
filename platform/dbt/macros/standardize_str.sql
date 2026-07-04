{% macro standardize_str(col) %}
    nullif(trim(regexp_replace(
        regexp_replace(upper({{ col }}), '[^A-Z0-9 ]', ' ', 'g'),
        '\s+', ' ', 'g'
    )), '')
{% endmacro %}
