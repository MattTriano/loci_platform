{% macro clean_address(address) %}
  trim(
    upper(
      regexp_replace(
        regexp_replace(
          {{ address }},
          '\s+', ' ', 'g'          -- collapse multiple spaces to one
        ),
        '\s+,', ',', 'g'           -- remove spaces before commas
      )
    )
  )
{% endmacro %}
