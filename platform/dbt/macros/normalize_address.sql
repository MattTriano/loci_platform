{# Produces a consistent normalized address string for hashing and deduplication.
   Accepts either structured parts or a full address string (or both — structured parts
   take priority). Trims, upper-cases, and collapses whitespace.

   Parameters that aren't passed are safely ignored.

   Usage:
     {{ normalize_address(full_address='my_address_col') }}
     {{ normalize_address(street='street', city='city', state='state', zip='zip') }}
#}

{% macro normalize_address(full_address=none, street=none, city=none, state=none, zip=none) %}
  {% set has_parts = street is not none or city is not none or state is not none or zip is not none %}
  {% set has_full = full_address is not none %}

  upper(
    regexp_replace(
      trim(
        {% if has_parts and has_full %}
        coalesce(
          nullif(
            concat_ws(', ',
              {{ "nullif(trim(" ~ street ~ "), '')" if street is not none else "null" }},
              {{ "nullif(trim(" ~ city ~ "), '')" if city is not none else "null" }},
              {{ "nullif(trim(" ~ state ~ "), '')" if state is not none else "null" }},
              {{ "nullif(trim(" ~ zip ~ "), '')" if zip is not none else "null" }}
            ),
            ''
          ),
          trim({{ full_address }})
        )
        {% elif has_parts %}
        nullif(
          concat_ws(', ',
            {{ "nullif(trim(" ~ street ~ "), '')" if street is not none else "null" }},
            {{ "nullif(trim(" ~ city ~ "), '')" if city is not none else "null" }},
            {{ "nullif(trim(" ~ state ~ "), '')" if state is not none else "null" }},
            {{ "nullif(trim(" ~ zip ~ "), '')" if zip is not none else "null" }}
          ),
          ''
        )
        {% elif has_full %}
        trim({{ full_address }})
        {% else %}
        null::text
        {% endif %}
      ),
      '\s+', ' ', 'g'
    )
  )
{% endmacro %}


{# Produces a hex-encoded SHA-256 hash of the normalized address.
   Pass the same arguments as normalize_address.

   Usage:
     {{ address_hash(full_address='my_address_col') }}
#}

{% macro address_hash(full_address=none, street=none, city=none, state=none, zip=none) %}
  md5(
    {{ normalize_address(full_address=full_address, street=street, city=city, state=state, zip=zip) }}
  )
{% endmacro %}
