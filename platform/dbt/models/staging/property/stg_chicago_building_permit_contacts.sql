-- models/staging/stg_chicago_building_permit_contacts.sql
with permits as (
    select * from {{ source('raw_data', 'chicago_building_permits') }}
    where valid_to is null
),
unioned_contacts as (
    {% for i in range(1, 16) %}
    select
        permit_,
        permit_type,
        issue_date,
        {{ i }} as contact_slot,
        contact_{{ i }}_type as contact_type,
        contact_{{ i }}_name as contact_name,
        contact_{{ i }}_city as contact_city,
        contact_{{ i }}_state as contact_state,
        contact_{{ i }}_zipcode as contact_zip,
        {{ standardize_str('contact_' ~ i ~ '_name') }} as name_norm,
        {{ standardize_str(
            "split_part(upper(contact_" ~ i ~ "_name), ' DBA ', 2)") }} as dba_name_norm
    from permits
    where contact_{{ i }}_name is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    permit_,
    permit_type,
    issue_date,
    contact_slot,
    contact_type,
    contact_name,
    contact_city,
    contact_state,
    left(regexp_replace(trim(contact_zip), '[^0-9 ]', '', 'g'), 5) as contact_zip,
    name_norm,
    dba_name_norm
from unioned_contacts
