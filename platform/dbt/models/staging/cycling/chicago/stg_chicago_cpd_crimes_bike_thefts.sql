select
    cc.id,
    cc.case_number,
    cc.date,
    cc.block,
    cc.cleaned_block,
    cc.full_address,
    cc.iucr,
    cc.primary_type,
    cc.description,
    cc.location_description,
    cc.arrest,
    cc.domestic,
    cc.beat,
    cc.district,
    cc.ward,
    cc.community_area,
    cc.fbi_code,
    cc.year,
    cc.updated_on,
    coalesce(cc.latitude, gac.latitude) as latitude,
    coalesce(cc.longitude, gac.longitude) as longitude,
    coalesce(cc.location, gac.geom) as location
from {{ ref('stg_chicago_crimes') }} as cc
left join {{ ref('geocoded_address_cache') }} as gac
on cc.full_address = gac.raw_address
where iucr in ('0917', '0918')
