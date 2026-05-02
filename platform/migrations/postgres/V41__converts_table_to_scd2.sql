drop table raw_data.cook_county_neighborhood_boundaries;
create table if not exists raw_data.cook_county_neighborhood_boundaries (
    "multipolygon" geometry(MultiPolygon, 4326),
    "nbhd" text,
    "town_nbhd" text,
    "township_code" text,
    "township_name" text,
    "triad_code" text,
    "triad_name" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "socrata_id" text,
    "socrata_updated_at" timestamptz,
    "socrata_created_at" timestamptz,
    "socrata_version" text,
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'utc'),
    "valid_to" timestamptz
);

alter table raw_data.cook_county_neighborhood_boundaries
    add constraint uq_cook_county_neighborhood_boundaries_entity_hash
    unique (town_nbhd, record_hash);

create index if not exists ix_cook_county_neighborhood_boundaries_current
    on raw_data.cook_county_neighborhood_boundaries (town_nbhd)
    where valid_to is null;

comment on column raw_data.cook_county_neighborhood_boundaries."multipolygon" is 'Geometry (neighborhood boundary)';
comment on column raw_data.cook_county_neighborhood_boundaries."nbhd" is 'Neighborhood number';
comment on column raw_data.cook_county_neighborhood_boundaries."town_nbhd" is 'Township and neighborhood number. First 2 digits are town, last 3 are neighborhood';
comment on column raw_data.cook_county_neighborhood_boundaries."township_code" is 'Township number';
comment on column raw_data.cook_county_neighborhood_boundaries."township_name" is 'Township name';
comment on column raw_data.cook_county_neighborhood_boundaries."triad_code" is 'Triad code. Reassessment of property in Cook County is done within a triennial cycle, meaning it occurs every three years. The Cook County Assessor''s Office alternates reassessments between triads: the north and west suburbs, the south and west suburbs and the City of Chicago.';
comment on column raw_data.cook_county_neighborhood_boundaries."triad_name" is 'Triad name. Reassessment of property in Cook County is done within a triennial cycle, meaning it occurs every three years. The Cook County Assessor''s Office alternates reassessments between triads: the north and west suburbs, the south and west suburbs and the City of Chicago.';