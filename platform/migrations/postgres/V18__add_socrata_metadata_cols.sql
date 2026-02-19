alter table raw_data.cook_county_single_and_multi_family_improvement_characteristics
    add column if not exists socrata_id text,
    add column if not exists socrata_updated_at timestamptz,
    add column if not exists socrata_created_at timestamptz,
    add column if not exists socrata_version text;