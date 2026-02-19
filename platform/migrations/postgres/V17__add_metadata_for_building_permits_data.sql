alter table raw_data.chicago_building_permits
    add column if not exists socrata_id text,
    add column if not exists socrata_updated_at timestamptz,
    add column if not exists socrata_created_at timestamptz,
    add column if not exists socrata_version text;
