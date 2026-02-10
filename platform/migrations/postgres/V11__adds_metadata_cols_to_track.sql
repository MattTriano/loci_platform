alter table meta.ingest_log
    add column if not exists rows_staged numeric,
    add column if not exists rows_merged numeric;