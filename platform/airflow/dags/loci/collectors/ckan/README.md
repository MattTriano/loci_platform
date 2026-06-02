# CKAN collector

CKAN powers open-data portals run by many governments and institutions (data.gov and countless municipal portals expose a CKAN action API). A CKAN dataset — a "package" — bundles one or more *resources* (files: CSV, GeoJSON, …), and some resources are also loaded into CKAN's *DataStore*, a queryable table sitting behind the file. This collector downloads the resource file(s), parses them, and SCD2-merges into a `raw_data` table.

It is full-refresh-only: every run re-downloads and re-merges, with no incremental path. That's fine for the small-to-medium datasets CKAN typically serves.

Reach for this collector when the portal exposes a CKAN action API at `/api/3/action/...`.

The four classes: `CKANMetadata` (browse the portal, inspect resources), `CKANDatasetSpec`, `CKANClient` (download files, query the DataStore), `CKANCollector`.

## 1. Find the dataset

`CKANMetadata` browses the portal; reach it through a client:

```python
from loci.collectors.ckan.client import CKANClient

client = CKANClient("https://data.cityofchicago.org")
meta = client.metadata

meta.search_datasets("food inspections")   # browse the catalog
```

A CKAN dataset is identified by its package name (URL slug) or UUID. Once you have it, list its resources:

```python
resources = meta.find_resources("4ijn-s7e5", "CSV")   # or meta.get_resource(resource_uuid)
for r in resources:
    print(r.id, r.format, r.datastore_active, r.url)
```

Each `CKANResource` carries `id`, `name`, `format`, `url`, and `datastore_active` (whether it's queryable via the DataStore). The spec selects resources either by these `id`s or by format.

## 2. Inspect the dataset

For a DataStore-backed resource you can read its schema without downloading the file:

```python
meta.get_datastore_fields(resource_id)   # [{"id": "inspection_id", "type": "text"}, ...]
```

And preview a few rows straight from the DataStore:

```python
client.datastore_search(resource_id, limit=5)
```

`get_datastore_fields` already strips CKAN's internal columns (`_id`, `_full_text`), so what you see is the actual data schema. For file-only resources (no DataStore) you inspect by previewing through a draft spec (`collector.preview(spec, limit=5)`), which downloads and parses the first few rows.

## 3. Write the spec

```python
from loci.collectors.ckan.spec import CKANDatasetSpec

CHICAGO_FOOD_INSPECTIONS_SPEC = CKANDatasetSpec(
    name="chicago_food_inspections",
    base_url="https://data.cityofchicago.org",
    dataset_id="4ijn-s7e5",
    target_table="chicago_food_inspections",
    entity_key=["inspection_id"],
    resource_format="CSV",
)
```

Field by field:

- `base_url` — the portal root (trailing slash is stripped automatically).
- `dataset_id` — the package slug or UUID.
- `resource_ids` vs `resource_format` — provide exactly one. `resource_ids` (explicit UUIDs) takes precedence and pins specific resources; `resource_format` (e.g. `"CSV"`, `"GeoJSON"`) ingests every matching resource on the dataset.
- `entity_key` — the SCD2 key. `None` means append-only.

### Gotchas

- **Provide `resource_ids` or `resource_format`.** Omitting both raises in `__post_init__` — there's no default selection.
- **Multiple resources feeding one table must share columns.** `generate_ddl` validates this (`_validate_columns_across_resources`) and fails hard on a mismatch, so a table fed by several yearly CSVs stays consistent.
- **CKAN's internal columns are handled for you.** `_id` and `_full_text` are stripped throughout — don't put them in the spec or the table.

### Finding the `entity_key`

Same principle as Socrata — verify uniqueness against the source rather than guessing — but the available tool depends on the resource:

- **DataStore-backed resources** support raw SQL via CKAN's `datastore_search_sql` action, so the uniqueness check is a server-side `GROUP BY ... HAVING count(*) > 1`, no ingest required:

  ```sql
  SELECT "inspection_id", count(*) AS n
  FROM "<resource_id>"
  GROUP BY "inspection_id" HAVING count(*) > 1 LIMIT 5
  ```

  An empty result means the column is unique. (This isn't wrapped in a helper yet — a CKAN analog of the Socrata `find_duplicate_keys` would be the natural addition.)
- **File-only resources** have no server-side query, so fall back to a one-time ingest plus a warehouse `group by ... having count(*) > 1`, or trust an obvious domain id.

## 4. Generate the DDL and create the table

```python
from loci.collectors.ckan.collector import CKANCollector

collector = CKANCollector(engine=engine)
collector.print_ddl(spec)
```

DDL is built from DataStore field types when available, otherwise from CSV/GeoJSON headers (typed as `text`, with a `geom` column added for GeoJSON). It includes `ingested_at` and, when `entity_key` is set, the SCD2 columns plus a unique constraint and a partial current-rows index. Paste it into a migration and apply it.

## 5. Collect

```python
collector.collect(spec, force=True)
```

CKAN is full-refresh-only, so `force` is accepted for interface parity but ignored — every run is a full refresh. `collect` returns a summary dict (`spec_name`, `mode`, `rows_merged`). On the first parsed batch of each resource the collector runs a warn-first column preflight against the table, so a resource that gained a column since the table was created gets flagged (logged by default; flip `raise_on_drift` to enforce) rather than silently dropped.

## Scheduling

The spec is wrapped in a `DatasetUpdateConfig` in `sources/update_configs.py`. The CKAN taskflow is single-path — full refresh → `check_ingestion_log`, with no `choose_update_mode` branch — because there's no incremental mode to choose. That's a deliberate divergence from the other sources. Set whatever cadence you want via `update_cron`.
