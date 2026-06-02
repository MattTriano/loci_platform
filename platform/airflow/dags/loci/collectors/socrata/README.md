# Socrata collector

Socrata (now Tyler Data & Insights) powers open-data portals like `data.cityofchicago.org` and the Cook County portal. Every dataset exposes a SODA API and bulk file exports. This collector pulls a Socrata dataset into a `raw_data` table via either the paginated SODA API (incremental-capable, preserves Socrata's system fields) or a bulk file download (for datasets too large to page through).

Reach for this collector when the source is a Socrata / Tyler open-data portal — you'll see the Socrata dataset UI and a `…/resource/<4x4>.json` API endpoint.

The tooling is four classes: `SocrataTableMetadata` (inspect a dataset), `SocrataDatasetSpec` (declare what to collect and where), `SocrataClient` (talk to the SODA API), and `SocrataCollector` (orchestrate collection and DDL).

## 1. Find the dataset

Socrata identifies every dataset by a four-by-four ID (the "4x4", e.g. `ydr8-5enu`). Grab it from the portal: open the dataset, click **Export → API** (or read it out of the URL), and copy the 4x4. Discovery itself happens on the portal — this collector takes over once you have a 4x4.

## 2. Inspect the dataset

`SocrataTableMetadata` fetches a dataset's schema without touching the warehouse, so you can explore before building anything:

```python
from loci.collectors.socrata.metadata import SocrataTableMetadata

meta = SocrataTableMetadata("ydr8-5enu")   # Chicago building permits
print(meta.domain)           # data.cityofchicago.org
print(meta.dataset_name)     # Building Permits
print(meta.is_geospatial)    # False -> CSV export; True -> GeoJSON export
print(meta.download_format)  # "csv" or "GeoJSON"

meta.print_column_summary()
# Field Name            Socrata Type    PG Type
# ----------------------------------------------
# permit_               text            text
# permit_type           text            text
# ...
```

`print_column_summary()` lists each column's API `field_name`, its Socrata datatype, and the Postgres type the DDL will assign (the mapping lives in `SocrataTableMetadata.SOCRATA_TO_PG_TYPE`). The `field_name` values are what you'll reference in `entity_key`.

Preview real rows straight from the API (no warehouse needed):

```python
import os
from loci.collectors.socrata.client import SocrataClient

client = SocrataClient(app_token=os.environ["SOCRATA_APP_TOKEN"])
rows = client.query(domain=meta.domain, dataset_id="ydr8-5enu", limit=5, include_system_fields=True)
```

`include_system_fields=True` surfaces the Socrata system fields (`:id`, `:updated_at`, …), which the collector renames to `socrata_id`, `socrata_updated_at`, and so on.

## 3. Write the spec

`SocrataDatasetSpec` describes what to pull and where it lands:

```python
from loci.collectors.socrata.spec import SocrataDatasetSpec

CHICAGO_BUILDING_PERMITS_SPEC = SocrataDatasetSpec(
    name="chicago_building_permits",
    dataset_id="ydr8-5enu",
    target_table="chicago_building_permits",
    target_schema="raw_data",
    entity_key=["permit_"],   # stable domain identifier
    full_update_mode="api",   # paginated SODA API
)
```

Field by field:

- `entity_key` — the column(s) that uniquely identify a record, used for SCD2 versioning. Prefer a stable domain identifier from the column summary (`permit_`, `inspection_id`, …). `None` means append-only (no versioning). You *can* use the system column `socrata_id`, but only in `api` mode — see the gotchas.
- `full_update_mode` — `"api"` or `"file_download"`, chosen by size:
  - `"api"` paginates the SODA API, preserves the Socrata system fields, and supports true incremental updates keyed off `:updated_at`. This is the default; use it for anything that pages through in reasonable time.
  - `"file_download"` pulls the bulk export. Every run is a full refresh (no incremental), and exports carry **no** system fields. Use it only for datasets too large to page through.
- `incremental_column` — the Socrata field incrementals key off. Defaults to `:updated_at`; rarely changed.
- `max_rows` — caps rows staged per *incremental* run, so a large `api` dataset can be seeded over several forward-walking runs. Not valid with an `api` full refresh (it would re-ingest the same prefix every run), and the spec rejects `max_rows <= 0`.

### Finding the `entity_key`

You don't need to land the data to pick a key — test against the source while exploring. Two steps:

First, check whether the maintainer already declared one. `SocrataTableMetadata.row_identifier` resolves the dataset's configured row identifier (its primary/upsert key) to a `field_name`, or returns `None` if none is set:

```python
meta.row_identifier   # e.g. "permit_", or None
```

If it's set, that's your `entity_key`. If it's `None` (the row identifier defaults to the system `:id`, which is no help in file mode), verify candidate columns against the SODA API — a single server-side `GROUP BY ... HAVING count(*) > 1`, no table or ingest required:

```python
client.find_duplicate_keys(meta.domain, "ydr8-5enu", ["permit_"])
# []            -> unique: a valid entity_key
# [ {...}, ... ] -> not unique: up to 5 example duplicate groups to inspect
```

Nulls surface naturally — rows sharing a null in the candidate column group together and show up as a duplicate, flagging a null-heavy column as a bad key. Once a candidate comes back empty, put it in the spec and generate the DDL.

### Gotchas

- **`file_download` needs a domain `entity_key`.** File exports have `socrata_id = NULL` on every row, so `entity_key=["socrata_id"]` would collapse every row to one null key and break the SCD2 merge. Pick a real domain identifier for file-mode datasets.
- **Geospatial datasets export as GeoJSON.** When `meta.is_geospatial` is true the collector downloads GeoJSON and the table needs a PostGIS geometry column — which `generate_ddl` produces automatically from the column types.

## 4. Generate the DDL and create the table

```python
from loci.collectors.socrata.collector import SocrataCollector

collector = SocrataCollector(engine=engine, app_token=os.environ["SOCRATA_APP_TOKEN"])
collector.print_ddl(CHICAGO_BUILDING_PERMITS_SPEC)
```

This prints a `create table if not exists …` with one column per source field (typed via the Socrata→PG mapping), the renamed system columns (`socrata_id`, `socrata_updated_at`, …), an `ingested_at` column, and — when `entity_key` is set — the SCD2 columns (`record_hash`, `valid_from`, `valid_to`) plus a unique constraint and a partial "current rows" index. Paste it into a migration and apply it; the collector does not create tables itself.

## 5. Collect

```python
collector.collect(spec, force=True)    # full refresh
collector.collect(spec, force=False)   # incremental update
```

`collect` returns a summary dict (`spec_name`, `mode`, `rows_merged`). What `force` means depends on the mode:

- `api`, `force=True` → full scan of the dataset via the API.
- `api`, `force=False` → incremental: resumes from the table's max `socrata_updated_at` (with `socrata_id` as a tiebreak).
- `file_download`, any `force` → full bulk-export refresh; `force` is ignored.

Drift protection: before a file download the collector peeks the CSV header and checks it against the table; an `api` incremental checks its first page's columns. New source columns surface as schema drift — warn-first on the file path, raising on the api path — so you add them via migration rather than silently dropping data.

## Scheduling

In production the spec is wrapped in a `DatasetUpdateConfig` in `sources/update_configs.py`, and the Socrata taskflow calls `collect(spec, force=…)` based on the schedule (the `choose_update_mode` task decides full vs. incremental for that run).
