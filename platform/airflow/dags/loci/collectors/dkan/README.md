# DKAN Collector

Collection tooling for [DKAN](https://getdkan.org/) data portals. One source serves every DKAN portal; the portal is identified by `base_url` on the spec, and the collector caches one client per portal.

Known CMS portals running DKAN:

| Portal | base_url | Contents |
|---|---|---|
| Provider Data Catalog | `https://data.cms.gov/provider-data` | Care Compare data: hospital/nursing-home/etc. quality and facility info |
| Open Payments | `https://openpaymentsdata.cms.gov` | Industry payments to physicians and teaching hospitals |

Note: despite the name, DKAN is **not** API-compatible with CKAN (it's a Drupal-based platform inspired by CKAN). The CKAN collector won't talk to these portals and vice versa.

## Classes

Standard four-class collector interface:

* `DKANClient` (`client.py`) — thin HTTP client for one portal: cached metastore catalog fetch, datastore query paging (capped at 500 rows/page by DKAN), datastore row counts, and retrying streamed file downloads.
* `DKANMetadata` (`metadata.py`) — catalog exploration: title search, dataset lookup, distribution listing, column sampling, and a `describe()` summary for notebook use.
* `DKANSpec` (`spec.py`) — declares what to collect and where it goes. Subclass of `DatasetSpec`.
* `DKANCollector` (`collector.py`) — orchestrates collection: `collect(spec, force)` and `generate_ddl(spec)`.

Concrete spec instances live in `instances.py`.

## Quick start

```python
from loci.collectors.dkan.client import DKANClient
from loci.collectors.dkan.metadata import DKANMetadata
from loci.collectors.dkan.collector import DKANCollector
from loci.collectors.dkan.spec import DKANSpec

# Explore
meta = DKANMetadata(DKANClient("https://data.cms.gov/provider-data"))
meta.titles("hospital")
meta.describe("Hospital General Information", stats=True)

# Specify
spec = DKANSpec(
    name="pdc_hospital_general_information",
    base_url="https://data.cms.gov/provider-data",
    dataset_identifiers=["xubh-q36u"],
    target_table="pdc_hospital_general_information",
    entity_key=["facility_id"],
    retrieval="datastore",
)

# Create the table (run the DDL via a migration), then collect
collector = DKANCollector(engine=engine)
collector.print_ddl(spec)
summary = collector.collect(spec)            # incremental: skips fresh datasets
summary = collector.collect(spec, force=True)  # recollect everything
```

## Update semantics

The unit of work is one metastore **dataset**. DKAN datasets are refreshed in place — one identifier, a dataset-level `modified` date, no version array — so `collect(spec, force=False)` recollects a dataset when it is missing from the target, incomplete, or its modified date has advanced. `force=True` recollects everything. All ingestion is `StagedIngest` in SCD2 mode, so recollection is always safe: unchanged rows dedupe away.

Every row is stamped with two hash-excluded provenance columns: `_source_dataset` (which dataset it came from) and `_source_modified` (the dataset's modified date at collection time). A dataset is considered fresh when both hold:

1. current target rows for that `_source_dataset` `>=` the datastore's row count (`>=`, not `==`, because with `invalidate_missing=False` rows removed at the source remain current in the target), and
2. the stored max `_source_modified` `>=` the catalog's modified date.

After each successful ingest, `_source_modified` is advanced on the dataset's current rows. Without this, a re-publication whose rows mostly dedupe away would leave stale stamps behind and the dataset would be re-downloaded on every run.

## Multi-dataset families

A spec may carry several `dataset_identifiers` landing in one target table — e.g. Open Payments publishes one dataset per program year. Each sibling is checked and collected independently, and a failure in one doesn't block the others.

**The entity key must distinguish entities across siblings** (e.g. `["record_id", "program_year"]` for Open Payments). If it doesn't, rows from sibling datasets collide as "changed" versions of one entity and silently corrupt SCD2 history.

Note that Open Payments republishes *every* program year each January (corrections and dispute resolutions apply retroactively), so expect an annual recollection of every year in the spec.

## Retrieval modes

* `retrieval="datastore"` — pages rows out of the datastore API at 500 rows/page. Fine up to a few hundred thousand rows per dataset.
* `retrieval="file"` — downloads the dataset's distribution file (assumed CSV) to a tempfile and stream-parses it. Use for the multimillion-row datasets (a single Open Payments year is ~11M rows / ~6 GB).

The two modes are hash-equivalent: switching a spec's retrieval mode never churns SCD2 versions (see normalization below; empty strings vs NULLs also hash identically via `StagedIngest`'s `coalesce` hashing).

## Column normalization

DKAN's datastore serves normalized column names while distribution files keep the original headers, so file-mode headers are normalized with **DKAN's own rule** to keep the modes consistent: lowercase, whitespace → underscore, all other punctuation **dropped** (so `County/Parish` → `countyparish`, not `county_parish` — this deliberately differs from the CKAN collector's normalizer).

Names are also truncated to Postgres's 63-character identifier limit with `_2`/`_3` collision suffixing. This is load-bearing: Open Payments has a 64-character column that would otherwise be silently truncated by Postgres at DDL time and then dropped at ingest time.

Use the normalized names in `entity_key`.

## invalidate_missing

For single-dataset refresh-in-place specs, `invalidate_missing=True` closes out (`valid_to` set) current rows whose entity is absent from the fresh pull — the correct semantics when disappearance means delisting (e.g. a hospital leaving Care Compare). The default is `False`, consistent with the other collectors, in which case removed rows simply remain current.

The spec **forbids** the flag for multi-dataset families: staging only ever holds one sibling, so invalidation would close out every other sibling's rows.

## DDL generation

`generate_ddl(spec)` samples one datastore row from every dataset in the spec and unions the normalized column sets, so column drift across a family's siblings all gets a home. All source columns are `text` (the APIs serve strings; casting is a downstream concern), plus `_source_dataset`/`_source_modified`, the pipeline columns, a `(entity_key, record_hash)` unique constraint, and a partial index on current rows. Keep `target_table` under ~48 characters so the generated constraint names stay within Postgres's identifier limit.

## Limitations

* File retrieval assumes the first distribution (`index 0`) is a CSV. No other formats or distribution selection yet.
* Datastore type metadata and data dictionaries are ignored; all columns are `text`.
* `generate_ddl` requires datastore-backed datasets (it samples a row). All CMS DKAN datasets qualify.
* Open Payments' `change_type` column participates in the record hash, so a record flipping e.g. `UNCHANGED` → `CHANGED` between publications creates a new SCD2 version even if payment fields are identical. That's arguably signal; if it churns too much history, add it to `hash_exclude_columns`.

## Tests

Behavior tests live in `tests/collectors/dkan/`. The HTTP boundary is faked (`FakeDKANClient`); ingestion runs against a real Postgres so `StagedIngest`'s actual SCD2 semantics are part of what's tested. DB-backed tests use the same `CMS_TEST_PG*` env vars as the CMS collector tests and skip when no database is reachable:

```console
uv run --env-file .env_test pytest platform/tests/collectors/dkan -v
```
