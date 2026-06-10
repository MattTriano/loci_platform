# CMS Collector

Collection tooling for [data.cms.gov](https://data.cms.gov), the main CMS data portal — home of the Medicare fee-for-service payment datasets (inpatient payments by DRG, physician payments by HCPCS code, Part D prescribers, and so on).

This source covers **data.cms.gov only**. CMS runs two other portals on a different platform (DKAN): the Provider Data Catalog (`data.cms.gov/provider-data`, Care Compare data) and Open Payments (`openpaymentsdata.cms.gov`). Those are served by the `dkan` collector — see `loci/collectors/dkan/README.md`.

## The publication model

data.cms.gov publishes a Project Open Data catalog at [`data.json`](https://data.cms.gov/data.json). Each dataset's `distribution` array contains **every published version** of the data in every available format: entries with `format: "API"` point at a versioned JSON API endpoint (each version has its own UUID, paged via `?size=N&offset=M`, max 5000 rows/page, with a `/data/stats` row-count endpoint), and entries with `mediaType: "text/csv"` are direct CSV downloads.

Versions correspond to temporal coverage — typically calendar years, called **vintages** here — and each carries its own modified date. Vintages are *not* immutable: CMS occasionally re-releases one with corrections, signaled by the modified date advancing.

## Classes

Standard four-class collector interface:

* `CMSClient` (`client.py`) — thin HTTP client: cached `data.json` catalog fetch, `/stats` row counts, page iteration over the versioned JSON API, streaming CSV download.
* `CMSMetadata` (`metadata.py`) — catalog exploration: title search, dataset lookup, `versions()` (groups distributions by temporal coverage into `CMSDatasetVersion` records with vintage label, modified date, API UUID, and CSV URL), column sampling, and a `describe()` summary for notebook use.
* `CMSSpec` (`spec.py`) — declares what to collect and where it goes. Subclass of `DatasetSpec`.
* `CMSCollector` (`collector.py`) — orchestrates collection: `collect(spec, force)` and `generate_ddl(spec)`.

Concrete spec instances live in `instances.py`.

## Quick start

```python
from loci.collectors.cms.client import CMSClient
from loci.collectors.cms.metadata import CMSMetadata
from loci.collectors.cms.collector import CMSCollector
from loci.collectors.cms.spec import CMSSpec

# Explore
meta = CMSMetadata(CMSClient())
meta.titles("inpatient hospitals")
meta.describe("Medicare Inpatient Hospitals - by Provider and Service", stats=True)

# Specify
spec = CMSSpec(
    name="medicare_inpatient_by_provider_and_service",
    dataset_title="Medicare Inpatient Hospitals - by Provider and Service",
    target_table="medicare_inpatient_by_provider_and_service",
    entity_key=["rndrng_prvdr_ccn", "drg_cd", "vintage"],
    retrieval="api",
)

# Create the table (run the DDL via a migration), then collect
collector = CMSCollector(engine=engine)
collector.print_ddl(spec)
summary = collector.collect(spec)            # incremental: skips fresh vintages
summary = collector.collect(spec, force=True)  # recollect everything
```

## Update semantics

The unit of work is one published **vintage**. `collect(spec, force=False)` collects only vintages that are missing from the target, incomplete, or re-released since last ingest; `force=True` recollects everything. All ingestion is `StagedIngest` in SCD2 mode, so recollection is always safe: unchanged rows dedupe away.

Every row is stamped with `vintage` (a data column — it must be part of the entity key, see below) and the hash-excluded `_source_modified` (the distribution's modified date at collection time). A vintage is considered fresh when both hold:

1. current target rows for that vintage `>=` the source's `/stats` row count (`>=`, not `==`, because rows removed by a CMS correction remain current in the target), and
2. the stored max `_source_modified` `>=` the catalog's modified date.

After each successful ingest, `_source_modified` is advanced on the vintage's current rows. Without this, a re-release whose rows mostly dedupe away would leave stale stamps behind and the vintage would be re-downloaded on every run.

The target table itself is the bookkeeping — there's no separate state table. Condition 1 is what guarantees a crashed or partial ingest is detected and healed on the next run.

## entity_key must include "vintage"

Each vintage is a distinct slice of the entity space: the same provider × DRG appears in every year. Without `vintage` in the entity key, rows from different years would collide as "changed" versions of one entity and silently corrupt SCD2 history. The spec enforces this at construction time.

## Retrieval modes

* `retrieval="api"` — pages rows out of the versioned JSON API at up to 5000 rows/page. Fine up to a few hundred thousand rows per vintage (the inpatient dataset is ~150–210k rows/vintage ≈ 40 pages).
* `retrieval="csv"` — downloads the vintage's CSV distribution to a tempfile and stream-parses it. Use for the multimillion-row datasets (Physician & Other Practitioners is ~9–10M rows/vintage, which would be ~2,000 API requests).

The two modes are hash-equivalent: `parse_csv` turns empty strings into `None` while the API serves empty strings, but `StagedIngest`'s `coalesce(col::text, '')` hashing makes `NULL` and `''` identical, so switching a spec's retrieval mode never churns SCD2 versions.

## Column normalization

Source column names are lowercased, spaces become underscores, and any BOM is stripped, so columns are queryable without double quotes (`rndrng_prvdr_ccn`, not `"Rndrng_Prvdr_CCN"`) and the API and CSV paths land identically. Use the normalized names in `entity_key`.

(Note this rule is simpler than the DKAN collector's, which must reproduce DKAN's own punctuation-dropping normalization; data.cms.gov column names contain no punctuation beyond underscores, so nothing fancier is needed here.)

## DDL generation

`generate_ddl(spec)` samples one row from **every** in-scope vintage and unions the column sets (newest-first ordering), because CMS has renamed columns across years on some datasets and every column needs a home. All source columns are `text` (the API serves strings; casting is a downstream concern), plus `vintage` (not null), `_source_modified`, the pipeline columns, a `(entity_key, record_hash)` unique constraint, and a partial index on current rows. Keep `target_table` under ~48 characters so the generated constraint names stay within Postgres's 63-character identifier limit.

Schema drift at collection time is warn-and-filter: source columns missing from the target are dropped with a warning, not an error.

## Limitations

* Rows removed by a CMS correction remain current in the target. `StagedIngest`'s `invalidate_missing` can't be used here — staging only ever holds one vintage, so it would close out every other vintage in the table. If this starts to matter, the fix is a vintage-scoped invalidation pass.
* A re-release that changes nothing but the modified date triggers exactly one wasted recollection (it merges zero rows), after which the advanced `_source_modified` resettles the vintage.
* `generate_ddl` requires API-accessible versions (it samples rows).

## Shipped specs

`instances.py` provides:

* `MEDICARE_INPATIENT_BY_PROVIDER_AND_SERVICE` — hospital payments by CCN × DRG × year, ~150–210k rows/vintage across 2013–2024 (~2.1M rows total), API retrieval. First validated load: 12 vintages, 2,131,132 rows, second run skipped all 12.
* `MEDICARE_PHYSICIANS_BY_PROVIDER_AND_SERVICE` — clinician payments by NPI × HCPCS × place of service × year, ~9–10M rows/vintage, CSV retrieval. Verify the entity-key grain against one vintage before the first big load (see the note in the file).

## Tests

Behavior tests live in `tests/collectors/cms/`. The HTTP boundary is faked (`FakeCMSClient`); ingestion runs against a real Postgres so `StagedIngest`'s actual SCD2 semantics are part of what's tested. DB-backed tests use the `CMS_TEST_PG*` env vars and skip when no database is reachable:

```console
uv run --env-file .env_test pytest platform/tests/collectors/cms -v
```
