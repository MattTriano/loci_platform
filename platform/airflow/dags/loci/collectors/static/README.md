# StaticFile collector

Collection tooling for datasets published as **static files at fixed URLs** — no catalog API, no query language, no pagination. The publisher overwrites a file when the data changes; everyone who requests the URL gets the same bytes. First concrete source: the [AHRQ Compendium of U.S. Health Systems](https://www.ahrq.gov/chsp/data-resources/compendium.html).

Reach for this collector when the source is "a download link on a webpage": annually published reference files, crosswalks, fee schedules, and the like. It also handles files you had to download by hand (e.g. from behind a bot-defense wall) via `file://` URLs — same parse and ingest path, no HTTP.

This source diverges from the four-class shape: there is **no Metadata class**. With no catalog to explore, the spec's manifest of files *is* the metadata. The pieces are `FileRef` + `StaticFileDatasetSpec` (`spec.py`), `StaticFileClient` (`client.py`), and `StaticFileCollector` (`collector.py`). Concrete manifests live in source modules like `ahrq_compendium.py`.

## 1. Find the files

Discovery is manual: locate the stable download URLs on the publisher's pages and record them in the manifest. Prefer CSV over XLSX when both are offered (smaller, no `openpyxl` dependency). Resist scraping the publisher's pages to discover files automatically — hand-edited URLs and roughly-annual editions mean a manifest entry per year is cheaper than maintaining a scraper.

Two publisher quirks to check up front:

- **Bot defenses.** Some hosts (ahrq.gov among them) sit behind AWS WAF. The escalation path, cheapest first: pass a browser `user_agent` to the client; preload a browser-minted token via `StaticFileClient(cookies={"aws-waf-token": ...})` (the session UA must match the browser that minted it); or download by hand and use `file://` URLs. Header spoofing beyond this doesn't help, and the client deliberately contains no WAF logic.
- **Revisions in place.** Publishers sometimes silently overwrite a file (AHRQ marks these with a `-rev` suffix, but not always). See `force=True` below.

## 2. Inspect the files

There's no metadata class, but you don't need the warehouse to look at a file — `iter_rows` yields the same row dicts the collector ingests:

```python
from itertools import islice
from loci.collectors.static.client import StaticFileClient
from loci.collectors.static.spec import FileRef

client = StaticFileClient()
ref = FileRef(url="https://.../chsp-hospital-linkage-2023.csv", vintage="2023", encoding="cp1252")
for row in islice(client.iter_rows(ref), 3):
    print(row)
```

Column names arrive sanitized (lowercase, runs of non-alphanumerics collapsed to underscores) and every value is a stripped string. If the file's encoding is wrong you'll know immediately: the client raises rather than silently mangling bytes (a cp1252 en dash under the default `utf-8-sig` is a `UnicodeDecodeError`, not a corrupted name).

## 3. Write the spec

```python
from loci.collectors.static.spec import FileRef, StaticFileDatasetSpec

AHRQ_HOSPITAL_LINKAGE = StaticFileDatasetSpec(
    name="ahrq_chsp_hospital_linkage",
    target_table="ahrq_chsp_hospital_linkage",
    entity_key=["ccn", "vintage"],
    files=[
        FileRef(url=".../chsp-hospital-linkage-2023.csv", vintage="2023", encoding="cp1252"),
    ],
)
```

Field by field:

- `files` — the manifest. Each `FileRef` carries one URL, its `vintage` label, and its parse options: `file_format` (`"csv"` or `"xlsx"`), `encoding` (default `utf-8-sig`; Windows-pipeline publishers are usually `cp1252`), `delimiter`, `sheet` (XLSX name or index), and `skip_rows` for files with preamble lines above the header. Adding a new edition later is one manifest line, not code. Vintages must be distinct; all files in one manifest land in one table and must share a column layout (if a publisher renames columns between editions, the divergent edition needs its own spec/table, reconciled downstream).
- `vintage` — stamped onto every row as the `vintage` column; this is what the freshness check keys on.
- `entity_key` — columns that uniquely identify a record for SCD2 versioning, in *sanitized* form. `None` means append-only (no SCD2 columns, constraint, or index in the DDL). See below.
- `target_schema` — defaults to `raw_data`.

### Finding the `entity_key`

For edition-snapshot sources, **the key should include `vintage`**: the same hospital in the 2022 and 2023 editions is two observations, not an update to one entity. With `vintage` in the key, SCD2 only versions in-place revisions *within* an edition — which is what you want.

For the domain part of the key, check the publisher's data dictionary / technical documentation first (AHRQ ships a techdoc PDF per file). If documentation is thin, verify candidates empirically — there's no server-side `GROUP BY` here like Socrata's `find_duplicate_keys`, but the file is already in memory:

```python
from collections import Counter

candidate = ("ccn",)
counts = Counter(tuple(row[c] for c in candidate) for row in client.iter_rows(ref))
dupes = {k: n for k, n in counts.items() if n > 1}
dupes  # {} -> unique within the file: valid with vintage appended
```

Watch for the usual key-poisoners: empty strings (all values are text, so nulls arrive as `""` and will group together — a candidate with many empties is a bad key) and identifiers that only look unique because of leading zeros, which this pipeline preserves precisely so keys like CCN stay intact.

### Gotchas

- **Wrong encoding fails loudly by design.** Declare the right one per `FileRef` rather than adding `errors="replace"` anywhere — a crash beats silently corrupted names. When one file from a publisher is cp1252, its siblings usually are too.
- **Everything is text.** `0895` stays `0895`; casting is a downstream concern. Never let anything numeric-parse identifier columns.
- **XLSX needs `openpyxl`** (imported lazily — CSV-only deployments don't need it). Excel has no date-only type, so date cells land as midnight ISO timestamps (`2023-01-02T00:00:00`); integral floats lose Excel's trailing `.0`.
- **A data URL returning HTML raises.** That's the guard catching a WAF challenge or error page before it gets parsed as data — see the bot-defense notes above.

## 4. Generate the DDL and create the table

```python
from loci.collectors.static.collector import StaticFileCollector

collector = StaticFileCollector(engine=engine)
collector.print_ddl(spec)
```

The DDL derives columns from the first file's header (so **each `generate_ddl` call costs one download** — fine at this source's cadence, just don't loop it against a touchy host): one `text` column per source column, `vintage text not null`, `ingested_at`, and — when `entity_key` is set — the SCD2 columns, the `(entity_key, record_hash)` unique constraint, and the partial current-rows index. Paste it into a migration and apply it; `collect` raises if the table doesn't exist.

## 5. Collect

```python
collector.collect(spec, force=False)   # load only vintages not yet in the target
collector.collect(spec, force=True)    # re-download and re-ingest every file
```

`collect` returns a summary dict (`dataset`, `files_processed`, `files_skipped`, `rows_staged`, `rows_merged`). What `force` means:

- `force=False` — skips any file whose `vintage` already has rows in the target. New-edition detection *is* the incremental story for annually published files: grow the manifest, rerun, and only the new edition downloads.
- `force=True` — recollects everything. Use it when a publisher revises a file in place; ingestion is `StagedIngest` in SCD2 mode, so recollection is always safe — unchanged rows dedupe away and changed rows get a new version with the old one closed out.

There is no `invalidate_missing` and no conditional-GET freshness checking — at one file per edition per year, vintage presence is the right amount of machinery. If a static source ever revises in place frequently, ETag/Last-Modified conditional GETs are the documented follow-up.

## Tests

Behavior tests live in `tests/collectors/static/`. The HTTP boundary is faked (`FakeStaticFileClient` overrides only `download()`, so parsing, sanitization, and encoding handling run the real code); ingestion runs against a real Postgres so `StagedIngest`'s actual SCD2 semantics are part of what's tested. DB-backed tests use the same `CMS_TEST_PG*` env vars as the other collector suites and skip when no database is reachable:

```console
uv run --env-file .env_test pytest platform/tests/collectors/static -v
```

## Scheduling

The spec is wrapped in a `DatasetUpdateConfig` in `sources/update_configs.py`. A frequent `force=False` run is nearly free (every vintage skips until the manifest grows), so the practical schedule is: run incrementals on the normal cadence, add manifest entries when the publisher ships a new edition, and reserve `force=True` for known in-place revisions.
