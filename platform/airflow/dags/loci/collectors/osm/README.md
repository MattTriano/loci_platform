# OSM (Overpass) collector

OpenStreetMap data, fetched from the Overpass API. Unlike the catalog-backed sources, OSM has nothing to browse: you declare *what* to fetch with an Overpass query — element types, tag filters, and a spatial extent — and the collector runs it, assembles geometry, promotes selected tags to typed columns, and SCD2-merges into a `raw_data` table. It supports incremental updates via Overpass's `(newer:)` filter.

Reach for this collector when you want features straight from OSM (cafes, bike racks, building footprints, …) rather than from a government portal.

This source diverges from the four-class shape: there's **no Metadata class** (no catalog exists). The pieces are `OverpassAPIQuery` (a declarative query builder), `OSMDatasetSpec`, `OSMClient`, and `OSMCollector`, with geometry assembly in a `geometry` module.

## 1. Explore: prototype an Overpass query

"Finding the dataset" here means building a query that returns what you want. `OverpassAPIQuery` is declarative — element types, tag filters, and a spatial extent:

```python
from loci.collectors.osm.query import OverpassAPIQuery, Regex

cafes = OverpassAPIQuery(
    element_types=["node", "way"],
    tag_filters=[{"amenity": "cafe"}],
)
```

A query with no `bbox`/`area_name` is a reusable *template*; calling `.to_ql()` on it raises. Bind it to a place with `.for_bbox((south, west, north, east))` or `.for_area("Chicago")`:

```python
chicago_cafes = cafes.for_bbox((41.62, -87.97, 42.05, -87.5))
print(chicago_cafes.to_ql())   # inspect the rendered Overpass QL before running it
```

Tag-filter value semantics, per group dict (groups are OR'd, entries within a group are AND'd):

- `None` — key exists with any value (`{"amenity": None}`).
- `str` — exact match (`{"amenity": "cafe"}`).
- `list[str]` — any of these values (`{"shop": ["supermarket", "convenience"]}`).
- `Regex("...")` — raw POSIX-ERE pattern, passed through unescaped; set `case_insensitive=True` for the `,i` modifier (Overpass doesn't support `(?i)`).

Preview what the query actually returns before wiring up a table — `fetch_rows` yields the same row dicts the collector ingests:

```python
from loci.collectors.osm.client import OSMClient

client = OSMClient()
for row in client.fetch_rows(spec):   # needs a spec; or client.fetch(query) for raw JSON
    print(row["osm_type"], row["osm_id"], row["tags"])
    break
```

## 2. Inspect: decide which tags to promote

Every row always carries the fixed columns — `osm_type`, `osm_id`, `osm_version`, `osm_timestamp`, `geom`, `node_ids`, and the full tag dict in a `tags` jsonb column. "Inspecting" is really deciding which tag keys to lift out of `tags` into their own typed columns. Look at the `tags` dict on a few previewed rows and pick the keys you'll query on (`name`, `addr:street`, `cuisine`, …). Everything stays in `tags` regardless, so promotion is purely about query convenience.

## 3. Write the spec

```python
from loci.collectors.osm.spec import OSMDatasetSpec

CHICAGO_CAFES_SPEC = OSMDatasetSpec(
    name="chicago_cafes",
    target_table="chicago_cafes",
    target_schema="raw_data",
    query=chicago_cafes,
    promoted_tags=["name", "addr:street", "cuisine"],
)
```

Field by field:

- `query` — an `OverpassAPIQuery` with a spatial extent set. A bare template (no `bbox`/`area_name`) will raise at collection time, so bind it with `.for_bbox`/`.for_area` first.
- `promoted_tags` — tag keys lifted into typed `text` columns. Keys that aren't valid Postgres identifiers are auto-normalized for the column name (`addr:street` → `addr_street`, `name:en` → `name_en`) while the original key is kept for the OSM lookup. May be empty.
- `entity_key` — defaults to `["osm_type", "osm_id"]`, which is the natural stable key for OSM. Overriding it emits a warning.

### Gotchas

- **Bind the query before collecting.** A template query (no extent) raises on `.to_ql()`.
- **`promoted_tags` collisions raise.** If two keys normalize to the same column name (or a key normalizes to empty / starts with a digit), the spec rejects it in `__post_init__` — rename one manually.
- **All tags are retained.** Promotion never drops data; unpromoted tags live in `tags`. If you mix subtypes in one query (e.g. `amenity` and `shop`), promote each and `COALESCE` at query time.

### Finding the `entity_key`

Nothing to discover here — `["osm_type", "osm_id"]` uniquely identifies an OSM element and is the default. Leave it alone unless you have a specific reason not to (and expect the warning if you do).

## 4. Generate the DDL and create the table

```python
from loci.collectors.osm.collector import OSMCollector

collector = OSMCollector(engine=engine)
collector.print_ddl(spec)
```

The DDL lays down the fixed OSM columns, one `text` column per promoted tag, `ingested_at`, and the SCD2 columns, plus a unique constraint on `(entity_key, record_hash)`, a partial index on the entity key for current rows, and a partial **GIST** index on `geom` for current rows. Paste it into a migration and apply it — `collect` raises if the table doesn't exist yet.

## 5. Collect

```python
collector.collect(spec, force=True)    # full pull
collector.collect(spec, force=False)   # incremental
```

`collect` returns a summary dict (`spec_name`, `mode`, `elements_fetched`, `rows_staged`, `rows_merged`, `rows_invalidated`, …). What `force` does:

- `force=True` → full pull with `invalidate_missing=True`, so elements that have disappeared from OSM are closed out (SCD2 `valid_to` set) — this is how deletions are caught.
- `force=False` → incremental: the collector reads `max(ingested_at)` from the table and passes it as the Overpass `(newer:)` floor, fetching only elements edited since. If the table is empty it transparently falls back to a full pull.

Note the high-water mark is `ingested_at`, not an OSM timestamp — so the incremental floor is "when we last collected," which is what you want for catching edits since the previous run.

## Scheduling

The spec is wrapped in a `DatasetUpdateConfig` in `sources/update_configs.py`, and the OSM taskflow uses `choose_update_mode` to branch full vs. incremental per scheduled run. Because a full pull is what catches deletions, it's worth scheduling a periodic `force=True` run rather than relying on incrementals forever.
