# Bike Index collector

[Bike Index](https://bikeindex.org) is a stolen-bike registry with a public API. There's no catalog of datasets to browse — you define a geographic search (a location and a radius), and the collector pulls matching stolen-bike reports in two phases (a cheap summary search, then per-bike detail enrichment) and SCD2-merges them into a `raw_data` table. It supports incremental updates keyed off each bike's `date_stolen`.

Reach for this collector when you want stolen-bike reports for an area.

This source diverges from the four-class shape: there's **no Metadata class** (no catalog). The pieces are `BikeIndexSearchParams` (a search descriptor, on the client), `BikeIndexDatasetSpec`, `BikeIndexClient`, and `BikeIndexCollector`. The table schema is fixed (hardcoded in `generate_ddl`), so there's no per-dataset column discovery.

## 1. Explore: shape a search and gauge volume

"Finding the dataset" means tuning a search. Start with the count endpoint to see how many reports a location/radius will return before pulling anything:

```python
from loci.collectors.bike_index.client import BikeIndexClient, BikeIndexSearchParams

client = BikeIndexClient()   # access_token optional; anonymous works for read-only search
search = BikeIndexSearchParams(location="Chicago, IL", distance=10, stolenness="proximity")

client.search_count(search)        # {"proximity": N, "stolen": N, "non": N}
```

Then look at the shape of the data — one page of summary results:

```python
client.search(search, page=1)["bikes"]    # list of summary dicts
client.search_all(search)                  # generator paginating every page
```

`stolenness` controls scope: `"proximity"` (stolen near the location), `"stolen"`, `"non"` (recovered/not stolen), or `"all"`. `query` adds a free-text filter (brand, model, color). Adjust `location`/`distance` until `search_count` looks right.

## 2. Inspect: understand search vs. detail

The schema is fixed, so inspecting is about understanding the two tiers of fields rather than discovering columns:

- **Search** results carry summary fields (`id`, `title`, `serial`, `manufacturer_name`, `frame_model`, `date_stolen`, `stolen_coordinates`, …).
- **Detail** (`client.get_bike(id)`) adds the `stolen_record` (precise `latitude`/`longitude`, theft/locking descriptions, police report fields), plus components and photos.

```python
client.get_bike(some_id)    # full detail dict for one bike
```

The collector flattens both into one wide row; detail-only columns are `NULL` until the detail phase fills them in. You don't choose columns — `generate_ddl` already encodes the full set.

## 3. Write the spec

```python
from loci.collectors.bike_index.spec import BikeIndexDatasetSpec

CHICAGO_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="chicago_bikeindex_bike_thefts",
    target_table="chicago_bikeindex_bike_thefts",
    location="Chicago, IL",
    distance=10,
    stolenness="proximity",
    entity_key=["id"],
)
```

Field by field:

- `location` — city name, zip, address, or `"lat,lon"`.
- `distance` — radius in miles from `location`.
- `stolenness` — `"proximity"`, `"stolen"`, `"non"`, or `"all"`; validated in `__post_init__`.
- `query` — optional free-text filter.
- `per_page` — page size (max 100).
- `entity_key` — defaults to `["id"]`, the Bike Index bike id.

`spec.to_search_params()` is what the collector feeds into `BikeIndexSearchParams`, so the spec and your exploration search stay in sync.

### Gotchas

- **`stolenness` is validated.** Anything outside the four allowed values raises at spec construction.
- **Two-phase, one table.** Search and detail both merge into the same table; a row appears first with detail columns `NULL`, then gets enriched. That's expected, not a bug.

### Finding the `entity_key`

Nothing to discover — the Bike Index `id` is the natural unique key and is the default (`["id"]`). Leave it as is.

## 4. Generate the DDL and create the table

```python
from loci.collectors.bike_index.collector import BikeIndexCollector

collector = BikeIndexCollector(client=client, engine=engine)
print(collector.generate_ddl(spec))
```

The DDL is a fixed wide table (core search fields, `stolen_record` fields, detail-only scalars, `components`/`public_images` as jsonb) plus `ingested_at` and the SCD2 columns, with a unique constraint on `(entity_key, record_hash)` and a partial current-rows index. Paste it into a migration and apply it.

## 5. Collect

```python
collector.collect(spec, force=True)    # full refresh
collector.collect(spec, force=False)   # incremental
```

`collect` runs both phases and returns a nested summary: `{spec_name, search, detail}`, where each phase reports rows staged/merged (and detail reports any per-bike fetch errors). What `force` does:

- `force=False` → incremental: the high-water mark is `max(date_stolen)` among current rows. The search phase skips bikes at or before it, and the detail phase only fetches bikes past it (ordered by `(date_stolen, id)` for resumable runs).
- `force=True` → full refresh: the high-water mark is reset to the epoch, so every matching bike is searched and its detail re-fetched.

You can also run the phases independently — `collect_search(spec)` and `collect_detail(spec)` — which is handy if a detail run was interrupted (it resumes from the table without re-searching).

## Scheduling

The spec is wrapped in a `DatasetUpdateConfig` in `sources/update_configs.py`, and the Bike Index taskflow uses `choose_update_mode` to branch full vs. incremental per scheduled run.
