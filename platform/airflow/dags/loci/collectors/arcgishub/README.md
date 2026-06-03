# ArcGIS Hub collector

ArcGIS Hub powers open-data sites backed by Esri feature services (many city and police portals). A Hub catalog *item* wraps a feature service, and a service can contain one or more *layers* — each a queryable table of features (attributes plus geometry). This collector queries the layer(s), flattens each feature into attributes + geometry, and SCD2-merges into a `raw_data` table. It supports incremental updates when the layer has a date field to key off.

Reach for this collector when the portal is an ArcGIS Hub / Esri site — you'll see `/api/search/v1/...` catalog endpoints and `FeatureServer` layer URLs.

The four classes: `ArcGISHubMetadata` (browse the catalog), `ArcGISHubDatasetSpec`, `ArcGISHubClient` (HTTP/JSON + pagination), `ArcGISHubCollector`.

## 1. Find the dataset

`ArcGISHubMetadata` browses the catalog (the Hub OGC API - Records endpoint), paginating for you:

```python
from loci.collectors.arcgishub.client import ArcGISHubClient
from loci.collectors.arcgishub.metadata import ArcGISHubMetadata

client = ArcGISHubClient("https://data.tps.ca")
meta = ArcGISHubMetadata(client)

for item in meta.search(q="arrests", limit=50):
    print(item["id"], item["properties"]["title"])
```

Each item's `id` is the Hub item id you'll put in the spec. `meta.get_dataset(item_id)` returns the full item dict, including the underlying feature service URL in its properties.

## 2. Inspect the dataset / layers

A single item can wrap a multi-layer service, so the cleanest way to see what you'll actually get is to draft a spec and let the collector resolve the layer(s) and show the column schema:

```python
print(collector.generate_ddl(spec))   # resolves the layer(s) and prints the columns + types
```

If you want the raw layer fields first, the feature service URL lives on the item, and an Esri layer reports its fields directly:

```python
item = meta.get_dataset(item_id)
service_url = item["properties"]["url"]              # the FeatureServer URL
layer0 = client.get_json(f"{service_url}/0", params={"f": "json"})
for f in layer0["fields"]:
    print(f["name"], f["type"])
```

## 3. Write the spec

```python
from loci.collectors.arcgishub.spec import ArcGISHubDatasetSpec

TPS_ARRESTS_SPEC = ArcGISHubDatasetSpec(
    name="tps_arrests",
    base_url="https://data.tps.ca",
    item_id="4702e79fd2404f7d93dd9866f45d7ec2",
    target_table="tps_arrests",
    entity_key=["Event_Unique_Id"],
    layer_index=0,
    incremental_column="last_edited_date",
)
```

Field by field:

- `base_url` / `item_id` — the Hub site and the catalog item id.
- `layer_index` — which layer(s) to pull: an `int` (default `0`), a `list[int]` for specific layers, or `"all"` to auto-discover and collect every layer in the service.
- `where` — a server-side SQL filter applied to every request (default `"1=1"`).
- `incremental_column` — a date/time field (e.g. `"last_edited_date"`) used for incrementals; `None` means a full refresh on every run.
- `out_fields` — a subset of fields to request; `None` means all.
- `layer_column` — if set, adds a column holding each row's layer name, useful when collecting multiple layers into one table.
- `min_field_overlap` — when collecting multiple layers, the minimum fraction of fields they must share (default `0.8`); the collector raises if overlap is lower.

### Gotchas

- **Do NOT use `OBJECTID` as the `entity_key`.** OBJECTID is not stable across service refreshes or republishes, so it makes a broken SCD2 key — a republish would version every row. Use a domain identifier (a case/event id). The spec docstring calls this out explicitly.
- **Multi-layer collections require field overlap.** Layers combined into one table must share at least `min_field_overlap` of their fields, or collection raises.
- **Esri returns errors as HTTP 200.** A feature service often replies `200` with an `{"error": ...}` body; the client accounts for this, which is why a "successful" request can still carry an error.

### Finding the `entity_key`

ArcGIS query layers support server-side aggregation, so you can check a candidate key's uniqueness without ingesting: query the layer with `groupByFieldsForStatistics` set to your candidate column(s) and an `outStatistics` count, then look for any group with a count above 1 (some services also honor a `having` parameter). This is the same "verify against the source" idea as the Socrata `find_duplicate_keys` check, just expressed in Esri's query grammar — and it's the constructive flip side of the OBJECTID warning: OBJECTID is unique but unstable, so confirm a *domain* column is unique and key on that. (Not wrapped in a helper yet.)

## 4. Generate the DDL and create the table

```python
from loci.collectors.arcgishub.collector import ArcGISHubCollector

collector = ArcGISHubCollector(client=client, engine=engine)
print(collector.generate_ddl(spec))
```

This resolves the layer(s), maps Esri field types to Postgres, adds a PostGIS geometry column, the optional `layer_column`, an `ingested_at` column, and the SCD2 columns when `entity_key` is set. Paste it into a migration and apply it.

## 5. Collect

```python
collector.collect(spec, force=True)    # full refresh
collector.collect(spec, force=False)   # incremental (requires incremental_column)
```

`collect` returns a summary dict (including `rows_staged` and `rows_merged`). With `force=False`, it resumes from the table's high-water mark in `incremental_column` (ArcGIS date fields are epoch milliseconds). If the spec has no `incremental_column`, every run is a full refresh regardless of `force`.

## Scheduling

The spec is wrapped in a `DatasetUpdateConfig` in `sources/update_configs.py`, and the ArcGIS taskflow uses `choose_update_mode` to branch full vs. incremental for each scheduled run.
