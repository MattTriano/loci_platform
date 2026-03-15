# Loci Platform

Loci is a data platform for collecting, transforming, and serving geospatial data. It doubles as a sandbox for building out data engineering ideas — a place to develop patterns for ingestion, transformation, and deployment, then apply them to real datasets and applications. Through experimenting with and evaluating different architectural data platform components/designs, this platform aims to support the search for a *critical locus* (a set of optimal constraint-satisfying points) for a data engineering situation.

The platform currently runs on a single machine using Podman Compose, which keeps costs near zero (the only expenses are an annual domain registration and a few pennies on S3). The architecture is designed to require very little RAM so that it's cheap to deploy on modest hardware or inexpensive cloud instances when the time comes. OpenTofu modules already manage AWS resources for deployed applications and support dev, staging, and prod environments via per-environment state backends.

## Data sources

Data is collected into a PostGIS data warehouse from five external sources:

- **Socrata** — Chicago and Cook County open data portals. Datasets include traffic crashes (three related tables), crimes, arrests, bike racks, building permits, food inspections, business licenses, CTA ridership, and more.
- **Census API** — American Community Survey tables at various geography levels (tract, block group, etc.).
- **TIGER/Line** — Census boundary and geographic feature shapefiles (tracts, counties, roads, railroads, water features) across multiple vintages.
- **OpenStreetMap** — PBF extracts parsed element-by-element using pyosmium, with geometry converted to WKB via shapely. Supports nodes (points), ways (lines/polygons via a node location cache), and relations (member lists). Used for bike infrastructure, points of interest, parks, and other features.
- **Bike Index** — Stolen bike reports for the Chicago area.

## Collector architecture

Each data source has a collector built on a shared set of abstractions:

**Specs** define what to collect. Each source has a dataclass-based spec type (`SocrataDatasetSpec`, `CensusDatasetSpec`, `OsmDatasetSpec`, `TigerDatasetSpec`, `BikeIndexDatasetSpec`) that inherits from a common `DatasetSpec` base. Specs carry the target table, target schema, entity key (for SCD2 tracking), and source-specific configuration like dataset IDs, API endpoints, geography levels, or tag filters.

**Clients** handle communication with external APIs. `SocrataClient` manages SODA API queries with pagination and retry logic. `CensusClient` handles variable resolution and API calls. `BikeIndexClient` manages search-based collection. Clients are separate from collectors so they can be used independently (e.g., in notebooks for exploration).

**Metadata** classes provide schema discovery. `SocrataTableMetadata` fetches column definitions from the Socrata catalog API. `CensusMetadata` browses available variables, groups, and geographies. `TigerMetadata` explores the Census TIGER file server to discover available datasets across vintages. Each metadata class exposes a `generate_ddl()` method that produces a `CREATE TABLE` statement for the target table — including column types, comments, SCD2 tracking columns, and constraints — directly from the source's schema information.

**Collectors** orchestrate the end-to-end flow: fetch data (via a client or file download), normalize it, and write it through `StagedIngest`. All collectors inherit from `BaseCollector`, which provides a shared HTTP session, logging, a `download_to_tempfile` utility, and integration with the optional `IngestionTracker`. Collectors return a `CollectionSummary` dataclass that standardizes reporting across sources.

**Parsers** handle file formats. Streaming parsers for CSV, GeoJSON, shapefiles, and PBF files all yield batches of row dicts without loading entire files into memory, keeping resource usage proportional to batch size rather than file size.

## Database and ingestion

`PostgresEngine` is the central interface to the PostGIS warehouse. Its `StagedIngest` context manager handles all writes: data lands in a temporary staging table first, then gets merged into the target in a single transaction. This keeps ingestion atomic and allows partial-failure recovery (if a collection fails mid-stream, whatever was already staged still gets merged).

For tables with an entity key, `StagedIngest` uses SCD2 (slowly changing dimension type 2) merge logic. It computes a record hash (MD5 of all non-metadata columns), compares against the current version in the target (`valid_to IS NULL`), closes out superseded rows, and inserts new versions. Tables without an entity key use a simpler `INSERT ... ON CONFLICT` path. The choice is driven by the spec's `entity_key` field — if it's set, SCD2 is used automatically.

`PostgresEngine` also handles geometry detection and casting (by type OID, not column name), batched COPY-based writes, server-side cursors for large reads, and retry logic for transient connection failures.

Schema migrations are managed by Flyway, run as a one-off container (`podman compose run --rm flyway-postgis`).

## Transformation

dbt models are organized into staging and marts layers. Staging models deduplicate SCD2 records (filtering to `valid_to IS NULL`), normalize column names (via a `standardize_column_name` macro or explicit mappings), and handle data quality issues. Mart models join across sources to produce analysis-ready datasets.

A `DbtModelGenerator` class automates creation of staging models. Given a source name, table name, and column list, it produces a `.sql` file following project conventions (SCD2 CTE if needed, column aliasing, proper file placement) and updates `sources.yml` idempotently. Source-specific subclasses like `CensusDbtPipelineBuilder` layer on additional logic — for Census data, this includes automated variable-name compression that turns codes like `B01001_001E` into readable column names within Postgres's 63-character limit, using a two-pass algorithm (group concept → prefix, label tree → distinguishing leaf tokens).

### Address extraction and geocoding cache

Multiple source datasets contain address data, and many downstream models need coordinates for spatial analysis. Rather than geocoding redundantly across models and pipeline runs, the platform uses a centralized geocoding cache.

A dbt incremental model (`geocoded_address_cache`) unions addresses from all source datasets, deduplicates by normalized address hash, and preserves source-provided lat/lng where available. An Airflow task then geocodes rows that are still missing coordinates using PostGIS's built-in TIGER geocoder, bounded to the Chicago metro area (in SRID 4269 to match TIGER's NAD83 datum). Results are stored with quality metadata — the TIGER rating score, the normalized input string, the TIGER data version, and the rating threshold in effect — so that geocoding quality can be audited and rows re-geocoded if the threshold changes. Downstream mart models join against the cache for coordinates without triggering any geocoding themselves.

The Airflow task flow for this is: build the incremental cache model (extracting new addresses) → geocode pending rows via TIGER → build downstream mart models that join on the cache.

## Orchestration

Airflow 3 DAGs coordinate collection, transformation, export, and deployment. Tasks use the `@task` and `@task_group` decorator API (not the legacy Operator style). `DatasetUpdateConfig` instances pair a spec with scheduling configuration — a cron for incremental updates and a separate cron for periodic full refreshes. A `choose_update_mode` task inspects the schedule and ingestion log at runtime to decide which path to take.

dbt is invoked via subprocess from Airflow tasks, using `--select` with intersection syntax to run specific segments of the DAG (e.g., `"model_a+,+model_n"` to run everything between two models).

An `IngestionTracker` logs every collection run (source, dataset, target table, row counts, duration, mode, errors) into a `meta.ingest_log` table for observability. A `ScheduleVisualizer` class can render Gantt-style charts of run history in Jupyter notebooks.

## Infrastructure

OpenTofu modules manage AWS resources:

- **State management** (`modules/state/`) — per-environment S3 bucket + DynamoDB table for OpenTofu state locking, supporting dev, staging, and prod environments.
- **Bike map** (`modules/bike-map/`) — S3 bucket (private, CloudFront-only access via OAC), ACM certificate with DNS validation (in us-east-1 for CloudFront), CloudFront distribution, Route 53 alias record, and an IAM deploy user scoped to S3 sync and cache invalidation.

The infrastructure code supports the full environment progression. The platform currently runs against the dev environment.

## Applications

### Chicago Bike Map

`apps/bike-map/` — a static web application showing bike crashes, bike thefts, and bike parking across Chicago. Built as a single HTML file with MapLibre GL JS and OpenFreeMap "liberty" tiles. Layers support clustering at coarse zoom levels and individual data points at fine zoom, with click-through detail popups and layer toggles. Currently under active development.

The end-to-end pipeline: Airflow collects from Socrata and Bike Index → dbt builds staging and mart models → a `GeoJsonExporter` queries mart tables and writes compact GeoJSON files → an S3 sync task deploys to CloudFront. The site is served at [bike-map.dev.missinglastmile.net](https://bike-map.dev.missinglastmile.net).

The platform is designed so that additional applications can follow the same pattern: collect → transform → export → deploy.

## Project structure

```
loci_platform/
├── apps/
│   └── bike-map/            # Static web app (HTML + GeoJSON)
├── infra/
│   ├── bootstrap/           # Per-environment state backend setup
│   │   ├── dev/
│   │   ├── staging/
│   │   └── prod/
│   └── modules/
│       ├── state/           # S3 + DynamoDB for OpenTofu state
│       └── bike-map/        # S3 + CloudFront + ACM + Route 53 + IAM
├── platform/
│   ├── airflow/
│   │   └── dags/
│   │       ├── dag_files/   # DAG definitions
│   │       └── loci/        # Shared library
│   │           ├── collectors/
│   │           │   ├── socrata/    # client, metadata, collector, spec
│   │           │   ├── census/
│   │           │   ├── osm/
│   │           │   ├── tiger/
│   │           │   └── bike_index/
│   │           ├── db/          # PostgresEngine, StagedIngest
│   │           ├── exports/     # GeoJSON export tooling
│   │           ├── parsers/     # Streaming parsers (CSV, GeoJSON, shapefile, PBF)
│   │           ├── tasks/       # Airflow task definitions
│   │           ├── tracking/    # Ingestion run tracking
│   │           └── transform/   # Geocoding and other Python transforms
│   ├── dbt/                 # dbt project (models, macros, sources)
│   ├── migrations/          # Flyway SQL migrations
│   └── docker-compose.yaml  # Full platform stack
└── notebooks/               # Jupyter notebooks for exploration
```

## Running locally

The platform runs via Podman Compose. The `docker-compose.yaml` in `platform/` defines the stack: PostGIS (data warehouse), Airflow (apiserver, scheduler, dag-processor, worker, triggerer), Postgres (Airflow metadata), and Redis (Celery broker).

```
cd platform
podman compose build
podman compose up -d
podman compose run --rm flyway-postgis   # run migrations
```

## Roadmap

Near-term work in progress or planned:

- **More mart models** — bike infrastructure quality layers (from OSM cycleway data), points of interest for cyclists (restaurants, cafes, bike shops), park polygons, and crash severity heatmaps with grid-based pre-aggregation.
- **Safety-weighted bike routing** — integrate crash density and theft hotspot data into a routing cost function (likely via pgRouting on the OSM bike network) to suggest routes that balance distance against safety. Would be served as a feature of the bike map.
- **Remove MySQL** — the current docker-compose includes a MySQL service that isn't used by anything in the platform. Removing it and its related configuration simplifies the stack.
- **Additional analyses and applications** — the collected data (crimes, building permits, food inspections, Census demographics, property assessments, transit ridership, etc.) supports a range of applications beyond cycling. Future work may include neighborhood-level dashboards, transit accessibility analysis, or property/zoning tools — following the same collect → transform → export → deploy pattern.
- **Automated dbt model generation** — extend `DbtPipelineBuilder` subclasses for Socrata, TIGER, and OSM sources so that adding a new dataset to the warehouse is a one-liner that generates the staging model, updates `sources.yml`, and wires up the correct SCD2/column-aliasing conventions.
