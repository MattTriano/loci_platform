# Platform

## Setup

Start off by creating a `.env` file via this command (after `cd`ing into this directorys)

```console
echo "AIRFLOW_UID=$(id -u)" > .env
```

Then, run this command to build the images.

```console
podman compose build
```

and if the images build successfully, you can start the system up via

```console
podman compose up
```

### API Keys / App Tokens

#### Census API key

To use the Census data collection tooling, you will need a Census API key (which you can get for free [here](https://api.census.gov/data/key_signup.html)), and you'll need to store it in an environment variable named `CENSUS_API_KEY`. You can set it in the `.env` file created above.

#### Socrata App Token

Follow the instructions [here](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys) to get an app token, then store it in an environment variable named `SOCRATA_APP_TOKEN`. You can set it in the `.env` file created above.

## Usage

### WARNING

For ease of use, I've just left the default passwords and configs in place (THIS IS OBVIOUSLY NOT PRODUCTION-READY). Before one could consider using this with any non-public data, they should do all of the following:
* Refactor out all public credentials (into at least a `.env` file) out of the `docker-compose.yml`.
* Run `podman compose down -v` to purge any created volumes.
* Change any used credentials.

### Resources

Assuming you're running this locally, you can access the Airflow Webserver at

* [http://localhost:8080](http://localhost:8080)
* Username: airflow
* Password: airflow

The Postgres and MySQL databases are running on the standard ports. The Postgres database is also the metadata database for Airflow.
* Postgres DB
    * Username: airflow
    * Password: airflow
    * DB name:  airflow
    * Port:     5432

* MySQL DB:
    * Username: loci
    * Password: loci
    * DB name:  loci
    * Port:     3306

As stated above, THIS SYSTEM IS NOT FOR PRODUCTION USE and probably shouldn't be used with any sensitive data.

# Project Organization

## podman/

This directory contains files needed to build the `podman` (ie `docker`) images used by this system.

The Airflow image (defined in `podman/orchestration/`) is the workhorse of this project; the `airflow-scheduler` service/container executes DAGs, so all required packages need to be installed in the Airflow image. This project uses a `slim` Airflow base image to avoid unnecessary dependency conflicts.

## airflow/dags/

This directory contains two important directories:
* `loci/`, which contains all of the python code that implements the majority of this project's functionality.
* `dag_files/`, which contains the DAG files that use that functionality to orchestrate data collection, ingestion, transformation, and subsequent operations.

### airflow/dags/loci/

#### airflow/dags/loci/sources/

This directory contains files that define `DatasetSpec`ifications and `DatasetUpdateConfig`urations.

`DatasetSpec`s define the content of a dataset, where that dataset should live in the data warehouse, and what set of columns defines a distinct entity (if relevant). Each data source (e.g. Socrata, Census, OSM) must define a `DatasetSpec` class that implements this interface, adding whatever params are needed to define a "dataset" (i.e., a set of columns where each row has a consistent granularity and scope).

`<source>DatasetSpec` instances should be added to the `sources/dataset_spec.py` file.

```python
class DatasetSpec(ABC):
    """ Common interface for dataset specifications. """
    name: str
    target_table: str
    target_schema: str
    entity_key: list[str] | None
```

`DatasetUpdateConfig`s define when and how to execute collection of the indicated dataset.

`DatasetUpdateConfig` instances should be added to the `sources/update_configs.py` file.

```python
@dataclass
class DatasetUpdateConfig:
    spec: DatasetSpec
    full_update_cron: str
    update_cron: str
    full_update_mode: str = "api"  # "api" or "file_download"
```

#### airflow/dags/loci/db/

This directory contains the tooling that is responsible for interacting with databases. Tooling has been developed with the goal of keeping memory usage very low during ingestion (so that this code can run on inexpensive machines with minimal RAM).

The `PostgresEngine` class in `core.py` provides functionality to execute queries and commands to a PostGIS/Postgres database.

The `StagedIngest` class implements ingestion of a data stream into a temporary staging table, and upon successful collection of the data stream then it will integrate the data in that temp table into the `target_table`, employing the configured integration mode (either Type 2 Slowly Changing Dimensions, upserting, or simple appending).

#### airflow/dags/loci/tracking/

This directory contains tooling that implements ingestion-run logging for auditability purposes.

#### airflow/dags/loci/collectors/

This directory contains the tooling that implements exploration, dataset specification, and collection from individual data sources.

ToDo: Document the division of responsibilities across the different emerging interfaces (i.e. `<Source>Collector`, `<Source>Client`, `<Source>Metadata`, etc) and the `generate_ddl()` functionality.

#### airflow/dags/loci/parsers/

This directory contains tooling that is responsible for parsing different file formats to data streams (i.e. batches of ingestable records).

#### airflow/dags/loci/tasks/

This directory contains encapsulated and reusable Airflow task code.

#### airflow/dags/loci/transform/

This directory contains tooling to generate `dbt` models for different data sources.

## migrations/

This directory contains the database migration files that execute the DDL that creates the durable data warehouse tables (e.g. raw_data tables, metadata tracking/auditing tables, tables with results of expensive calculations, etc).

New migration scripts must start with `V<N+1>__some_description` (where `N` is the number of the latest existing migration script), and the number can't be zero-padded.

To execute migrations, run

```console
podman compose up flyway-postgis
```

## tests/

This directory contains automated tests, mainly covering the functionality defined in `airflow/dags/loci/`.

To run all tests, `cd` to `platform/` and run

```console
uv run pytest
```

There are some tests that actually make network calls, and this can be somewhat slow (resulting in ~120s testing runs). To exclude those tests, run

```console
uv run pytest -m "not network"
```
