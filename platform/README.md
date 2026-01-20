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
