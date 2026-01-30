## Usage

To run migrations, add migration scripts to the relevant location in the `/migrations` dir. Make sure they match the naming scheme `V[1-9]__description_of_changes.sql` (I don't think you can zero-pad the number).

This command runs migrations for both databases.

```console
podman compose up flyway-mysql flyway-postgres
```

If a migration fails, in MySQL you have to manually revert any changes made in the failed migration pre-failure then repair the metadata in the `flyway_schema_history` (via the `repair` command below), then rerun your migration after fixing the latest migration file.

```console
podman compose run --rm flyway-mysql repair
```