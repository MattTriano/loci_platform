# Data Transformations via `dbt`

## Dev Notes

The (quite opinionated) notes below assume you are using `uv` for local dev purposes and that you're `ssh`'d into the machine you're developing on.

### Generating and serving the docs site (shows lineage and all models/tests/macros)

To generate the `dbt docs` site, run the commands below to set the required env-vars and generate the docs asset.

```console
export DBT_PG_HOST=localhost
export DBT_PG_PORT=54321
uv run dbt docs generate --profiles-dir ..
```

Then, to serve the docs

```console
uv run dbt docs serve --host 0.0.0.0 --port <any_free_port_number>
```

Then the site will be accessible via your browser at http://<dev_machine_ip_addr>:<port_number_from_above>. You can use `hostname -I` or `ip addr` to find your machine's IP address.


## `dbt` Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on `dbt`'s development and best practices
