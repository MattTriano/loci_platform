import datetime as dt
import logging
import shutil
import time
from pathlib import Path

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain

from db.airflow import get_postgres_engine, get_mysql_engine
from datagen.data_model_mocker import DataFaker

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule="0 20 20 1 *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["mock_data"],
)
def generate_and_ingest_data():
    @task
    def show_postgres_schemas() -> list[dict[str, str]]:
        pg_engine = get_postgres_engine(conn_id="dwh_db", logger=task_logger)
        schemas = pg_engine.query(""" select * from information_schema.schemata """)
        for schema in schemas["schema_name"].values:
            print(schema)
        return schemas

    @task
    def show_mysql_schemas() -> list[dict[str, str]]:
        mysql_engine = get_mysql_engine(conn_id="app_db", logger=task_logger)
        schemas = mysql_engine.query(""" show schemas """)
        for schema in schemas["Database"].values:
            print(schema)
        return schemas

    @task
    def genate_fake_data() -> str:
        data_dir = Path("/opt/airflow/raw_data").joinpath("mock_data_model")
        shutil.rmtree(data_dir)
        _ = DataFaker(output_dir=data_dir, seed=42, incremental=False)
        time.sleep(2)
        _ = DataFaker(output_dir=data_dir, seed=314159, incremental=True)
        time.sleep(5)
        _ = DataFaker(output_dir=data_dir, seed=2718, incremental=True)
        return str(data_dir)

    @task
    def ingest_fake_data_files(data_dir) -> bool:
        pg_engine = get_postgres_engine(conn_id="dwh_db", logger=task_logger)
        data_dir = Path(data_dir)
        file_paths = [p for p in data_dir.iterdir() if p.name.endswith(".csv")]
        file_paths.sort()
        addr_files = [p for p in file_paths if "address" in p.name]
        biz_files = [p for p in file_paths if "business" in p.name]
        trading_files = [p for p in file_paths if "trading_partnership" in p.name]
        for fp in addr_files:
            task_logger.info(f"Ingesting {fp.name}")
            results = pg_engine.ingest_csv(filepath=fp, schema="raw_data", table_name="addresses")
            print(results)
            task_logger.info(f"File ingested")

        for fp in biz_files:
            task_logger.info(f"Ingesting {fp.name}")
            results = pg_engine.ingest_csv(filepath=fp, schema="raw_data", table_name="businesses")
            print(results)
            task_logger.info(f"File ingested")

        for fp in trading_files:
            task_logger.info(f"Ingesting {fp.name}")
            results = pg_engine.ingest_csv(
                filepath=fp, schema="raw_data", table_name="trading_partnerships"
            )
            print(results)
            task_logger.info(f"File ingested")
        return True

    @task
    def create_raw_data_schema() -> bool:
        pg_engine = get_postgres_engine(conn_id="dwh_db", logger=task_logger)
        result = pg_engine.execute(""" create schema if not exists raw_data """)
        task_logger.info(f"Created 'raw_data' schema, result: {result} ")
        return True

    @task
    def inspect_item(item) -> bool:
        print(item)
        return True

    generate_data = genate_fake_data()
    pg_schemas = show_postgres_schemas()
    mysql_schemas = show_mysql_schemas()
    create_raw = create_raw_data_schema()
    ingest_files = ingest_fake_data_files(generate_data)
    chain(create_raw, [pg_schemas, mysql_schemas], generate_data, ingest_files)


generate_and_ingest_data()
