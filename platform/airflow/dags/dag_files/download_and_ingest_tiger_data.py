import datetime as dt
import logging
import subprocess
from pathlib import Path
from typing import Optional

from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook

from loci.db.af_utils import get_postgres_engine

task_logger = logging.getLogger("airflow.task")

# Configure these
TIGER_YEAR = "2025"
TIGER_DATA_DIR = Path("/opt/airflow/raw_data/gisdata")
CONN_ID = "gis_dwh_db"

# State FIPS codes to load (None = all states)
# Common ones: 11=DC, 06=CA, 36=NY, 48=TX, 17=IL
STATE_FIPS_LIST: Optional[list[str]] = ["06", "11", "17", "36"]


def get_connection_dict(conn_id: str) -> dict:
    """Extract connection parameters from Airflow connection."""
    conn = BaseHook.get_connection(conn_id)
    return {
        "host": conn.host,
        "port": conn.port or 5432,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password,
    }


@dag(
    schedule="0 20 20 1 *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["tiger", "geocoder"],
)
def download_and_ingest_tiger_data():
    @task
    def setup_tiger_geocoder() -> dict:
        """
        Set up PostGIS tiger_geocoder extension and configure loader settings.
        Returns connection info and staging directory.
        """
        pg_engine = get_postgres_engine(conn_id=CONN_ID, logger=task_logger)
        conn_dict = get_connection_dict(CONN_ID)

        pg_engine.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;")
        pg_engine.execute(
            "CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder CASCADE;"
        )
        pg_engine.execute("CREATE EXTENSION IF NOT EXISTS address_standardizer;")
        pg_engine.execute("CREATE SCHEMA IF NOT EXISTS tiger_data;")
        pg_engine.execute("CREATE SCHEMA IF NOT EXISTS tiger_staging;")

        staging_dir = TIGER_DATA_DIR / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        temp_dir = staging_dir / "temp"
        temp_dir.mkdir(parents=True, exist_ok=True)

        declare_sect = f'''
TMPDIR="{temp_dir}/"
UNZIPTOOL=unzip
WGETTOOL="/usr/bin/wget"
export PGBIN=/usr/bin
export PGPORT={conn_dict["port"]}
export PGHOST={conn_dict["host"]}
export PGUSER={conn_dict["user"]}
export PGPASSWORD={conn_dict["password"]}
export PGDATABASE={conn_dict["database"]}
PSQL=${{PGBIN}}/psql
SHP2PGSQL=${{PGBIN}}/shp2pgsql
'''

        pg_engine.execute(f"""
            UPDATE tiger.loader_platform
            SET declare_sect = '{declare_sect}'
            WHERE os = 'sh';
            """)
        pg_engine.execute(f"""
            UPDATE tiger.loader_variables
            SET staging_fold = '{str(staging_dir)}', tiger_year = '{TIGER_YEAR}';
            """)

        task_logger.info(f"Configured tiger_geocoder for year {TIGER_YEAR}")

        return {
            "staging_dir": str(staging_dir),
            "year": TIGER_YEAR,
        }

    @task
    def generate_nation_script(config: dict) -> str:
        pg_engine = get_postgres_engine(conn_id=CONN_ID, logger=task_logger)
        staging_dir = Path(config["staging_dir"])
        result = pg_engine.query("SELECT loader_generate_nation_script('sh');")
        script_content = result.iloc[0, 0]

        script_path = staging_dir / "load_nation.sh"
        script_path.write_text(script_content)
        script_path.chmod(0o755)

        task_logger.info(f"Generated nation script: {script_path}")
        return str(script_path)

    @task
    def run_nation_script(script_path: str) -> bool:
        task_logger.info(f"Running nation script: {script_path}")

        result = subprocess.run(
            ["bash", script_path],
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
        )

        if result.stdout:
            task_logger.info(f"stdout: {result.stdout[-2000:]}")  # Last 2000 chars
        if result.stderr:
            task_logger.warning(f"stderr: {result.stderr[-2000:]}")

        if result.returncode != 0:
            raise RuntimeError(f"Nation script failed with code {result.returncode}")

        task_logger.info("Nation script completed successfully")
        return True

    @task
    def get_state_list(config: dict) -> list[str]:
        pg_engine = get_postgres_engine(conn_id=CONN_ID, logger=task_logger)

        if STATE_FIPS_LIST:
            fips_str = ",".join(f"'{f}'" for f in STATE_FIPS_LIST)
            result = pg_engine.query(f"""
                SELECT abbrev FROM tiger.state_lookup
                WHERE st_code IN ({fips_str})
                ORDER BY abbrev;
            """)
            states = result["abbrev"].tolist()
            task_logger.info(f"Using configured state list: {states}")
            return states

        result = pg_engine.query("""
            SELECT abbrev FROM tiger.state_lookup ORDER BY abbrev;
        """)

        states = result["abbrev"].tolist()
        task_logger.info(f"Found {len(states)} states to process")
        return states

    @task
    def generate_state_script(state_abbrev: str, config: dict) -> str:
        pg_engine = get_postgres_engine(conn_id=CONN_ID, logger=task_logger)
        staging_dir = Path(config["staging_dir"])
        result = pg_engine.query(f"""
            SELECT loader_generate_script(ARRAY['{state_abbrev}'], 'sh');
        """)

        script_content = result.iloc[0, 0]

        if not script_content:
            task_logger.warning(f"Empty script generated for {state_abbrev}")
            return ""

        script_path = staging_dir / f"load_{state_abbrev.lower()}.sh"
        script_path.write_text(script_content)
        script_path.chmod(0o755)

        task_logger.info(f"Generated state script: {script_path}")
        return str(script_path)

    @task
    def run_state_script(script_path: str) -> bool:
        if not script_path:
            task_logger.info("Skipping empty script path")
            return True

        task_logger.info(f"Running state script: {script_path}")

        result = subprocess.run(
            ["bash", script_path],
            capture_output=True,
            text=True,
        )

        if result.stdout:
            task_logger.info(f"stdout: {result.stdout[-2000:]}")
        if result.stderr:
            task_logger.warning(f"stderr: {result.stderr[-2000:]}")

        if result.returncode != 0:
            task_logger.error(f"State script failed: {script_path}")
            return False

        task_logger.info(f"State script completed: {script_path}")
        return True

    @task
    def create_indexes_and_finalize(config: dict) -> bool:
        pg_engine = get_postgres_engine(conn_id=CONN_ID, logger=task_logger)
        task_logger.info("Installing missing indexes...")
        pg_engine.execute("SELECT install_missing_indexes();")
        task_logger.info("Updating statistics...")
        tables = pg_engine.query("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'tiger_data';
        """)

        for table in tables["table_name"].values:
            try:
                pg_engine.execute(f"ANALYZE tiger_data.{table};")
            except Exception as e:
                task_logger.warning(f"Failed to analyze {table}: {e}")

        task_logger.info("Finalization complete")
        return True

    @task
    def test_geocoder() -> bool:
        pg_engine = get_postgres_engine(conn_id=CONN_ID, logger=task_logger)

        test_addresses = [
            "1600 Pennsylvania Ave NW, Washington, DC 20500",
            "350 Fifth Avenue, New York, NY 10118",
        ]

        for addr in test_addresses:
            task_logger.info(f"Testing: {addr}")

            try:
                result = pg_engine.query(f"""
                    SELECT
                        g.rating,
                        ST_Y(g.geomout) AS lat,
                        ST_X(g.geomout) AS lon,
                        pprint_addy(g.addy) AS formatted
                    FROM geocode('{addr}') AS g
                    LIMIT 1;
                """)

                if not result.empty:
                    row = result.iloc[0]
                    task_logger.info(
                        f"  Result: {row['formatted']} "
                        f"({row['lat']}, {row['lon']}) rating={row['rating']}"
                    )
                else:
                    task_logger.warning(f"  No results for: {addr}")
            except Exception as e:
                task_logger.error(f"  Geocode failed: {e}")

        return True

    config = setup_tiger_geocoder()
    nation_script = generate_nation_script(config)
    nation_done = run_nation_script(nation_script)
    state_list = get_state_list(config)
    state_list.set_upstream(nation_done)
    state_scripts = generate_state_script.expand(
        state_abbrev=state_list, config=[config]
    )
    state_results = run_state_script.expand(script_path=state_scripts)
    finalize = create_indexes_and_finalize(config)
    finalize.set_upstream(state_results)
    test = test_geocoder()
    test.set_upstream(finalize)


download_and_ingest_tiger_data()
