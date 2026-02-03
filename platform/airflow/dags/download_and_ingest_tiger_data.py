import datetime as dt
import logging
import os
import subprocess
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain
from airflow.sdk.bases.hook import BaseHook

from db.core import get_postgres_engine, PostgresEngine


task_logger = logging.getLogger("airflow.task")


class TIGERDownloader:
    NATIONAL_LAYERS = ["STATE", "COUNTY"]
    STATE_LAYERS = [
        "PLACE",
        "COUSUB",
        "TRACT",
        "BG",
        "TABBLOCK20",
        "EDGES",
        "FACES",
        "ADDR",
        "FEATNAMES",
    ]

    def __init__(
        self, data_dir: str | Path, year: str, state_fips: Optional[list[str]] = None
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.year = year
        self.state_fips = state_fips

    def download_data(self) -> None:
        total = 0
        print("\n=== National layers ===")
        for layer in self.NATIONAL_LAYERS:
            count = self.download_layer(layer)
            print(f"{layer}: {count} files")
            total += count
        print("\n=== State layers ===")
        for layer in self.STATE_LAYERS:
            count = self.download_layer(layer)
            print(f"{layer}: {count} files")
            total += count
        print(f"\nDone. Total files: {total}")

    @property
    def base_url(self):
        return f"https://www2.census.gov/geo/tiger/TIGER{self.year}"

    def get_file_list(self, url: str) -> list[str]:
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                html = response.read().decode("utf-8")
            files = []
            for line in html.split('"'):
                if line.endswith(".zip"):
                    files.append(line)
            return files
        except Exception as e:
            print(f"Error listing {url}: {e}")
            return []

    def download_file(self, url: str, dest_path: Path) -> bool:
        if dest_path.exists():
            print(f"Skipping (exists): {dest_path.name}")
            return True
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading: {dest_path.name}")
        try:
            urllib.request.urlretrieve(url, dest_path)
            return True
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return False

    def download_layer(self, layer: str) -> int:
        layer_url = f"{self.base_url}/{layer}/"
        files = self.get_file_list(layer_url)
        if self.state_fips:
            files = [
                f for f in files if any(f"_{fips}_" in f for fips in self.state_fips)
            ]
        downloaded = 0
        for filename in files:
            url = f"{layer_url}{filename}"
            dest_path = self.data_dir.joinpath(layer, filename)
            if self.download_file(url, dest_path):
                downloaded += 1
        return downloaded


"""
TIGERIngester: Load TIGER shapefiles into PostGIS and configure the geocoder.

Requires:
- PostGIS with postgis_tiger_geocoder extension
- shp2pgsql utility (comes with PostGIS)
- psycopg2
"""


class TIGERIngester:
    """Ingest TIGER data into PostGIS and configure the geocoder."""

    # Mapping of TIGER layers to their target schemas/tables
    LAYER_CONFIG = {
        # National layers
        "STATE": {"schema": "tiger_data", "table": "state_all", "loader": "state"},
        "COUNTY": {"schema": "tiger_data", "table": "county_all", "loader": "county"},
        # State layers
        "PLACE": {"schema": "tiger_data", "table": "place", "loader": "place"},
        "COUSUB": {"schema": "tiger_data", "table": "cousub", "loader": "cousub"},
        "TRACT": {"schema": "tiger_data", "table": "tract", "loader": "tract"},
        "BG": {"schema": "tiger_data", "table": "bg", "loader": "bg"},
        "TABBLOCK20": {
            "schema": "tiger_data",
            "table": "tabblock20",
            "loader": "tabblock20",
        },
        "EDGES": {"schema": "tiger_data", "table": "edges", "loader": "edges"},
        "FACES": {"schema": "tiger_data", "table": "faces", "loader": "faces"},
        "ADDR": {"schema": "tiger_data", "table": "addr", "loader": "addr"},
        "FEATNAMES": {
            "schema": "tiger_data",
            "table": "featnames",
            "loader": "featnames",
        },
    }

    def __init__(self, conn_id: str, data_dir: str | Path, pg_engine: PostgresEngine):
        self.conn_id = conn_id
        self.data_dir = Path(data_dir)
        self.pg_engine = pg_engine
        self._conn_dict = self._extract_connection()

    def _extract_connection(self) -> dict:
        conn = BaseHook.get_connection(self.conn_id)
        port = conn.port
        if port is None:
            port = "5432"
        return {
            "host": conn.host,
            "port": port,
            "database": conn.schema,
            "user": conn.login,
            "password": conn.password,
        }

    def setup_schemas(self) -> None:
        """Create required schemas."""
        print("Setting up schemas...")
        self.pg_engine.execute("CREATE SCHEMA IF NOT EXISTS tiger;")
        self.pg_engine.execute("CREATE SCHEMA IF NOT EXISTS tiger_data;")
        self.pg_engine.execute("GRANT USAGE ON SCHEMA tiger TO PUBLIC;")
        self.pg_engine.execute("GRANT USAGE ON SCHEMA tiger_data TO PUBLIC;")
        self.pg_engine.execute("GRANT select ON ALL TABLES IN SCHEMA tiger TO PUBLIC;")
        self.pg_engine.execute(
            "GRANT select ON ALL TABLES IN SCHEMA tiger_data TO PUBLIC;"
        )
        print("Schemas created.")

    def extract_zip(self, zip_path: Path, extract_dir: Path) -> Path:
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        shp_files = list(extract_dir.glob("*.shp"))
        if shp_files:
            return shp_files[0]
        dbf_files = list(extract_dir.glob("*.dbf"))
        if dbf_files:
            return dbf_files[0]
        raise FileNotFoundError(f"No shapefile or DBF found in {zip_path}")

    def load_shapefile(
        self,
        shp_path: Path,
        schema: str,
        table: str,
        append: bool = True,
    ) -> bool:
        """Load a shapefile into PostGIS using shp2pgsql."""
        mode_flags = ["-a"] if append else ["-c", "-D"]
        shp2pgsql_cmd = [
            "shp2pgsql",
            *mode_flags,
            "-s",
            "4269",
            "-g",
            "the_geom",
            "-W",
            "latin1",
            str(shp_path),
            f"{schema}.{table}",
        ]
        psql_env = {**os.environ, "PGPASSWORD": self._conn_dict["password"]}
        psql_cmd = [
            "psql",
            "-h",
            self._conn_dict["host"],
            "-p",
            str(self._conn_dict["port"]),
            "-d",
            self._conn_dict["database"],
            "-U",
            self._conn_dict["user"],
            "-q",  # quiet
        ]

        try:
            # Pipe shp2pgsql output to psql
            shp2pgsql_proc = subprocess.Popen(
                shp2pgsql_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            print(f"psql command: {' '.join(psql_cmd)}")
            psql_proc = subprocess.Popen(
                psql_cmd,
                stdin=shp2pgsql_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=psql_env,
            )
            shp2pgsql_proc.stdout.close()
            shp2pgsql_stderr = shp2pgsql_proc.stderr.read()
            if shp2pgsql_stderr:
                print(f"shp2pgsql stderr: {shp2pgsql_stderr.decode()}")

            psql_stdout, psql_stderr = psql_proc.communicate()
            print(f"psql returncode: {psql_proc.returncode}")

            if psql_proc.returncode != 0:
                print(f"Error loading {shp_path}: {psql_stderr.decode()}")
                print(f"psql stdout: {psql_stdout.decode()}")
                print(f"psql stderr: {psql_stderr.decode()}")
                return False
            return True
        except Exception as e:
            print(f"Error loading {shp_path}: {e}")
            return False

    def table_exists(self, schema: str, table: str) -> bool:
        result = self.pg_engine.query(f"""
            select exists (
                select 1 from information_schema.tables
                where table_schema = '{schema}' and table_name = '{table}'
            );
        """)
        return result.iloc[0, 0]

    def layer_loaded(self, schema: str, table: str) -> bool:
        if not self.table_exists(schema, table):
            return False
        result = self.pg_engine.query(f"select COUNT(*) > 0 from {schema}.{table};")
        return result.iloc[0, 0]

    def ingest_layer(self, layer: str) -> int:
        """Ingest all files for a layer."""
        if layer not in self.LAYER_CONFIG:
            print(f"Unknown layer: {layer}")
            return 0

        config = self.LAYER_CONFIG[layer]
        if self.layer_loaded(config["schema"], config["table"]):
            print(f"Skipping {layer}: already loaded")
            return 0

        layer_dir = self.data_dir / layer
        if not layer_dir.exists():
            print(f"Layer directory not found: {layer_dir}")
            return 0

        zip_files = sorted(layer_dir.glob("*.zip"))
        if not zip_files:
            print(f"No zip files found for layer {layer}")
            return 0

        print(f"\nIngesting {layer} ({len(zip_files)} files)...")
        loaded = 0
        first_file = True

        config = self.LAYER_CONFIG[layer]
        table_exists = self.table_exists(config["schema"], config["table"])

        for zip_path in zip_files:
            append = table_exists or not first_file
            extract_dir = layer_dir / "extracted" / zip_path.stem
            try:
                shp_path = self.extract_zip(zip_path, extract_dir)
                success = self.load_shapefile(
                    shp_path,
                    config["schema"],
                    config["table"],
                    append=append,
                )
                if success:
                    loaded += 1
                    first_file = False
                    print(f"  Loaded: {zip_path.name}")
            except Exception as e:
                print(f"  Error processing {zip_path.name}: {e}")

        return loaded

    def ingest_all(self) -> None:
        """Ingest all downloaded TIGER data."""
        print("\n=== Starting TIGER data ingestion ===")
        self.setup_schemas()
        national_layers = ["STATE", "COUNTY"]
        for layer in national_layers:
            count = self.ingest_layer(layer)
            print(f"{layer}: {count} files loaded")

        state_layers = [
            "PLACE",
            "COUSUB",
            "TRACT",
            "BG",
            "TABBLOCK20",
            "EDGES",
            "FACES",
            "ADDR",
            "FEATNAMES",
        ]
        for layer in state_layers:
            count = self.ingest_layer(layer)
            print(f"{layer}: {count} files loaded")
        self.create_indexes()
        self.update_loader_tables()
        print("\n=== Ingestion complete ===")

    def create_indexes(self) -> None:
        """Create spatial and attribute indexes for geocoding performance."""
        print("\nCreating indexes...")

        index_statements = [
            ("tiger_data", "edges", "the_geom", "gist"),
            ("tiger_data", "faces", "the_geom", "gist"),
            # ("tiger_data", "addr", "the_geom", "gist"),
            ("tiger_data", "place", "the_geom", "gist"),
            ("tiger_data", "cousub", "the_geom", "gist"),
            ("tiger_data", "county_all", "the_geom", "gist"),
            ("tiger_data", "state_all", "the_geom", "gist"),
            ("tiger_data", "tract", "the_geom", "gist"),
            ("tiger_data", "bg", "the_geom", "gist"),
            ("tiger_data", "tabblock20", "the_geom", "gist"),
        ]

        attr_indexes = [
            # "create index if not exists idx_tiger_edges_tlid ON tiger_data.edges(tlid);",
            "create index if not exists idx_tiger_edges_tfidl ON tiger_data.edges(tfidl);",
            "create index if not exists idx_tiger_edges_tfidr ON tiger_data.edges(tfidr);",
            "create index if not exists idx_tiger_edges_countyfp ON tiger_data.edges(countyfp);",
            "create index if not exists idx_tiger_edges_statefp ON tiger_data.edges(statefp);",
            "create index if not exists idx_tiger_edges_fullname ON tiger_data.edges(fullname);",
            "create index if not exists idx_tiger_edges_lfromadd ON tiger_data.edges(lfromadd);",
            "create index if not exists idx_tiger_edges_ltoadd ON tiger_data.edges(ltoadd);",
            "create index if not exists idx_tiger_edges_rfromadd ON tiger_data.edges(rfromadd);",
            "create index if not exists idx_tiger_edges_rtoadd ON tiger_data.edges(rtoadd);",
            "create index if not exists idx_tiger_edges_zipl ON tiger_data.edges(zipl);",
            "create index if not exists idx_tiger_edges_zipr ON tiger_data.edges(zipr);",
            # "create index if not exists idx_tiger_faces_tfid ON tiger_data.faces(tfid);",
            "create index if not exists idx_tiger_faces_countyfp20 ON tiger_data.faces(countyfp20);",
            "create index if not exists idx_tiger_faces_statefp20 ON tiger_data.faces(statefp20);",
            "create index if not exists idx_tiger_addr_tlid ON tiger_data.addr(tlid);",
            "create index if not exists idx_tiger_addr_statefp ON tiger_data.addr(statefp);",
            "create index if not exists idx_tiger_addr_zip ON tiger_data.addr(zip);",
            "create index if not exists idx_tiger_addr_fromhn ON tiger_data.addr(fromhn);",
            "create index if not exists idx_tiger_addr_tohn ON tiger_data.addr(tohn);",
            "create index if not exists idx_tiger_featnames_tlid ON tiger_data.featnames(tlid);",
            "create index if not exists idx_tiger_featnames_name ON tiger_data.featnames(name);",
            "create index if not exists idx_tiger_featnames_fullname ON tiger_data.featnames(fullname);",
            "create index if not exists idx_tiger_place_statefp ON tiger_data.place(statefp);",
            "create index if not exists idx_tiger_place_name ON tiger_data.place(name);",
            "create index if not exists idx_tiger_cousub_statefp ON tiger_data.cousub(statefp);",
            "create index if not exists idx_tiger_cousub_countyfp ON tiger_data.cousub(countyfp);",
            "create index if not exists idx_tiger_county_statefp ON tiger_data.county_all(statefp);",
            "create index if not exists idx_tiger_county_name ON tiger_data.county_all(name);",
            "create index if not exists idx_tiger_state_stusps ON tiger_data.state_all(stusps);",
            "create index if not exists idx_tiger_state_name ON tiger_data.state_all(name);",
        ]
        self.pg_engine.execute("alter table tiger.state_lookup alter column name type varchar(100);")
        self.pg_engine.execute("alter table tiger.county_lookup alter column name type varchar(100);")

        for schema, table, column, idx_type in index_statements:
            idx_name = f"idx_{table}_{column}"
            try:
                self.pg_engine.execute(f"""
                    create index if not exists {idx_name}
                    on {schema}.{table}
                    using {idx_type} ({column});
                """)
                print(f"  Created index: {idx_name}")
            except Exception as e:
                print(f"  Error creating index {idx_name}: {e}")
        for stmt in attr_indexes:
            try:
                self.pg_engine.execute(stmt)
            except Exception as e:
                print(f"  Error: {e}")

        print("Indexes created.")

    def update_loader_tables(self) -> None:
        """Update the geocoder loader tables to point to our data."""
        print("\nUpdating geocoder configuration...")
        declare_sect = f"""TMPDIR="${{staging_fold}}/temp/"
            UNZIPTOOL=unzip
            WGETTOOL="/usr/bin/wget"
            export PGBIN=/usr/bin
            export PGPORT={self._conn_dict["port"]}
            export PGHOST={self._conn_dict["host"]}
            export PGUSER={self._conn_dict["user"]}
            export PGPASSWORD={self._conn_dict["password"]}
            export PGDATABASE={self._conn_dict["database"]}
            PSQL=${{PGBIN}}/psql
            SHP2PGSQL=${{PGBIN}}/shp2pgsql"""

        self.pg_engine.execute(f"""
            update tiger.loader_platform
            set declare_sect = '{declare_sect}'
            where os = 'sh'; """)
        print("Geocoder configuration updated.")

        self.pg_engine.execute("""
            insert into tiger.state_lookup (st_code, name, abbrev)
            select statefp::integer, name, stusps
            from tiger_data.state_all
            on conflict (st_code) DO NOTHING;
        """)
        self.pg_engine.execute("""
            insert into tiger.county_lookup (st_code, co_code, name)
            select statefp::integer, countyfp::integer, name
            from tiger_data.county_all
            on conflict DO NOTHING;
        """)
        print("Geocoder configuration updated.")

    def create_state_tables(self, state_fips: str | None = None) -> None:
        """
        Create state-specific tables using PostGIS tiger geocoder conventions.
        This follows the naming pattern: {state_abbr}_{layer}

        If state_fips is None, creates tables for all states.
        """
        if state_fips is None:
            states = self.pg_engine.query("""
                SELECT statefp, stusps FROM tiger_data.state_all ORDER BY statefp;
            """)
            for _, row in states.iterrows():
                self.create_state_tables(row['statefp'])
            return
        print(f"\nCreating state tables for FIPS {state_fips}...")
        result = self.pg_engine.query(f"""
            SELECT stusps FROM tiger_data.state_all
            WHERE statefp = '{state_fips}' LIMIT 1;
        """)

        if result.empty:
            print(f"State FIPS {state_fips} not found")
            return

        state_abbr = result.iloc[0, 0].lower()
        # Define tables and their statefp column names
        tables = [
            ("edges", "statefp"),
            ("faces", "statefp"),
            ("place", "statefp"),
            ("cousub", "statefp"),
            ("tract", "statefp"),
            ("bg", "statefp"),
            ("tabblock20", "statefp20"),
        ]
        for source_table, fips_col in tables:
            target_table = f"{state_abbr}_{source_table}"
            self.pg_engine.execute(f"""
                CREATE TABLE IF NOT EXISTS tiger_data.{target_table} AS
                SELECT * FROM tiger_data.{source_table} WHERE {fips_col} = '{state_fips}';
            """)
            print(f"  Created {target_table}")
        self.pg_engine.execute(f"""
            CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_addr AS
            SELECT a.* FROM tiger_data.addr a
            JOIN tiger_data.edges e ON a.tlid = e.tlid
            WHERE e.statefp = '{state_fips}';
        """)
        self.pg_engine.execute(f"""
            CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_county AS
            SELECT * FROM tiger_data.county_all WHERE statefp = '{state_fips}';
        """)
        print(f"  Created {state_abbr}_county")
        # Zip lookup
        self.pg_engine.execute(f"""
            CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_zip_lookup_base AS
            SELECT DISTINCT e.zipl AS zip, e.statefp, e.countyfp
            FROM tiger_data.edges e
            WHERE e.statefp = '{state_fips}' AND e.zipl IS NOT NULL
            UNION
            SELECT DISTINCT e.zipr AS zip, e.statefp, e.countyfp
            FROM tiger_data.edges e
            WHERE e.statefp = '{state_fips}' AND e.zipr IS NOT NULL;
        """)
        self.pg_engine.execute(f"""
            CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_featnames AS
            SELECT f.* FROM tiger_data.featnames f
            JOIN tiger_data.edges e ON f.tlid = e.tlid
            WHERE e.statefp = '{state_fips}';
        """)
        print(f"  Created {state_abbr}_zip_lookup_base")
        print(f"State tables created for {state_abbr.upper()}")

    # def create_state_tables(self, state_fips: str) -> None:
    #     """
    #     Create state-specific tables using PostGIS tiger geocoder conventions.
    #     This follows the naming pattern: {state_abbr}_{layer}
    #     """
    #     print(f"\nCreating state tables for FIPS {state_fips}...")

    #     with self.get_connection() as conn:
    #         with conn.cursor() as cur:
    #             self.pg_engine.execute(
    #                 """
    #                 select stusps from tiger_data.state_all
    #                 where statefp = %s LIMIT 1;
    #             """,
    #                 (state_fips,),
    #             )
    #             result = cur.fetchone()
    #             if not result:
    #                 print(f"State FIPS {state_fips} not found")
    #                 return

    #             state_abbr = result[0].lower()
    #             self.pg_engine.execute(f"""
    #                 create table if not exists tiger_data.{state_abbr}_edges as
    #                 select * from tiger_data.edges where statefp = '{state_fips}'; """)
    #             self.pg_engine.execute(f"""
    #                 create table if not exists tiger_data.{state_abbr}_faces as
    #                 select * from tiger_data.faces where statefp = '{state_fips}'; """)
    #             self.pg_engine.execute(f"""
    #                 create table if not exists tiger_data.{state_abbr}_addr as
    #                 select * from tiger_data.addr where statefp = '{state_fips}'; """)
    #             self.pg_engine.execute(f"""
    #                 create table if not exists tiger_data.{state_abbr}_featnames as
    #                 select * from tiger_data.featnames where statefp = '{state_fips}' ; """)
    #             self.pg_engine.execute(f"""
    #                 create table if not exists tiger_data.{state_abbr}_place as
    #                 select * from tiger_data.place where statefp = '{state_fips}'; """)
    #             self.pg_engine.execute(f"""
    #                 create table if not exists tiger_data.{state_abbr}_cousub as
    #                 select * from tiger_data.cousub where statefp = '{state_fips}'; """)
    #             self.pg_engine.execute(f"""
    #                 create table if not exists tiger_data.{state_abbr}_county as
    #                 select * from tiger_data.county_all where statefp = '{state_fips}'; """)
    #             self.pg_engine.execute(f"""
    #                 create table if not exists tiger_data.{state_abbr}_zip_lookup_base as
    #                 select distinct e.zipl as zip, e.statefp, e.countyfp
    #                 from tiger_data.edges e
    #                 where e.statefp = ''{state_fips}' AND e.zipl is not null
    #                 union
    #                 select distinct e.zipr as zip, e.statefp, e.countyfp
    #                 from tiger_data.edges e
    #                 where e.statefp = '{state_fips}' AND e.zipr is not null; """)
    #     print(f"State tables created for {state_abbr.upper()}")

    def vacuum_analyze(self) -> None:
        """Run VACUUM ANALYZE on all tiger_data tables."""
        print("\nRunning VACUUM ANALYZE...")

        tables = self.pg_engine.query("""
            select table_name
            from information_schema.tables
            where table_schema = 'tiger_data';
        """)

        for table in tables["table_name"].values:
            print(f"  Analyzing tiger_data.{table}...")
            self.pg_engine.execute(f"VACUUM ANALYZE tiger_data.{table};")
        print("VACUUM ANALYZE complete.")

    def test_geocoder(self, address: str) -> None:
        """Test the geocoder with a sample address."""
        print(f"\nTesting geocoder with: {address}")
        norm = self.pg_engine.query(
            f""" select * from normalize_address('{address}'); """
        )
        print(f"Normalized: {norm}")

        results = self.pg_engine.query(f"""
            select g.rating,
                   ST_X(g.geomout) as lon,
                   ST_Y(g.geomout) as lat,
                   pprint_addy(g.addy) as formatted_address
            from geocode('{address}') as g
            limit 5; """)

        if results:
            print("Geocoding results:")
            for rating, lon, lat, formatted in results:
                print(f"  Rating: {rating}, ({lat}, {lon})")
                print(f"  Address: {formatted}")
        else:
            print("No geocoding results found.")


@dag(
    schedule="0 20 20 1 *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["mock_data"],
)
def download_and_ingest_tiger_data():
    @task
    def download_tiger_data() -> bool:
        data_dir = Path("/opt/airflow/raw_data/gisdata")
        tiger_downloader = TIGERDownloader(data_dir=data_dir, year="2025")
        tiger_downloader.download_data()
        task_logger.info("Downloaded TIGER Data ")
        return True

    @task
    def load_tiger_data() -> bool:
        data_dir = Path("/opt/airflow/raw_data/gisdata")
        pg_engine = get_postgres_engine(conn_id="gis_dwh_db", logger=task_logger)
        tiger_loader = TIGERIngester(
            conn_id="gis_dwh_db", data_dir=data_dir, pg_engine=pg_engine
        )
        tiger_loader.ingest_all()
        tiger_loader.create_state_tables()
        tiger_loader.vacuum_analyze()
        task_logger.info("Loaded TIGER Data ")
        return True

    get_data = download_tiger_data()
    ingest_data = load_tiger_data()
    chain(get_data, ingest_data)


download_and_ingest_tiger_data()
