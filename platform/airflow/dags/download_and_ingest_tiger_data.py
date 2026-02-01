import datetime as dt
import logging
import os
import shutil
import subprocess
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain
from airflow.sdk.bases.hook import BaseHook
import psycopg2

from db.core import get_postgres_engine, PostgresEngine
from datagen.data_model_mocker import DataFaker

import subprocess
import zipfile
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2 import sql

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

    def __init__(self, data_dir: str | Path, year: str, state_fips: Optional[list[str]] = None):
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
            files = [f for f in files if any(f"_{fips}_" in f for fips in self.state_fips)]
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
        "TABBLOCK20": {"schema": "tiger_data", "table": "tabblock20", "loader": "tabblock20"},
        "EDGES": {"schema": "tiger_data", "table": "edges", "loader": "edges"},
        "FACES": {"schema": "tiger_data", "table": "faces", "loader": "faces"},
        "ADDR": {"schema": "tiger_data", "table": "addr", "loader": "addr"},
        "FEATNAMES": {"schema": "tiger_data", "table": "featnames", "loader": "featnames"},
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
        self.pg_engine.execute("GRANT SELECT ON ALL TABLES IN SCHEMA tiger TO PUBLIC;")
        self.pg_engine.execute("GRANT SELECT ON ALL TABLES IN SCHEMA tiger_data TO PUBLIC;")
        print("Schemas created.")

    def extract_zip(self, zip_path: Path, extract_dir: Path) -> Path:
        """Extract a zip file and return the path to the shapefile."""
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        # Find the .shp file
        shp_files = list(extract_dir.glob("*.shp"))
        if not shp_files:
            raise FileNotFoundError(f"No shapefile found in {zip_path}")
        return shp_files[0]

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
            "-s", "4269",
            "-g", "the_geom",
            "-W", "latin1",
            str(shp_path),
            f"{schema}.{table}",
        ]
        psql_env = {**os.environ, "PGPASSWORD": self._conn_dict["password"]}
        psql_cmd = [
            "psql",
            "-h", self._conn_dict["host"],
            "-p", str(self._conn_dict["port"]),
            "-d", self._conn_dict["database"],
            "-U", self._conn_dict["user"],
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

    def ingest_layer(self, layer: str) -> int:
        """Ingest all files for a layer."""
        if layer not in self.LAYER_CONFIG:
            print(f"Unknown layer: {layer}")
            return 0

        config = self.LAYER_CONFIG[layer]
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

        for zip_path in zip_files:
            extract_dir = layer_dir / "extracted" / zip_path.stem
            try:
                shp_path = self.extract_zip(zip_path, extract_dir)
                # First file creates table, rest append
                success = self.load_shapefile(
                    shp_path,
                    config["schema"],
                    config["table"],
                    append=not first_file,
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

        # Ingest national layers first
        national_layers = ["STATE", "COUNTY"]
        for layer in national_layers:
            count = self.ingest_layer(layer)
            print(f"{layer}: {count} files loaded")

        # Ingest state layers
        state_layers = ["PLACE", "COUSUB", "TRACT", "BG", "TABBLOCK20",
                        "EDGES", "FACES", "ADDR", "FEATNAMES"]
        for layer in state_layers:
            count = self.ingest_layer(layer)
            print(f"{layer}: {count} files loaded")

        # Create indexes and finalize
        self.create_indexes()
        self.update_loader_tables()

        print("\n=== Ingestion complete ===")

    def create_indexes(self) -> None:
        """Create spatial and attribute indexes for geocoding performance."""
        print("\nCreating indexes...")

        index_statements = [
            # Spatial indexes
            ("tiger_data", "edges", "the_geom", "gist"),
            ("tiger_data", "faces", "the_geom", "gist"),
            ("tiger_data", "addr", "the_geom", "gist"),
            ("tiger_data", "place", "the_geom", "gist"),
            ("tiger_data", "cousub", "the_geom", "gist"),
            ("tiger_data", "county_all", "the_geom", "gist"),
            ("tiger_data", "state_all", "the_geom", "gist"),
            ("tiger_data", "tract", "the_geom", "gist"),
            ("tiger_data", "bg", "the_geom", "gist"),
            ("tiger_data", "tabblock20", "the_geom", "gist"),
        ]

        # Attribute indexes for geocoding
        attr_indexes = [
            # Edges indexes
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_tlid ON tiger_data.edges(tlid);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_tfidl ON tiger_data.edges(tfidl);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_tfidr ON tiger_data.edges(tfidr);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_countyfp ON tiger_data.edges(countyfp);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_statefp ON tiger_data.edges(statefp);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_fullname ON tiger_data.edges(fullname);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_lfromadd ON tiger_data.edges(lfromadd);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_ltoadd ON tiger_data.edges(ltoadd);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_rfromadd ON tiger_data.edges(rfromadd);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_rtoadd ON tiger_data.edges(rtoadd);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_zipl ON tiger_data.edges(zipl);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_edges_zipr ON tiger_data.edges(zipr);",
            # Faces indexes
            "CREATE INDEX IF NOT EXISTS idx_tiger_faces_tfid ON tiger_data.faces(tfid);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_faces_countyfp ON tiger_data.faces(countyfp);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_faces_statefp ON tiger_data.faces(statefp);",
            # Addr indexes
            "CREATE INDEX IF NOT EXISTS idx_tiger_addr_tlid ON tiger_data.addr(tlid);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_addr_statefp ON tiger_data.addr(statefp);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_addr_zip ON tiger_data.addr(zip);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_addr_fromhn ON tiger_data.addr(fromhn);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_addr_tohn ON tiger_data.addr(tohn);",
            # Featnames indexes
            "CREATE INDEX IF NOT EXISTS idx_tiger_featnames_tlid ON tiger_data.featnames(tlid);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_featnames_statefp ON tiger_data.featnames(statefp);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_featnames_name ON tiger_data.featnames(name);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_featnames_fullname ON tiger_data.featnames(fullname);",
            # Place indexes
            "CREATE INDEX IF NOT EXISTS idx_tiger_place_statefp ON tiger_data.place(statefp);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_place_name ON tiger_data.place(name);",
            # Cousub indexes
            "CREATE INDEX IF NOT EXISTS idx_tiger_cousub_statefp ON tiger_data.cousub(statefp);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_cousub_countyfp ON tiger_data.cousub(countyfp);",
            # County indexes
            "CREATE INDEX IF NOT EXISTS idx_tiger_county_statefp ON tiger_data.county_all(statefp);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_county_name ON tiger_data.county_all(name);",
            # State indexes
            "CREATE INDEX IF NOT EXISTS idx_tiger_state_stusps ON tiger_data.state_all(stusps);",
            "CREATE INDEX IF NOT EXISTS idx_tiger_state_name ON tiger_data.state_all(name);",
        ]

        for schema, table, column, idx_type in index_statements:
            idx_name = f"idx_{table}_{column}"
            try:
                self.pg_engine.execute(f"""
                    CREATE INDEX IF NOT EXISTS {idx_name}
                    ON {schema}.{table}
                    USING {idx_type} ({column});
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
        declare_sect = f'''TMPDIR="${{staging_fold}}/temp/"
            UNZIPTOOL=unzip
            WGETTOOL="/usr/bin/wget"
            export PGBIN=/usr/bin
            export PGPORT={self._conn_dict["port"]}
            export PGHOST={self._conn_dict["host"]}
            export PGUSER={self._conn_dict["user"]}
            export PGPASSWORD={self._conn_dict["password"]}
            export PGDATABASE={self._conn_dict["database"]}
            PSQL=${{PGBIN}}/psql
            SHP2PGSQL=${{PGBIN}}/shp2pgsql'''

        self.pg_engine.execute(f"""
            UPDATE tiger.loader_platform
            SET declare_sect = '{declare_sect}'
            WHERE os = 'sh'; """)

        print("Geocoder configuration updated.")

        # Populate state lookup if empty
        self.pg_engine.execute("""
            INSERT INTO tiger.state_lookup (st_code, name, abbrev)
            SELECT statefp::integer, name, stusps
            FROM tiger_data.state_all
            ON CONFLICT (st_code) DO NOTHING;
        """)

        # Populate county lookup
        self.pg_engine.execute("""
            INSERT INTO tiger.county_lookup (st_code, co_code, name)
            SELECT statefp::integer, countyfp::integer, name
            FROM tiger_data.county_all
            ON CONFLICT DO NOTHING;
        """)
        print("Geocoder configuration updated.")

    def create_state_tables(self, state_fips: str) -> None:
        """
        Create state-specific tables using PostGIS tiger geocoder conventions.
        This follows the naming pattern: {state_abbr}_{layer}
        """
        print(f"\nCreating state tables for FIPS {state_fips}...")

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Get state abbreviation
                self.pg_engine.execute("""
                    SELECT stusps FROM tiger_data.state_all 
                    WHERE statefp = %s LIMIT 1;
                """, (state_fips,))
                result = cur.fetchone()
                if not result:
                    print(f"State FIPS {state_fips} not found")
                    return

                state_abbr = result[0].lower()
                self.pg_engine.execute(f"""
                    CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_edges AS
                    SELECT * FROM tiger_data.edges WHERE statefp = '{state_fips}'; """)
                self.pg_engine.execute(f"""
                    CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_faces AS
                    SELECT * FROM tiger_data.faces WHERE statefp = '{state_fips}'; """)
                self.pg_engine.execute(f"""
                    CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_addr AS
                    SELECT * FROM tiger_data.addr WHERE statefp = '{state_fips}'; """)
                self.pg_engine.execute(f"""
                    CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_featnames AS
                    SELECT * FROM tiger_data.featnames WHERE statefp = '{state_fips}' ; """)
                self.pg_engine.execute(f"""
                    CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_place AS
                    SELECT * FROM tiger_data.place WHERE statefp = '{state_fips}'; """)
                self.pg_engine.execute(f"""
                    CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_cousub AS
                    SELECT * FROM tiger_data.cousub WHERE statefp = '{state_fips}'; """)
                self.pg_engine.execute(f"""
                    CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_county AS
                    SELECT * FROM tiger_data.county_all WHERE statefp = '{state_fips}'; """)
                self.pg_engine.execute(f"""
                    CREATE TABLE IF NOT EXISTS tiger_data.{state_abbr}_zip_lookup_base AS
                    SELECT DISTINCT e.zipl as zip, e.statefp, e.countyfp
                    FROM tiger_data.edges e
                    WHERE e.statefp = ''{state_fips}' AND e.zipl IS NOT NULL
                    UNION
                    SELECT DISTINCT e.zipr as zip, e.statefp, e.countyfp
                    FROM tiger_data.edges e
                    WHERE e.statefp = '{state_fips}' AND e.zipr IS NOT NULL; """)
        print(f"State tables created for {state_abbr.upper()}")

    def vacuum_analyze(self) -> None:
        """Run VACUUM ANALYZE on all tiger_data tables."""
        print("\nRunning VACUUM ANALYZE...")

        tables = self.pg_engine.query("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'tiger_data';
        """)

        for table in tables["table_name"].values:
            print(f"  Analyzing tiger_data.{table}...")
            self.pg_engine.execute(f"VACUUM ANALYZE tiger_data.{table};")
        print("VACUUM ANALYZE complete.")

    def test_geocoder(self, address: str) -> None:
        """Test the geocoder with a sample address."""
        print(f"\nTesting geocoder with: {address}")
        norm = self.pg_engine.query(f""" SELECT * FROM normalize_address('{address}'); """)
        print(f"Normalized: {norm}")

        results = self.pg_engine.query(f"""
            SELECT g.rating,
                   ST_X(g.geomout) as lon,
                   ST_Y(g.geomout) as lat,
                   pprint_addy(g.addy) as formatted_address
            FROM geocode('{address}') as g
            LIMIT 5; """)

        if results:
            print("Geocoding results:")
            for rating, lon, lat, formatted in results:
                print(f"  Rating: {rating}, ({lat}, {lon})")
                print(f"  Address: {formatted}")
        else:
            print("No geocoding results found.")


# # Example usage
# if __name__ == "__main__":
#     ingester = TIGERIngester(
#         data_dir="/data/tiger",
#         db_host="localhost",
#         db_port=5432,
#         db_name="geocoder",
#         db_user="postgres",
#         db_password="postgres",
#     )

#     # Run full ingestion
#     ingester.ingest_all()

#     # Optional: Create state-specific tables
#     # ingester.create_state_tables("06")  # California

#     # Run maintenance
#     ingester.vacuum_analyze()

#     # Test
#     ingester.test_geocoder("1600 Pennsylvania Avenue NW, Washington, DC 20500")

class TIGERLoader:
    LAYER_CONFIG = {
        "STATE": {"table": "state", "mode": "-a", "srid": "4269"},
        "COUNTY": {"table": "county", "mode": "-a", "srid": "4269"},
        "PLACE": {"table": "place", "mode": "-a", "srid": "4269"},
        "COUSUB": {"table": "cousub", "mode": "-a", "srid": "4269"},
        "TRACT": {"table": "tract", "mode": "-a", "srid": "4269"},
        "BG": {"table": "bg", "mode": "-a", "srid": "4269"},
        "TABBLOCK20": {"table": "tabblock20", "mode": "-a", "srid": "4269"},
        "EDGES": {"table": "edges", "mode": "-a", "srid": "4269"},
        "FACES": {"table": "faces", "mode": "-a", "srid": "4269"},
        "ADDR": {"table": "addr", "mode": "-a", "srid": "4269"},
        "FEATNAMES": {"table": "featnames", "mode": "-a", "srid": "4269"},
    }

    def __init__(self, data_dir: str | Path, pg_engine: PostgresEngine):
        self.data_dir = Path(data_dir)
        self.pg_engine = pg_engine

    def ingest_all_files(self):
        self.init_tracking_table()
        total_loaded = 0
        total_skipped = 0
        for layer in self.LAYER_CONFIG:
            layer_dir = self.data_dir.joinpath(layer)
            if not layer_dir.exists():
                continue
            print(f"\n=== Processing {layer} ===")
            for zip_path in sorted(layer_dir.glob("*.zip")):
                filename = zip_path.name
                if self.is_loaded(filename):
                    print(f"Skipping (already loaded): {filename}")
                    total_skipped += 1
                    continue
                print(f"Loading: {filename}")
                if self.process_zip(zip_path, layer):
                    self.mark_loaded(filename, layer)
                    total_loaded += 1
                else:
                    print(f"Failed: {filename}")
        print(f"\nDone. Loaded: {total_loaded}, Skipped: {total_skipped}")


    def init_tracking_table(self):
        self.pg_engine.execute("""
            create table if not exists tiger.loader_progress (
                filename text primary key,
                layer text,
                loaded_at timestamp default now()
            )
        """)

    def is_loaded(self, filename: str) -> bool:
        df = self.pg_engine.query(f"""
            select 1
            from tiger.loader_progress
            where filename = '{filename}'
            limit 1 """)
        return len(df) > 0

    def mark_loaded(self, filename: str, layer: str) -> None:
        self.pg_engine.execute(
            f"insert into tiger.loader_progress (filename, layer) values ({filename}, {layer})"
        )

    def process_zip(self, zip_path: Path, layer: str) -> bool:
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmpdir)
            shp_files = list(Path(tmpdir).glob("*.shp"))
            if not shp_files:
                print(f"No shapefile in {zip_path}")
                return False
            return self.load_shapefile(shp_files[0], layer)

    def load_shapefile(self, shp_path: Path, layer: str) -> bool:
        config = self.LAYER_CONFIG.get(layer)
        if not config:
            print(f"Unknown layer: {layer}")
            return False
        table = f"tiger.{config['table']}"
        srid = config["srid"]
        mode = config["mode"]
        cmd = ["shp2pgsql", mode, "-s", srid, "-W", "latin1", str(shp_path), table]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            sql = result.stdout
            self.pg_engine.execute(sql)
            return True
        except subprocess.CalledProcessError as e:
            print(f"shp2pgsql error: {e.stderr}")
            return False
        except psycopg2.Error as e:
            print(f"Database error: {e}")
            return False





class TIGERScriptLoader:
    FIPS_TO_ABBREV = {
        "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
        "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
        "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
        "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
        "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
        "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
        "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
        "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
        "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
        "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
        "56": "WY", "72": "PR", "78": "VI",
    }

    def __init__(
        self,
        data_dir: Path,
        conn_id: str,
        tiger_year: str = "2025",
        logger: Optional[logging.Logger] = None,
    ):
        self.data_dir = data_dir
        self.conn_id = conn_id
        self.tiger_year = tiger_year
        self.logger = logger
        self._conn_dict = self._extract_connection()

    def configure_loader_variables(self):
        self._run_sql(f"""
            UPDATE tiger.loader_variables SET
                staging_fold = '{self.data_dir}',
                tiger_year = '{self.tiger_year}'
        """)

    # def _preprocess_script(self, script: str) -> str:
    #     lines = []
    #     for line in script.split("\n"):
    #         if "wget " in line:
    #             continue
    #         if "unzip " in line:
    #             line = line.replace(
    #                 f"${{staging_fold}}/www2.census.gov/geo/tiger/TIGER{self.tiger_year}/",
    #                 "${staging_fold}/"
    #             )
    #         if line.strip().startswith("cd "):
    #             line = line.replace("/temp/", "/")
    #             line = line.replace(f"www2.census.gov/geo/tiger/TIGER{self.tiger_year}/", "")
    #         lines.append(line)
    #     return "\n".join(lines)

    def _preprocess_script(self, script: str) -> str:
        lines = []
        for line in script.split("\n"):
            stripped = line.strip()
            # Skip wget lines entirely
            if "wget " in line:
                continue
            # Skip lines that are just file:// URLs (leftover wget args)
            if stripped.startswith("file://"):
                continue
            # Fix unzip paths
            if "unzip " in line:
                line = line.replace(
                    f"${{staging_fold}}/www2.census.gov/geo/tiger/TIGER{self.tiger_year}/",
                    "${staging_fold}/"
                )
            # Fix cd paths - remove file:// prefix and census.gov path components
            if stripped.startswith("cd "):
                line = line.replace("file://", "")
                line = line.replace("/temp/", "/")
                line = line.replace(f"www2.census.gov/geo/tiger/TIGER{self.tiger_year}/", "")
                line = line.replace(f"/opt/airflow/raw_data/gisdata/opt/airflow", "/opt/airflow")
            lines.append(line)
        return "\n".join(lines)

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

    def _get_env(self) -> dict:
        """Return environment with PGPASSWORD set."""
        env = os.environ.copy()
        env["PGPASSWORD"] = self._conn_dict["password"]
        return env

    def _psql_cmd(self) -> list[str]:
        """Base psql command with connection params."""
        return [
            "psql",
            "-h", self._conn_dict["host"],
            "-p", str(self._conn_dict["port"]),
            "-d", self._conn_dict["database"],
            "-U", self._conn_dict["user"],
        ]

    def _run_sql(self, sql: str):
        """Execute SQL via psql."""
        cmd = self._psql_cmd() + ["-c", sql]
        subprocess.run(cmd, env=self._get_env(), check=True)

    def _run_sql_query(self, sql: str) -> str:
        """Execute SQL and return result."""
        cmd = self._psql_cmd() + ["-t", "-A", "-c", sql]
        result = subprocess.run(cmd, env=self._get_env(), capture_output=True, text=True, check=True)
        return result.stdout.strip()

    def _log(self, msg: str):
        if self.logger:
            self.logger.info(msg)
        else:
            print(msg)

    def generate_nation_script(self) -> str:
        """Generate nation loader script."""
        return self._run_sql_query("SELECT loader_generate_nation_script('sh')")

    def generate_state_script(self, state_fips_list: list[str]) -> str:
        """Generate state loader script."""
        abbrevs = [self.FIPS_TO_ABBREV[f] for f in state_fips_list]
        abbrev_array = "ARRAY[" + ",".join(f"'{a}'" for a in abbrevs) + "]"
        return self._run_sql_query(f"SELECT loader_generate_script({abbrev_array}, 'sh')")

    def run_script(self, script: str):
        """Write script to temp file and execute with credentials in env."""
        script = self._preprocess_script(script)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write(script)
            script_path = f.name

        # Debug: print script to see what's running
        self._log(f"Script contents:\n{script[:2000]}")

        try:
            subprocess.run(["bash", script_path], env=self._get_env(), check=True)
        finally:
            os.unlink(script_path)

    def load_nation(self):
        """Load nation-level tables."""
        self._log("=== Loading nation layers ===")
        self.configure_loader_variables()
        script = self.generate_nation_script()
        self.run_script(script)

    def load_states(self, state_fips_list: list[str]):
        """Load state-level tables."""
        abbrevs = [self.FIPS_TO_ABBREV[f] for f in state_fips_list]
        self._log(f"=== Loading states: {', '.join(abbrevs)} ===")
        script = self.generate_state_script(state_fips_list)
        self.run_script(script)

    def load_all(self, state_fips_list: list[str] = None):
        """Full load: nation then states."""
        if state_fips_list is None:
            state_fips_list = list(self.FIPS_TO_ABBREV.keys())

        self.load_nation()
        self.load_states(state_fips_list)

@dag(
    schedule="0 20 20 1 *",
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["mock_data"],
)
def download_and_ingest_tiger_data():

    @task
    def download_tiger_data() -> bool:
        data_dir=Path("/opt/airflow/raw_data/gisdata")
        tiger_downloader = TIGERDownloader(data_dir=data_dir, year="2025")
        tiger_downloader.download_data()
        task_logger.info(f"Downloaded TIGER Data ")
        return True

    @task
    def load_tiger_data() -> bool:
        data_dir=Path("/opt/airflow/raw_data/gisdata")
        pg_engine = get_postgres_engine(conn_id="gis_dwh_db", logger=task_logger)
        tiger_loader = TIGERIngester(conn_id="gis_dwh_db", data_dir=data_dir, pg_engine=pg_engine)
        tiger_loader.ingest_all()

        # tiger_loader = TIGERScriptLoader(
        #     data_dir=Path("/opt/airflow/raw_data/gisdata"),
        #     conn_id="gis_dwh_db",
        #     logger=task_logger,
        # )
        # tiger_loader.load_all()
        task_logger.info(f"Loaded TIGER Data ")
        return True

    get_data = download_tiger_data()
    ingest_data = load_tiger_data()
    chain(get_data, ingest_data)


download_and_ingest_tiger_data()
