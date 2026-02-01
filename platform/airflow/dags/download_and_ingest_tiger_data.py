import datetime as dt
import logging
import shutil
import time
import urllib.request
from pathlib import Path
from typing import Optional

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain

from db.core import get_postgres_engine, get_mysql_engine
from datagen.data_model_mocker import DataFaker

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

    get_data = download_tiger_data()
    chain(get_data)


download_and_ingest_tiger_data()
