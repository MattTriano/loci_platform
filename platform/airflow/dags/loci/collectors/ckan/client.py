import requests

from loci.collectors.ckan.metadata import CKANMetadata


class CKANClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.metadata = CKANMetadata(self.base_url)

    def download_resource(self, resource_url: str, dest_path: str) -> str:
        """Download a resource file from its URL and save it to dest_path.

        Returns the path the file was saved to.
        """
        response = requests.get(resource_url, timeout=60, stream=True)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return dest_path
