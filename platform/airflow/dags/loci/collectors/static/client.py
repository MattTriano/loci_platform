"""
StaticFileClient — downloads and parses static published files.

Deliberately dumb: it knows how to fetch bytes politely and turn CSV/XLSX
bytes into rows of strings, and nothing else. Everything source-specific
(which URLs, which sheet, which delimiter) lives on the spec's FileRefs.

Bot-defense note: some publishers (e.g. ahrq.gov) sit behind AWS WAF
JavaScript challenges that this client cannot solve. The `cookies`
parameter exists so a manually obtained token (e.g. aws-waf-token copied
from a browser) can be injected; if that's too fragile, FileRef supports
file:// URLs for manually downloaded copies. No WAF logic lives here.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from collections.abc import Iterator
from urllib.parse import urlparse
from urllib.request import url2pathname

import requests
from loci.collectors.static.spec import FileRef

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:151.0) Gecko/20100101 Firefox/151.0"
)


class StaticFileDownloadError(RuntimeError):
    """A download failed or returned something other than the expected file."""


def sanitize_column_name(name: str) -> str:
    """Normalize a source column name for the warehouse: lowercase, runs of
    non-alphanumerics collapsed to single underscores."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return cleaned or "unnamed"


class StaticFileClient:
    """
    Parameters
    ----------
    user_agent : str
        Sent on every request. Defaults to an honest, identified UA, which
        is the polite norm for government data sites; swap in a browser UA
        only if the honest one gets challenged.
    cookies : dict[str, str] | None
        Cookies to preload into the session (e.g. a manually obtained
        aws-waf-token). If using a browser-minted WAF token, set
        user_agent to match the browser it came from.
    delay_seconds : float
        Minimum spacing between HTTP requests.
    timeout : int
        Per-request timeout in seconds.
    max_attempts : int
        Attempts per URL before giving up (connection errors and 5xx
        retry with exponential backoff; 4xx fails immediately).
    """

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        cookies: dict[str, str] | None = None,
        delay_seconds: float = 1.0,
        timeout: int = 60,
        max_attempts: int = 3,
    ):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        for name, value in (cookies or {}).items():
            self.session.cookies.set(name, value)
        self.delay_seconds = delay_seconds
        self.timeout = timeout
        self.max_attempts = max_attempts
        self._last_request_at = 0.0

    # ------------------------------------------------------------------
    # Downloading
    # ------------------------------------------------------------------

    def download(self, url: str) -> bytes:
        """Return the raw bytes at `url`. Supports https:// and file://."""
        parsed = urlparse(url)
        if parsed.scheme == "file":
            path = url2pathname(parsed.path)
            logger.info("Reading local file %s", path)
            with open(path, "rb") as f:
                return f.read()

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            try:
                response = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = exc
                logger.warning(
                    "Attempt %d/%d for %s failed: %s", attempt, self.max_attempts, url, exc
                )
                time.sleep(2**attempt)
                continue

            if response.status_code >= 500:
                last_error = StaticFileDownloadError(f"HTTP {response.status_code} for {url}")
                logger.warning(
                    "Attempt %d/%d for %s: HTTP %d",
                    attempt,
                    self.max_attempts,
                    url,
                    response.status_code,
                )
                time.sleep(2**attempt)
                continue

            response.raise_for_status()
            self._raise_if_html(url, response)
            logger.info("Downloaded %s (%d bytes)", url, len(response.content))
            return response.content

        raise StaticFileDownloadError(
            f"Giving up on {url} after {self.max_attempts} attempts"
        ) from last_error

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)
        self._last_request_at = time.monotonic()

    @staticmethod
    def _raise_if_html(url: str, response: requests.Response) -> None:
        """A data URL returning HTML almost always means a bot-defense
        challenge page or an error page — never parse it as data."""
        content_type = response.headers.get("Content-Type", "")
        head = response.content[:512].lstrip().lower()
        if "text/html" in content_type or head.startswith((b"<!doctype", b"<html")):
            raise StaticFileDownloadError(
                f"Expected a data file but got an HTML page from {url} "
                f"(Content-Type: {content_type!r}). This is likely a WAF "
                "challenge or an expired token; see the module docstring."
            )

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def iter_rows(self, file_ref: FileRef) -> Iterator[dict[str, str]]:
        """Download and parse one file, yielding rows as dicts of strings
        keyed by sanitized column names."""
        data = self.download(file_ref.url)
        if file_ref.file_format == "csv":
            yield from self._iter_csv(data, file_ref)
        else:
            yield from self._iter_xlsx(data, file_ref)

    @staticmethod
    def _iter_csv(data: bytes, file_ref: FileRef) -> Iterator[dict[str, str]]:
        text = io.TextIOWrapper(io.BytesIO(data), encoding=file_ref.encoding, newline="")
        for _ in range(file_ref.skip_rows):
            text.readline()
        reader = csv.DictReader(text, delimiter=file_ref.delimiter)
        for raw in reader:
            yield {
                sanitize_column_name(key): (value or "").strip()
                for key, value in raw.items()
                if key is not None  # DictReader uses key None for overflow cells
            }

    @staticmethod
    def _iter_xlsx(data: bytes, file_ref: FileRef) -> Iterator[dict[str, str]]:
        import openpyxl  # imported lazily; only needed for xlsx manifests

        workbook = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        if isinstance(file_ref.sheet, int):
            sheet = workbook.worksheets[file_ref.sheet]
        else:
            sheet = workbook[file_ref.sheet]

        rows = sheet.iter_rows(min_row=file_ref.skip_rows + 1, values_only=True)
        header_cells = next(rows, None)
        if header_cells is None:
            return
        columns = [sanitize_column_name(str(c)) for c in header_cells if c is not None]

        for cells in rows:
            yield {col: _cell_to_str(value) for col, value in zip(columns, cells, strict=True)}
        workbook.close()


def _cell_to_str(value) -> str:
    """Render an openpyxl cell value as text without Excel artifacts:
    integral floats lose the trailing '.0' Excel gives them, dates become
    ISO 8601, None becomes empty string."""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value).strip()
