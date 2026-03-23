import tempfile
from pathlib import Path


def make_temp_dir(prefix: str) -> tuple[Path, tempfile.TemporaryDirectory]:
    """Create a temporary directory and return its path and handle.

    The directory exists as long as the returned handle is alive.
    When the handle is garbage-collected or explicitly closed, the
    directory and its contents are deleted.

    Parameters
    ----------
    prefix : str
        Prefix for the temporary directory name.

    Returns
    -------
    tuple of (Path, TemporaryDirectory)
        The path to the directory, and the handle that controls its lifespan.
    """
    handle = tempfile.TemporaryDirectory(prefix=prefix)
    return Path(handle.name), handle
