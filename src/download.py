"""Stream-download lido-export.nt.gz with a progress bar."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import requests
from tqdm import tqdm

# The monthly LiDO dump, as listed on
# https://data.overheid.nl/dataset/linked-data-overheid.  Until late 2026 it
# was published as lido-export.ttl.gz; that path now returns an HTML error page
# with HTTP 200.
LIDO_URL = "https://linkeddata.overheid.nl/export/lido-export.nt.gz"

GZIP_MAGIC = b"\x1f\x8b"
RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 30  # seconds; doubles on every retry


def download(dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            _download_once(dest)
            break
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in RETRY_STATUSES or attempt == MAX_ATTEMPTS:
                raise
            delay = RETRY_BASE_DELAY * 2 ** (attempt - 1)
            print(f"HTTP {status} from {LIDO_URL}; retrying in {delay}s …", file=sys.stderr)
            time.sleep(delay)

    print(f"Downloaded {dest}", file=sys.stderr)


def _download_once(dest: Path) -> None:
    tmp = dest.with_name(dest.name + ".part")
    try:
        with requests.get(LIDO_URL, stream=True, timeout=60) as response:
            response.raise_for_status()
            total = int(response.headers.get("content-length", 0)) or None

            with (
                open(tmp, "wb") as fh,
                tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=dest.name,
                ) as bar,
            ):
                first = True
                for chunk in response.iter_content(chunk_size=1 << 20):
                    if first:
                        _check_gzip(chunk)
                        first = False
                    fh.write(chunk)
                    bar.update(len(chunk))
        tmp.replace(dest)
    finally:
        tmp.unlink(missing_ok=True)


def _check_gzip(first_chunk: bytes) -> None:
    # The server answers missing files with HTTP 200 and an HTML error page, so
    # the status code alone cannot be trusted.
    if not first_chunk.startswith(GZIP_MAGIC):
        preview = first_chunk[:200].decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{LIDO_URL} returned something that is not a gzip file; the export "
            f"may have moved (check https://data.overheid.nl/dataset/linked-data-overheid).\n"
            f"Response starts with: {preview!r}"
        )
