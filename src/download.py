"""Stream-download lido-export.ttl.gz with a progress bar."""

from __future__ import annotations

import sys
from pathlib import Path

import requests
from tqdm import tqdm

LIDO_URL = "https://linkeddata.overheid.nl/export/lido-export.ttl.gz"


def download(dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(LIDO_URL, stream=True, timeout=60) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0)) or None

        with (
            open(dest, "wb") as fh,
            tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=dest.name,
            ) as bar,
        ):
            for chunk in response.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
                bar.update(len(chunk))

    print(f"Downloaded {dest}", file=sys.stderr)
