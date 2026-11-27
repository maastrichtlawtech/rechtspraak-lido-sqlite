import gzip
from pathlib import Path
from unittest.mock import patch

import pytest

from src import download as download_mod
from src.download import LIDO_URL, download

# What linkeddata.overheid.nl actually served (with HTTP 200) for the removed
# lido-export.ttl.gz URL in November 2026.
HTML_ERROR_PAGE = (
    b'<!DOCTYPE html\n SYSTEM "about:legacy-compat">\n'
    b"<html><body>/export/lido-export.ttl.gz (No such file or directory)"
    b"</body></html>"
)


class FakeResponse:
    def __init__(self, status_code: int, body: bytes, headers: dict | None = None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {"content-length": str(len(body))}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def iter_content(self, chunk_size: int):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def fake_get(responses: list[FakeResponse], calls: list[str]):
    def _get(url, **kwargs):
        calls.append(url)
        return responses.pop(0)

    return _get


@pytest.fixture(autouse=True)
def no_sleep():
    with patch.object(download_mod.time, "sleep"):
        yield


def test_download_fetches_the_ntriples_export(tmp_path: Path):
    body = gzip.compress(b"<http://e/s> <http://e/p> <http://e/o> .\n")
    calls: list[str] = []
    dest = tmp_path / "lido-export.nt.gz"

    with patch("requests.get", fake_get([FakeResponse(200, body)], calls)):
        download(dest)

    assert calls == ["https://linkeddata.overheid.nl/export/lido-export.nt.gz"]
    assert LIDO_URL == calls[0]
    assert dest.read_bytes() == body


def test_download_rejects_html_error_page_served_with_200(tmp_path: Path):
    dest = tmp_path / "lido-export.nt.gz"

    with patch("requests.get", fake_get([FakeResponse(200, HTML_ERROR_PAGE)], [])):
        with pytest.raises(RuntimeError, match="not a gzip file"):
            download(dest)

    assert not dest.exists()


def test_download_retries_rate_limit_and_gateway_errors(tmp_path: Path):
    body = gzip.compress(b"<http://e/s> <http://e/p> <http://e/o> .\n")
    calls: list[str] = []
    responses = [FakeResponse(429, b"slow down"), FakeResponse(504, b"timeout"), FakeResponse(200, body)]
    dest = tmp_path / "lido-export.nt.gz"

    with patch("requests.get", fake_get(responses, calls)):
        download(dest)

    assert len(calls) == 3
    assert dest.read_bytes() == body


def test_download_gives_up_after_repeated_server_errors(tmp_path: Path):
    responses = [FakeResponse(503, b"down") for _ in range(10)]
    calls: list[str] = []
    dest = tmp_path / "lido-export.nt.gz"

    with patch("requests.get", fake_get(responses, calls)):
        with pytest.raises(Exception):
            download(dest)

    assert 1 < len(calls) < 10
    assert not dest.exists()


def test_download_does_not_retry_not_found(tmp_path: Path):
    calls: list[str] = []
    dest = tmp_path / "lido-export.nt.gz"

    with patch("requests.get", fake_get([FakeResponse(404, b"nope")], calls)):
        with pytest.raises(Exception):
            download(dest)

    assert len(calls) == 1
