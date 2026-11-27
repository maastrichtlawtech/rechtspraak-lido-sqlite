import gzip
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from src.parse import parse_into_db

CASE = "<http://linkeddata.overheid.nl/terms/jurisprudentie/id/ECLI:NL:GHSHE:2016:2829>"
T = "http://linkeddata.overheid.nl/terms/"

# Lines copied verbatim from the November 2026 lido-export.nt.gz, in file order
# (the CELEX identifier really does precede the ECLI one), plus one broken line.
NT_LINES = [
    f"{CASE} <http://purl.org/dc/terms/creator> <http://standaarden.overheid.nl/owms/terms/Gerechtshof_'s-Hertogenbosch> .",
    f'{CASE} <http://purl.org/dc/terms/identifier> "CELEX:82016NL0712(51)" .',
    f'{CASE} <http://purl.org/dc/terms/identifier> "ECLI:NL:GHSHE:2016:2829" .',
    f"{CASE} <{T}heeftBron> <{T}bron/id/rvr-jurisprudentie> .",
    f'{CASE} <{T}heeftProceduresoort> "Hoger beroep" .',
    f'{CASE} <{T}heeftUitspraakdatum> "2016-07-12"^^<http://www.w3.org/2001/XMLSchema#date> .',
    f'{CASE} <{T}heeftZaaknummer> "200.179.500_01" .',
    "<http://broken line without a terminating dot",
    f'{CASE} <http://purl.org/dc/terms/spatial> "\'s-Hertogenbosch" .',
]


@pytest.fixture
def nt_gz(tmp_path: Path) -> Path:
    path = tmp_path / "lido-export.nt.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(NT_LINES) + "\n")
    return path


def _metadata(db_path: Path) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM metadata")]
    conn.close()
    return {r["ecli"]: r for r in rows}


def test_ntriples_export_builds_case_rows_without_a_turtle_converter(nt_gz: Path, tmp_path: Path):
    db_path = tmp_path / "lido.db"

    # serdi/rapper are only needed for Turtle; pretend none are installed.
    with patch("src.parse.shutil.which", return_value=None):
        parse_into_db(nt_gz, db_path)

    rows = _metadata(db_path)
    assert list(rows) == ["ECLI:NL:GHSHE:2016:2829"]
    row = rows["ECLI:NL:GHSHE:2016:2829"]
    assert row["date_decision"] == "2016-07-12"
    assert row["procedure"] == "Hoger beroep"
    assert row["zaaknummer"] == "200.179.500_01"
    assert row["spatial"] == "'s-Hertogenbosch"
