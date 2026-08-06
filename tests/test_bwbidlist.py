import sqlite3
import zipfile
from pathlib import Path
from unittest.mock import patch

from src.bwbidlist import _parse_bwbidlist_xml, build_law_aliases

SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<BWBIdServiceResult xmlns="http://schemas.overheid.nl/bwbidservice">
  <RegelingInfoLijst>
    <RegelingInfo>
      <BWBId>BWBR0005289</BWBId>
      <OfficieleTitel>Burgerlijk Wetboek Boek 6</OfficieleTitel>
      <CiteertitelLijst>
        <Citeertitel><titel>Burgerlijk Wetboek</titel></Citeertitel>
      </CiteertitelLijst>
      <AfkortingLijst>
        <Afkorting>BW</Afkorting>
      </AfkortingLijst>
      <NietOfficieleTitelLijst>
        <NietOfficieleTitel>BW6</NietOfficieleTitel>
      </NietOfficieleTitelLijst>
    </RegelingInfo>
  </RegelingInfoLijst>
</BWBIdServiceResult>
"""


def test_parse_bwbidlist_xml_extracts_all_alias_kinds(tmp_path: Path):
    xml_path = tmp_path / "BWBIdList.xml"
    xml_path.write_text(SAMPLE_XML, encoding="utf-8")

    result = _parse_bwbidlist_xml(xml_path)

    assert len(result) == 1
    bwb_id, aliases = result[0]
    assert bwb_id == "BWBR0005289"
    assert aliases == ["Burgerlijk Wetboek Boek 6", "Burgerlijk Wetboek", "BW", "BW6"]


def test_build_law_aliases_extracts_zip_member_correctly(tmp_path: Path):
    """Regression test: zip_path.with_suffix(".xml") on "BWBIdList.xml.zip"
    used to produce "BWBIdList.xml.xml" (with_suffix only replaces the last
    suffix), not the "BWBIdList.xml" the zip actually contains -- silently
    failing every real run with a file-not-found error post-download."""
    zip_path = tmp_path / "BWBIdList.xml.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("BWBIdList.xml", SAMPLE_XML)

    db_path = tmp_path / "lido.db"

    with patch("src.bwbidlist.download_bwbidlist") as mock_download:
        written = build_law_aliases(db_path, zip_path)
    mock_download.assert_called_once_with(zip_path)

    assert written == 4  # 4 aliases for the one regeling in SAMPLE_XML

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT alias, bwb_id, source FROM law_alias ORDER BY alias").fetchall()
    conn.close()
    assert rows == [
        ("BW", "BWBR0005289", "bwbidlist"),
        ("BW6", "BWBR0005289", "bwbidlist"),
        ("Burgerlijk Wetboek", "BWBR0005289", "bwbidlist"),
        ("Burgerlijk Wetboek Boek 6", "BWBR0005289", "bwbidlist"),
    ]
