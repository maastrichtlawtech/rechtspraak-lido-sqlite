"""Download and parse BWBIdList.xml.zip into law_alias rows.

A separate data source from lido-export.ttl.gz -- this gives alias titles for
BWB-ids (e.g. "BW" -> BWBR0005289), used to construct human-readable
citations. Ported from case-law-explorer's airflow/dags/lido/tasks/
prepare_bwbidlist.py + bwbidlist_to_sqlite.py, which used to run this as part
of a second, independent DAG/database.
"""

from __future__ import annotations

import re
import sqlite3
import sys
import zipfile
from pathlib import Path

import requests
from lxml import etree

BWB_IDS_ZIP_URL = "https://zoekservice.overheid.nl/BWBIdService/BWBIdList.xml.zip"

_NS = {"NS1": "http://schemas.overheid.nl/bwbidservice"}


def download_bwbidlist(dest_zip: Path) -> None:
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(BWB_IDS_ZIP_URL, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(dest_zip, "wb") as fh:
            for chunk in response.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    print(f"Downloaded {dest_zip}", file=sys.stderr)


def _parse_bwbidlist_xml(xml_path: Path) -> list[tuple[str, list[str]]]:
    """Return [(bwb_id, [alias, ...]), ...]."""
    output = []
    root = etree.parse(str(xml_path))
    regeling_lijst = root.find("NS1:RegelingInfoLijst", namespaces=_NS)
    for regeling in regeling_lijst.iterfind("NS1:RegelingInfo", namespaces=_NS):
        bwb_id = regeling.find("NS1:BWBId", namespaces=_NS).text
        titel_officieel = regeling.find("NS1:OfficieleTitel", namespaces=_NS).text

        citeer_titels = [
            titel.find("NS1:titel", namespaces=_NS).text
            for titel in regeling.find("NS1:CiteertitelLijst", namespaces=_NS).iterfind(
                "NS1:Citeertitel", namespaces=_NS
            )
        ]
        afkortingen = [
            afkorting.text
            for afkorting in regeling.find("NS1:AfkortingLijst", namespaces=_NS).iterfind(
                "NS1:Afkorting", namespaces=_NS
            )
        ]
        titels_niet_officieel = [
            titel.text
            for titel in regeling.find("NS1:NietOfficieleTitelLijst", namespaces=_NS).iterfind(
                "NS1:NietOfficieleTitel", namespaces=_NS
            )
        ]

        titels = [titel_officieel] + citeer_titels + afkortingen + titels_niet_officieel
        titels = [
            re.sub(r"\s+", " ", re.sub(r"^(\s|[.,;:])+", "", titel))
            for titel in titels
            if titel and 0 < len(titel) < 2500
        ]

        output.append((bwb_id, titels))

    return output


def build_law_aliases(db_path: Path, zip_path: Path) -> int:
    """Download+parse BWBIdList.xml.zip and insert its aliases into
    law_alias in db_path. Returns the number of aliases written."""
    download_bwbidlist(zip_path)

    xml_path = zip_path.parent / "BWBIdList.xml"
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extract("BWBIdList.xml", xml_path.parent)

    conn = sqlite3.connect(db_path)
    schema_sql = (Path(__file__).parent / "schema.sql").read_text()
    conn.executescript(schema_sql)

    cursor = conn.cursor()
    written = 0
    for bwb_id, aliases in _parse_bwbidlist_xml(xml_path):
        for alias in aliases:
            cursor.execute(
                "INSERT OR IGNORE INTO law_alias (alias, bwb_id, source) VALUES (?, ?, 'bwbidlist')",
                (alias, bwb_id),
            )
            written += 1
    conn.commit()
    conn.close()

    print(f"Inserted {written:,} law alias rows into {db_path}", file=sys.stderr)
    return written
