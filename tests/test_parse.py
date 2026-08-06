import sqlite3
from pathlib import Path

import pytest

from src.parse import (
    LAW_SUBJECT_MARKER,
    _LawIndex,
    _parse_referentie,
    _subject_to_law_row,
    resolve_case_law_links,
)

# Real sample values captured from a built lido.db this session.
REF_BWB = (
    "linktype=http://linkeddata.overheid.nl/terms/linktype/id/lx-referentie"
    "|target=bwb|uri=jci1.3:c:BWBR0005288&boek=5&titeldeel=1&artikel=1&z=2024-01-01&g=2024-01-01"
    "|lido-id=http://linkeddata.overheid.nl/terms/bwb/id/BWBR0005288/1723924/1992-01-01/1992-01-01"
    "|opschrift=artikel 5:1 BW"
)
REF_ECLI = (
    "linktype=http://linkeddata.overheid.nl/terms/linktype/id/lx-referentie"
    "|target=ecli|uri=ECLI:NL:HR:2011:BQ7597"
    "|lido-id=http://linkeddata.overheid.nl/terms/jurisprudentie/id/ECLI:NL:HR:2011:BQ7597"
    "|opschrift=ECLI:NL:HR:2011:BQ7597"
)
REF_CELEX = (
    "linktype=http://linkeddata.overheid.nl/terms/linktype/id/lx-referentie"
    "|target=celex|uri=CELEX:32014L0041"
    "|lido-id=http://linkeddata.overheid.nl/terms/eu-regelgeving/id/32014L0041"
    "|opschrift=Richtlijn 2014/41/EU"
)
REF_PROCEDURAL = (
    "linktype=http://linkeddata.overheid.nl/terms/linktype/id/rvr-hogerberoep-eerdereaanleg"
    "|target=jurisprudentie|uri=ECLI:NL:RBROT:2021:6457"
    "|type=hogerBeroep|aanleg=eerdereAanleg|gevolg="
)


def test_parse_referentie_splits_bwb_reference():
    ref = _parse_referentie(REF_BWB)
    assert ref["target"] == "bwb"
    assert ref["opschrift"] == "artikel 5:1 BW"
    assert ref["lido-id"] == (
        "http://linkeddata.overheid.nl/terms/bwb/id/BWBR0005288/1723924/1992-01-01/1992-01-01"
    )


def test_parse_referentie_splits_ecli_reference():
    ref = _parse_referentie(REF_ECLI)
    assert ref["target"] == "ecli"
    assert ref["uri"] == "ECLI:NL:HR:2011:BQ7597"


def test_parse_referentie_handles_procedural_relation_shape_without_crashing():
    ref = _parse_referentie(REF_PROCEDURAL)
    assert ref["target"] == "jurisprudentie"
    assert ref.get("lido-id") is None
    assert ref.get("opschrift") is None


def test_subject_to_law_row_extracts_artikel():
    subject = (
        "http://linkeddata.overheid.nl/terms/bwb/id/BWBR0005289/2963064/2020-01-01/2020-01-01"
    )
    triples = {
        "rdf_type": ["http://linkeddata.overheid.nl/terms/Artikel"],
        "title_dcterms": ["Artikel 27"],
        "jc_id_candidates": ["jci1.3:c:BWBR0005289&artikel=27"],
        "number": ["27"],
    }
    row = _subject_to_law_row(subject, triples)
    assert row == (
        "artikel",
        "BWBR0005289",
        "2963064",
        "BWBR0005289/2963064/2020-01-01/2020-01-01",
        "jci1.3:c:BWBR0005289&artikel=27",
        "27",
        "Artikel 27",
    )


def test_subject_to_law_row_falls_back_through_title_predicates():
    subject = "http://linkeddata.overheid.nl/terms/bwb/id/BWBR0005289/0/2020-01-01/2020-01-01"
    triples = {
        "rdf_type": ["http://linkeddata.overheid.nl/terms/Wet"],
        "title_rdfs": ["Burgerlijk Wetboek"],
    }
    row = _subject_to_law_row(subject, triples)
    assert row[0] == "wet"
    assert row[6] == "Burgerlijk Wetboek"


def test_subject_to_law_row_returns_none_for_unrecognized_type():
    subject = "http://linkeddata.overheid.nl/terms/bwb/id/BWBR0005289/0/2020-01-01/2020-01-01"
    triples = {"rdf_type": ["http://linkeddata.overheid.nl/terms/SomeOtherType"]}
    assert _subject_to_law_row(subject, triples) is None


def test_subject_to_law_row_returns_none_for_non_law_subject():
    subject = "http://linkeddata.overheid.nl/terms/jurisprudentie/id/ECLI:NL:HR:2020:1"
    assert LAW_SUBJECT_MARKER not in subject
    assert _subject_to_law_row(subject, {"rdf_type": ["http://linkeddata.overheid.nl/terms/Wet"]}) is None


@pytest.fixture
def db_conn(tmp_path: Path):
    db_path = tmp_path / "lido.db"
    conn = sqlite3.connect(db_path)
    schema_sql = (Path(__file__).parent.parent / "src" / "schema.sql").read_text()
    conn.executescript(schema_sql)
    yield conn
    conn.close()


def test_law_index_matches_by_exact_lido_id(db_conn):
    db_conn.execute(
        "INSERT INTO law_element (type, bwb_id, bwb_label_id, lido_id, title) "
        "VALUES ('artikel', 'BWBR0005288', '1723924', "
        "'BWBR0005288/1723924/1992-01-01/1992-01-01', 'Artikel 5:1')"
    )
    matched = _LawIndex(db_conn).resolve(
        "http://linkeddata.overheid.nl/terms/bwb/id/BWBR0005288/1723924/1992-01-01/1992-01-01",
    )
    assert matched == "BWBR0005288/1723924/1992-01-01/1992-01-01"


def test_law_index_falls_back_to_bwb_id_only(db_conn):
    db_conn.execute(
        "INSERT INTO law_element (type, bwb_id, bwb_label_id, lido_id, title) "
        "VALUES ('wet', 'BWBR0005288', NULL, 'BWBR0005288/0/2020-01-01/2020-01-01', 'BW')"
    )
    matched = _LawIndex(db_conn).resolve(
        "http://linkeddata.overheid.nl/terms/bwb/id/BWBR0005288/9999999/2024-01-01/2024-01-01",
    )
    assert matched == "BWBR0005288/0/2020-01-01/2020-01-01"


def test_law_index_returns_none_for_ecli_lido_id(db_conn):
    assert _LawIndex(db_conn).resolve(
        "http://linkeddata.overheid.nl/terms/jurisprudentie/id/ECLI:NL:HR:2020:1"
    ) is None


def test_resolve_case_law_links_end_to_end(db_conn):
    db_conn.execute(
        "INSERT INTO law_element (type, bwb_id, bwb_label_id, lido_id, title) "
        "VALUES ('artikel', 'BWBR0005288', '1723924', "
        "'BWBR0005288/1723924/1992-01-01/1992-01-01', 'Artikel 5:1')"
    )
    db_conn.execute(
        "INSERT INTO metadata (ecli, citations_outgoing, legislations_cited) VALUES (?, ?, ?)",
        (
            "ECLI:NL:GHDHA:2025:1739",
            "\n".join([REF_BWB, REF_ECLI, REF_CELEX, REF_PROCEDURAL]),
            "",
        ),
    )
    db_conn.commit()

    written = resolve_case_law_links(db_conn)
    assert written == 1  # only the bwb entry resolves against law_element

    case_law = db_conn.execute(
        "SELECT ecli, law_lido_id, source, opschrift FROM case_law"
    ).fetchall()
    assert case_law == [
        (
            "ECLI:NL:GHDHA:2025:1739",
            "BWBR0005288/1723924/1992-01-01/1992-01-01",
            "lido-ref",
            "artikel 5:1 BW",
        )
    ]

    citations_outgoing = db_conn.execute(
        "SELECT citations_outgoing FROM metadata WHERE ecli = 'ECLI:NL:GHDHA:2025:1739'"
    ).fetchone()[0]
    assert citations_outgoing == "ECLI:NL:HR:2011:BQ7597"  # celex/procedural/bwb all dropped


def test_resolve_case_law_links_is_resumable(db_conn):
    """An interrupted run leaves some rows already cleaned (no '|') and others
    still raw; rerunning must skip the former (an already-clean value has no
    trace of what was dropped, so reprocessing it would wrongly wipe it) and
    still process the latter."""
    db_conn.execute(
        "INSERT INTO law_element (type, bwb_id, bwb_label_id, lido_id, title) "
        "VALUES ('artikel', 'BWBR0005288', '1723924', "
        "'BWBR0005288/1723924/1992-01-01/1992-01-01', 'Artikel 5:1')"
    )
    # Already processed by a prior (interrupted) run: clean, no pipes.
    db_conn.execute(
        "INSERT INTO metadata (ecli, citations_outgoing, legislations_cited) VALUES (?, ?, ?)",
        ("ECLI:NL:HR:2020:1", "ECLI:NL:HR:2011:BQ7597", ""),
    )
    # Not yet processed: still raw.
    db_conn.execute(
        "INSERT INTO metadata (ecli, citations_outgoing, legislations_cited) VALUES (?, ?, ?)",
        ("ECLI:NL:HR:2020:2", REF_BWB, ""),
    )
    db_conn.commit()

    written = resolve_case_law_links(db_conn)
    assert written == 1  # only the still-raw row's bwb entry gets resolved

    rows = dict(
        db_conn.execute("SELECT ecli, citations_outgoing FROM metadata ORDER BY ecli").fetchall()
    )
    assert rows["ECLI:NL:HR:2020:1"] == "ECLI:NL:HR:2011:BQ7597"  # untouched, not wiped
    assert rows["ECLI:NL:HR:2020:2"] == ""  # REF_BWB has no target=ecli entry
