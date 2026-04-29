"""Stream-parse lido-export.ttl.gz and insert case metadata into SQLite."""

from __future__ import annotations

import gzip
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyoxigraph
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Predicate → column mapping
# ---------------------------------------------------------------------------

# Subjects of interest: http://deeplink.rechtspraak.nl/uitspraak?id={ECLI}
CASE_SUBJECT_MARKER = "deeplink.rechtspraak.nl/uitspraak"

# Multi-valued predicates: values are joined with "\n" (matching GROUP_CONCAT separator
# used by the SPARQL query in rechtspraak-extractor).
# Single-valued predicates: last seen value wins (equivalent to SPARQL MAX).
MULTI_VALUE_PREDICATES: set[str] = {
    "http://purl.org/dc/terms/creator",
    "http://purl.org/dc/terms/hasVersion",
    "http://purl.org/dc/terms/relation",
    "http://purl.org/dc/terms/references",
    "http://purl.org/dc/terms/subject",
    "http://psi.rechtspraak.nl/zaaknummer",
    "http://psi.rechtspraak.nl/procedure",
}

# Maps predicate URI → metadata column name.
# A predicate listed under MULTI_VALUE_PREDICATES will accumulate a list;
# all others are treated as single-valued.
PREDICATE_MAP: dict[str, str] = {
    "http://purl.org/dc/terms/identifier":      "ecli",
    "http://purl.org/dc/terms/creator":         "instance",
    "http://purl.org/dc/terms/date":            "date_decision",
    "http://purl.org/dc/terms/issued":          "date_publication",
    "http://purl.org/dc/terms/type":            "document_type",
    "http://purl.org/dc/terms/language":        "language",
    "http://purl.org/dc/terms/spatial":         "jurisdiction_city",
    "http://purl.org/dc/terms/title":           "title",
    "http://purl.org/dc/terms/description":     "info",
    "http://purl.org/dc/terms/hasVersion":      "alternative_publications",
    "http://purl.org/dc/terms/relation":        "citing",
    "http://purl.org/dc/terms/references":      "legislations_cited",
    "http://purl.org/dc/terms/subject":         "domains",
    "http://psi.rechtspraak.nl/zaaknummer":     "case_number",
    "http://psi.rechtspraak.nl/procedure":      "procedure_type",
    "http://psi.rechtspraak.nl/inhoudsindicatie": "summary",
    "http://psi.rechtspraak.nl/uitspraak":      "full_text",
}

# Columns with static values (not derived from predicates)
STATIC_COLUMNS: dict[str, str] = {
    "source": "Rechtspraak",
}


def _extract_value(obj: Any) -> str:
    """Return the string value of an RDF term, stripping datatype/language metadata."""
    value = str(obj)
    # pyoxigraph Literal.__str__ includes the full N-Triples representation for
    # typed/lang literals, e.g. "text"@nl or "2023-01-01"^^xsd:date.
    # We want only the lexical value.
    if hasattr(obj, "value"):
        return obj.value
    return value


def _accumulator() -> dict[str, Any]:
    """Return an empty per-subject accumulator."""
    return defaultdict(list)


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

_ALL_COLUMNS = [
    "ecli", "document_type", "date_decision", "date_publication", "language",
    "instance", "jurisdiction_city", "case_number", "procedure_type", "spatial",
    "domains", "referenced_legislation_titles", "alternative_publications", "title",
    "full_text", "summary", "citing", "cited_by", "legislations_cited",
    "predecessor_successor_cases", "url_publications", "info", "source",
]

_INSERT_SQL = (
    f"INSERT OR REPLACE INTO metadata ({', '.join(_ALL_COLUMNS)}) "
    f"VALUES ({', '.join('?' * len(_ALL_COLUMNS))})"
)


def _subject_to_row(subject_uri: str, triples: dict[str, list[str]]) -> tuple | None:
    """Convert an accumulated subject dict to an INSERT row tuple, or None to skip."""
    ecli_values = triples.get("ecli", [])
    if not ecli_values:
        return None
    ecli = ecli_values[0]

    def single(col: str) -> str:
        vals = triples.get(col, [])
        return vals[0] if vals else ""

    def joined(col: str) -> str:
        seen: list[str] = []
        for v in triples.get(col, []):
            if v not in seen:
                seen.append(v)
        return "\n".join(seen)

    url_publications = f"https://uitspraken.rechtspraak.nl/inziendocument?id={ecli}"

    return (
        ecli,
        single("document_type"),
        single("date_decision"),
        single("date_publication"),
        single("language") or "nl",
        joined("instance"),
        single("jurisdiction_city"),
        joined("case_number"),
        joined("procedure_type"),
        single("jurisdiction_city"),   # spatial mirrors jurisdiction_city
        joined("domains"),
        "",                            # referenced_legislation_titles (not in lido)
        joined("alternative_publications"),
        single("title"),
        single("full_text"),
        single("summary"),
        joined("citing"),
        "",                            # cited_by (reverse relation, not in lido)
        joined("legislations_cited"),
        "",                            # predecessor_successor_cases (not in lido)
        url_publications,
        single("info"),
        "Rechtspraak",
    )


def _flush(conn: sqlite3.Connection, pending: dict[str, dict[str, list[str]]]) -> int:
    """Insert all pending subjects and return the number of rows written."""
    rows = []
    for subject_uri, triples in pending.items():
        row = _subject_to_row(subject_uri, triples)
        if row is not None:
            rows.append(row)
    if rows:
        conn.executemany(_INSERT_SQL, rows)
        conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

BATCH_SIZE = 10_000
TTL_BASE_IRI = "https://linkeddata.overheid.nl/"


def parse_into_db(ttl_gz_path: Path, db_path: Path) -> None:
    """Stream-parse *ttl_gz_path* and populate the metadata table in *db_path*."""
    conn = sqlite3.connect(db_path)
    schema_sql = (Path(__file__).parent / "schema.sql").read_text()
    conn.executescript(schema_sql)

    pending: dict[str, dict[str, list[str]]] = {}
    total_written = 0
    unknown_predicates: set[str] = set()

    with gzip.open(ttl_gz_path, "rb") as fh:
        triples_iter = pyoxigraph.parse(fh, mime_type="text/turtle", base_iri=TTL_BASE_IRI)

        with tqdm(unit=" triples", desc="Parsing", file=sys.stderr) as bar:
            for triple in triples_iter:
                bar.update(1)

                subject = str(triple.subject)
                if CASE_SUBJECT_MARKER not in subject:
                    continue

                predicate = str(triple.predicate)
                column = PREDICATE_MAP.get(predicate)
                if column is None:
                    unknown_predicates.add(predicate)
                    continue

                value = _extract_value(triple.object)
                if not value:
                    continue

                if subject not in pending:
                    pending[subject] = defaultdict(list)
                pending[subject][column].append(value)

                if len(pending) >= BATCH_SIZE:
                    total_written += _flush(conn, pending)
                    pending.clear()

    if pending:
        total_written += _flush(conn, pending)

    conn.close()
    print(f"Inserted {total_written:,} rows into {db_path}", file=sys.stderr)

    if unknown_predicates:
        print(
            "Unknown predicates on case subjects (add to PREDICATE_MAP if needed):",
            file=sys.stderr,
        )
        for p in sorted(unknown_predicates):
            print(f"  {p}", file=sys.stderr)
