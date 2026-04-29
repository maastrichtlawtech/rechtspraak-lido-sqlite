"""Stream-parse lido-export.ttl.gz and insert case metadata into SQLite."""

from __future__ import annotations

import gzip
import shutil
import sqlite3
import subprocess
import sys
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

import pyoxigraph
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Predicate → column mapping
# ---------------------------------------------------------------------------

# Subjects of interest: http://deeplink.rechtspraak.nl/uitspraak?id={ECLI}
CASE_SUBJECT_MARKER = "deeplink.rechtspraak.nl/uitspraak"

PREDICATE_MAP: dict[str, str] = {
    "http://purl.org/dc/terms/identifier":        "ecli",
    "http://purl.org/dc/terms/creator":           "instance",
    "http://purl.org/dc/terms/date":              "date_decision",
    "http://purl.org/dc/terms/issued":            "date_publication",
    "http://purl.org/dc/terms/type":              "document_type",
    "http://purl.org/dc/terms/language":          "language",
    "http://purl.org/dc/terms/spatial":           "jurisdiction_city",
    "http://purl.org/dc/terms/title":             "title",
    "http://purl.org/dc/terms/description":       "info",
    "http://purl.org/dc/terms/hasVersion":        "alternative_publications",
    "http://purl.org/dc/terms/relation":          "citing",
    "http://purl.org/dc/terms/references":        "legislations_cited",
    "http://purl.org/dc/terms/subject":           "domains",
    "http://psi.rechtspraak.nl/zaaknummer":       "case_number",
    "http://psi.rechtspraak.nl/procedure":        "procedure_type",
    "http://psi.rechtspraak.nl/inhoudsindicatie": "summary",
    "http://psi.rechtspraak.nl/uitspraak":        "full_text",
}


def _extract_value(obj: Any) -> str:
    """Return the lexical value of an RDF term."""
    if hasattr(obj, "value"):
        return obj.value
    return str(obj)


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
# N-Triples conversion via rapper or serdi
#
# The lido-export.ttl.gz file contains systematic Turtle syntax violations
# (invalid escape sequences, bare colons, non-IRI characters) that a strict
# parser cannot handle.  The same issue is documented in the case-law-explorer
# Airflow DAG, which pipes the file through `serdi -l` (lax mode) before
# parsing.  We do the same here: convert to N-Triples using an external tool
# that tolerates errors, then parse each N-Triple independently so a single
# bad line never aborts the whole run.
# ---------------------------------------------------------------------------

TTL_BASE_IRI = "https://linkeddata.overheid.nl/"

_CONVERTERS: list[list[str]] = [
    # serdi (serd): -l = lax/skip errors, -b = base IRI
    ["serdi", "-l", "-b", TTL_BASE_IRI, "-i", "turtle", "-o", "ntriples", "-"],
    # rapper (raptor2): -q = quiet, last positional arg is the base URI
    ["rapper", "-q", "-i", "turtle", "-o", "ntriples", "-", TTL_BASE_IRI],
]


def _find_converter() -> list[str]:
    """Return the command list for the first available TTL→N-Triples converter."""
    for cmd in _CONVERTERS:
        if shutil.which(cmd[0]):
            return cmd
    tools = ", ".join(c[0] for c in _CONVERTERS)
    raise RuntimeError(
        f"None of [{tools}] found on PATH.\n"
        "Install one to convert the Turtle source file:\n"
        "  macOS:  brew install serd        # provides serdi\n"
        "          brew install raptor       # provides rapper\n"
        "  Ubuntu: sudo apt install serd\n"
        "          sudo apt install raptor2-utils\n"
    )


def _iter_ntriples(ttl_gz_path: Path) -> Iterator[bytes]:
    """Yield raw N-Triple lines by piping the decompressed TTL through a converter."""
    cmd = _find_converter()

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,   # lax-mode warnings go here; suppress them
    )

    def _feed() -> None:
        assert proc.stdin is not None
        with gzip.open(ttl_gz_path, "rb") as fh:
            while chunk := fh.read(1 << 20):
                proc.stdin.write(chunk)
        proc.stdin.close()

    feeder = threading.Thread(target=_feed, daemon=True)
    feeder.start()

    assert proc.stdout is not None
    yield from proc.stdout

    proc.wait()
    feeder.join()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

BATCH_SIZE = 10_000


def parse_into_db(ttl_gz_path: Path, db_path: Path) -> None:
    """Convert *ttl_gz_path* to N-Triples and insert case metadata into *db_path*."""
    conn = sqlite3.connect(db_path)
    schema_sql = (Path(__file__).parent / "schema.sql").read_text()
    conn.executescript(schema_sql)

    pending: dict[str, dict[str, list[str]]] = {}
    total_written = 0
    unknown_predicates: set[str] = set()
    skipped_lines = 0

    with tqdm(unit=" lines", desc="Parsing", file=sys.stderr) as bar:
        for raw_line in _iter_ntriples(ttl_gz_path):
            bar.update(1)
            line = raw_line.strip()
            if not line or line.startswith(b"#"):
                continue

            try:
                triples = list(pyoxigraph.parse(line, pyoxigraph.RdfFormat.N_TRIPLES))
            except Exception:
                skipped_lines += 1
                continue

            for triple in triples:
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
    if skipped_lines:
        print(f"Skipped {skipped_lines:,} unparseable N-Triple lines", file=sys.stderr)

    if unknown_predicates:
        print(
            "Unknown predicates on case subjects (add to PREDICATE_MAP if needed):",
            file=sys.stderr,
        )
        for p in sorted(unknown_predicates):
            print(f"  {p}", file=sys.stderr)
