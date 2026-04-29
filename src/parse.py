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

# Subjects of interest: https://linkeddata.overheid.nl/terms/jurisprudentie/id/{ECLI}
# The ECLI is the final path segment, e.g. ECLI:NL:HR:1998:AA9342
CASE_SUBJECT_MARKER = "linkeddata.overheid.nl/terms/jurisprudentie/id/"

PREDICATE_MAP: dict[str, str] = {
    # Dublin Core terms — present on case subjects
    "http://purl.org/dc/terms/identifier":   "ecli",
    "http://purl.org/dc/terms/creator":      "instance",
    "http://purl.org/dc/terms/issued":       "date_publication",
    "http://purl.org/dc/terms/language":     "language",
    "http://purl.org/dc/terms/spatial":      "jurisdiction_city",
    "http://purl.org/dc/terms/title":        "title",
    "http://purl.org/dc/terms/hasVersion":   "alternative_publications",
    "http://purl.org/dc/terms/type":         "document_type",
    "http://purl.org/dc/terms/isReplacedBy": "predecessor_successor_cases",
    "http://purl.org/dc/terms/replaces":     "predecessor_successor_cases",

    # Lido-specific predicates (confirmed from unknown-predicates output)
    "http://linkeddata.overheid.nl/terms/heeftUitspraakdatum": "date_decision",
    "http://linkeddata.overheid.nl/terms/heeftZaaknummer":     "case_number",
    "http://linkeddata.overheid.nl/terms/heeftProceduresoort": "procedure_type",
    "http://linkeddata.overheid.nl/terms/heeftRechtsgebied":   "domains",
    "http://linkeddata.overheid.nl/terms/linkt":               "legislations_cited",
    "http://linkeddata.overheid.nl/terms/refereertAan":        "citing",
    "http://linkeddata.overheid.nl/terms/heeftBron":           "info",

    # LX (alternate lido) predicates
    "http://linkeddata.overheid.nl/lx/creator":        "instance",
    "http://linkeddata.overheid.nl/lx/date":           "date_decision",
    "http://linkeddata.overheid.nl/lx/hasVersion":     "alternative_publications",
    "http://linkeddata.overheid.nl/lx/heeftZaaknummer": "case_number",
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
    # Fall back to extracting the ECLI from the subject URI's last path segment
    # e.g. https://linkeddata.overheid.nl/terms/jurisprudentie/id/ECLI:NL:HR:1998:AA9342
    ecli = ecli_values[0] if ecli_values else subject_uri.rsplit("/", 1)[-1]
    if not ecli.startswith("ECLI:"):
        return None

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

# Each entry is (tool_binary, args_template) where {base} is replaced at runtime.
# We probe each tool with a tiny snippet first to verify it works before committing
# to streaming the full file through it.
_PROBE = b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"

_CONVERTER_TEMPLATES: list[list[str]] = [
    # serdi (serd ≥ 0.30): -l = lax, -b = base IRI, INPUT=- means stdin
    ["serdi", "-l", "-b", "{base}", "-i", "turtle", "-o", "ntriples", "-"],
    # serdi without explicit format flags (older versions / auto-detect)
    ["serdi", "-l", "-"],
    # rapper (raptor2): -q = quiet, FILE=- means stdin, last arg is base URI
    ["rapper", "-q", "-i", "turtle", "-o", "ntriples", "-", "{base}"],
]


def _build_cmd(template: list[str]) -> list[str]:
    return [a.replace("{base}", TTL_BASE_IRI) for a in template]


def _probe(cmd: list[str]) -> bool:
    """Return True if the command can parse a trivial N-Triple snippet."""
    try:
        result = subprocess.run(
            cmd,
            input=_PROBE,
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0 and b"example.org" in result.stdout
    except Exception:
        return False


def _find_converter() -> list[str]:
    """Return the first working converter command, probed with a tiny test."""
    for template in _CONVERTER_TEMPLATES:
        binary = template[0]
        if not shutil.which(binary):
            continue
        cmd = _build_cmd(template)
        if _probe(cmd):
            return cmd
        # Binary exists but probe failed — keep trying other templates for
        # the same binary (e.g., the no-flag serdi variant).

    tools = sorted({t[0] for t in _CONVERTER_TEMPLATES})
    raise RuntimeError(
        f"No working TTL→N-Triples converter found (tried: {tools}).\n"
        "Install one:\n"
        "  macOS:  brew install serd        # provides serdi\n"
        "          brew install raptor       # provides rapper\n"
        "  Ubuntu: sudo apt install serd\n"
        "          sudo apt install raptor2-utils\n"
    )


def _iter_ntriples(ttl_gz_path: Path) -> Iterator[bytes]:
    """Yield raw N-Triple lines by piping the decompressed TTL through a converter."""
    cmd = _find_converter()
    print(f"Using converter: {' '.join(cmd)}", file=sys.stderr)

    stderr_chunks: list[bytes] = []

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    def _feed() -> None:
        assert proc.stdin is not None
        try:
            with gzip.open(ttl_gz_path, "rb") as fh:
                while chunk := fh.read(1 << 20):
                    proc.stdin.write(chunk)
        except BrokenPipeError:
            pass  # subprocess exited early; stderr will explain why
        finally:
            try:
                proc.stdin.close()
            except OSError:
                pass

    def _drain_stderr() -> None:
        assert proc.stderr is not None
        stderr_chunks.append(proc.stderr.read())

    feeder = threading.Thread(target=_feed, daemon=True)
    drainer = threading.Thread(target=_drain_stderr, daemon=True)
    feeder.start()
    drainer.start()

    assert proc.stdout is not None
    yield from proc.stdout

    proc.wait()
    feeder.join()
    drainer.join()

    if proc.returncode not in (0, 1):  # raptor exits 1 on warnings; that's fine
        stderr_text = b"".join(stderr_chunks).decode(errors="replace").strip()
        raise RuntimeError(
            f"Converter exited with code {proc.returncode}.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Stderr: {stderr_text[:1000] or '(empty)'}"
        )


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
                # Use .value to get the bare IRI string; str() returns "<iri>" in pyoxigraph
                subject = triple.subject.value
                if CASE_SUBJECT_MARKER not in subject:
                    continue

                # Ensure the subject is tracked even if no predicate maps to a column,
                # so _subject_to_row can still derive the ECLI from the URI.
                if subject not in pending:
                    pending[subject] = defaultdict(list)

                predicate = triple.predicate.value
                column = PREDICATE_MAP.get(predicate)
                if column is None:
                    unknown_predicates.add(predicate)
                    continue

                value = _extract_value(triple.object)
                if not value:
                    continue

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
