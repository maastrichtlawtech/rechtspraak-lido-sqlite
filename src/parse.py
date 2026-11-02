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

# Law-element subjects: .../terms/bwb/id/{BWBR..}/{label}/{date}/{date}
# Ported from case-law-explorer's airflow/dags/lido/tasks/laws_to_sqlite.py --
# that DAG used to parse these into a second, independent database; folding
# it in here means one download/parse of the export instead of two.
LAW_SUBJECT_MARKER = "linkeddata.overheid.nl/terms/bwb/id/"
LAW_ID_PREFIX = "http://linkeddata.overheid.nl/terms/bwb/id/"

REGELING_ONDERDELEN: dict[str, str] = {
    "http://linkeddata.overheid.nl/terms/Wet": "wet",
    "http://linkeddata.overheid.nl/terms/Deel": "deel",
    "http://linkeddata.overheid.nl/terms/Boek": "boek",
    "http://linkeddata.overheid.nl/terms/Titeldeel": "titeldeel",
    "http://linkeddata.overheid.nl/terms/Hoofdstuk": "hoofdstuk",
    "http://linkeddata.overheid.nl/terms/Artikel": "artikel",
    "http://linkeddata.overheid.nl/terms/Paragraaf": "paragraaf",
    "http://linkeddata.overheid.nl/terms/SubParagraaf": "subparagraaf",
    "http://linkeddata.overheid.nl/terms/Afdeling": "afdeling",
}

LAW_PREDICATE_MAP: dict[str, str] = {
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": "rdf_type",
    "http://purl.org/dc/terms/title": "title_dcterms",
    "http://www.w3.org/2004/02/skos/core#prefLabel": "title_skos",
    "http://www.w3.org/2000/01/rdf-schema#label": "title_rdfs",
    "http://linkeddata.overheid.nl/terms/heeftJuriconnect": "jc_id_candidates",
    "http://linkeddata.overheid.nl/terms/heeftOnderdeelNummer": "number",
}

PREDICATE_MAP: dict[str, str] = {
    # Dublin Core terms — present on case subjects
    "http://purl.org/dc/terms/identifier":   "ecli",
    "http://purl.org/dc/terms/creator":      "creator",
    "http://purl.org/dc/terms/issued":       "issued",
    "http://purl.org/dc/terms/language":     "language",
    "http://purl.org/dc/terms/spatial":      "spatial",
    "http://purl.org/dc/terms/title":        "title",
    "http://purl.org/dc/terms/hasVersion":   "hasVersion",
    "http://purl.org/dc/terms/type":         "type",
    "http://purl.org/dc/terms/isReplacedBy": "relation",
    "http://purl.org/dc/terms/replaces":     "relation",

    # Lido-specific predicates (confirmed from unknown-predicates output)
    "http://linkeddata.overheid.nl/terms/heeftUitspraakdatum": "date_decision",
    "http://linkeddata.overheid.nl/terms/heeftZaaknummer":     "zaaknummer",
    "http://linkeddata.overheid.nl/terms/heeftProceduresoort": "procedure",
    "http://linkeddata.overheid.nl/terms/heeftRechtsgebied":   "subject",
    "http://linkeddata.overheid.nl/terms/linkt":               "legislations_cited",
    "http://linkeddata.overheid.nl/terms/refereertAan": "citations_outgoing",
    "http://linkeddata.overheid.nl/terms/heeftBron":           "info",

    # LX (alternate lido) predicates
    "http://linkeddata.overheid.nl/lx/creator":        "creator",
    "http://linkeddata.overheid.nl/lx/date":           "date_decision",
    "http://linkeddata.overheid.nl/lx/hasVersion":     "hasVersion",
    "http://linkeddata.overheid.nl/lx/heeftZaaknummer": "zaaknummer",
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
    "ecli", "issued", "language", "creator", "date_decision",
    "zaaknummer", "type", "procedure", "spatial", "subject",
    "relation", "references", "hasVersion", "link", "title",
    "inhoudsindicatie", "info", "full_text", "jurisdiction_country", "source",
    "citations_incoming", "citations_outgoing", "legislations_cited",
    "summary", "bwb_id",
]

_SQL_RESERVED = frozenset({"references"})


def _sql_col(col: str) -> str:
    return f'"{col}"' if col in _SQL_RESERVED else col


_COL_LIST = ", ".join(_sql_col(c) for c in _ALL_COLUMNS)
_PLACEHOLDERS = ", ".join("?" * len(_ALL_COLUMNS))
_INSERT_SQL = (
    f"INSERT OR REPLACE INTO metadata ({_COL_LIST}) "
    f"VALUES ({_PLACEHOLDERS})"
)


def _subject_to_row(subject_uri: str, triples: dict[str, list[str]]) -> tuple | None:
    """Convert an accumulated subject dict to an INSERT row tuple, or None to skip."""
    ecli_values = triples.get("ecli", [])
    # Fall back to extracting the ECLI from the subject URI's last path segment
    # e.g. https://linkeddata.overheid.nl/terms/jurisprudentie/id/ECLI:NL:HR:1998:AA9342
    # A case can also have other identifiers (e.g. CELEX:...) in any order.
    ecli = next(
        (value for value in ecli_values if value.startswith("ECLI:")),
        subject_uri.rsplit("/", 1)[-1],
    )
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

    link = f"https://uitspraken.rechtspraak.nl/inziendocument?id={ecli}"

    return (
        ecli,
        single("issued"),
        single("language") or "nl",
        joined("creator"),
        single("date_decision"),
        joined("zaaknummer"),
        single("type"),
        joined("procedure"),
        single("spatial"),
        joined("subject"),
        joined("relation"),
        "",                            # references (not in lido)
        joined("hasVersion"),
        link,
        single("title"),
        "",                            # inhoudsindicatie (not in lido)
        single("info"),
        single("full_text"),
        "",                            # jurisdiction_country (added by downstream script)
        "Rechtspraak",
        "",                            # citations_incoming (reverse relation, not in lido)
        joined("citations_outgoing"),
        joined("legislations_cited"),
        "",                            # summary (not in lido)
        "",                            # bwb_id (not in lido)
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


_LAW_INSERT_SQL = (
    "INSERT OR IGNORE INTO law_element "
    "(type, bwb_id, bwb_label_id, lido_id, jc_id, number, title) "
    "VALUES (?, ?, ?, ?, ?, ?, ?)"
)


def _subject_to_law_row(subject_uri: str, triples: dict[str, list[str]]) -> tuple | None:
    """Convert an accumulated law-subject dict to a law_element row, or None
    to skip. Ported from lido/tasks/laws_to_sqlite.py::process_law_element,
    with one deliberate divergence: the original only looked at the first
    rdf:type triple and dropped the subject entirely if that one didn't
    happen to be a recognized Regeling type, even if a later rdf:type triple
    on the same subject was. This checks all of them."""
    if LAW_SUBJECT_MARKER not in subject_uri:
        return None
    lido_id = subject_uri.split(LAW_SUBJECT_MARKER, 1)[1]
    if not lido_id:
        return None

    parts = lido_id.split("/")
    bwb_id = parts[0] if parts and parts[0] else None
    if not bwb_id:
        return None
    bwb_label_id = parts[1] if len(parts) > 1 and parts[1] else None

    element_type = next(
        (REGELING_ONDERDELEN[t] for t in triples.get("rdf_type", []) if t in REGELING_ONDERDELEN),
        None,
    )
    if element_type is None:
        return None

    def first(col: str) -> str | None:
        vals = triples.get(col, [])
        return vals[0] if vals else None

    title = first("title_dcterms") or first("title_skos") or first("title_rdfs")

    jc_id = next(
        (j for j in triples.get("jc_id_candidates", []) if j.startswith("jci1.3")), None
    )

    number_vals = triples.get("number", [])
    number = number_vals[0] if len(number_vals) == 1 else None

    return (element_type, bwb_id, bwb_label_id, lido_id, jc_id, number, title)


def _flush_law(conn: sqlite3.Connection, pending_law: dict[str, dict[str, list[str]]]) -> int:
    """Insert all pending law-element subjects and return the number written."""
    rows = []
    for subject_uri, triples in pending_law.items():
        row = _subject_to_law_row(subject_uri, triples)
        if row is not None:
            rows.append(row)
    if rows:
        conn.executemany(_LAW_INSERT_SQL, rows)
        conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Case-law link resolution (refereertAan / linkt decomposition)
#
# refereertAan's object is not a plain URI -- it's a pipe-delimited compound
# string, e.g.:
#   linktype=...|target=bwb|uri=jci1.3:c:BWBR0005288&...|lido-id=http://...|opschrift=artikel 5:1 BW
# `target=` distinguishes what's being cited: bwb (Dutch legislation), ecli
# (another case), celex (EU legislation), or other linktypes entirely (e.g.
# the rvr-hogerberoep-eerdereaanleg procedural-relation linktype, which has
# no lido-id/opschrift fields at all). Only bwb and ecli are handled here --
# celex has no consumer/table yet, and the procedural-relation linktype isn't
# a citation. linkt's object, unlike refereertAan's, is already a plain URI.
#
# This can't run inline during the main streaming pass: a case's citation may
# be parsed before or after the law_element it points to, since the export
# isn't subject-sorted. Run only after that pass has fully populated
# law_element.
# ---------------------------------------------------------------------------

_RESOLVE_BATCH_SIZE = 50_000


def _parse_referentie(raw: str) -> dict[str, str | None]:
    """Split a refereertAan value on `|` then `=` into its named fields."""
    return dict(((item.split("=", 1) + [None])[:2]) for item in raw.split("|"))


def _strip_law_lido_id(url: str) -> str | None:
    if url.startswith(LAW_ID_PREFIX) and len(url) > len(LAW_ID_PREFIX):
        return url[len(LAW_ID_PREFIX):]
    return None


class _LawIndex:
    """In-memory lido_id/bwb_id/(bwb_id,bwb_label_id) -> lido_id lookup,
    built once from law_element. A per-row SQL SELECT for every citation
    (up to 3 queries per reference, times ~15 references per case, times
    millions of cases) is what made the original version of this function
    take upwards of a day; law_element (under 1M rows) fits comfortably in
    memory, and dict lookups replace what used to be repeated round trips
    through sqlite's query planner for the exact same fixed set of keys."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._by_lido_id: dict[str, str] = {}
        self._by_bwb_label: dict[tuple[str, str], str] = {}
        self._by_bwb_id: dict[str, str] = {}
        for lido_id, bwb_id, bwb_label_id in conn.execute(
            "SELECT lido_id, bwb_id, bwb_label_id FROM law_element"
        ):
            self._by_lido_id.setdefault(lido_id, lido_id)
            if bwb_id and bwb_label_id:
                self._by_bwb_label.setdefault((bwb_id, bwb_label_id), lido_id)
            if bwb_id:
                self._by_bwb_id.setdefault(bwb_id, lido_id)

    def resolve(self, lido_id_url: str) -> str | None:
        """Mirrors case-law-explorer's cases_to_sqlite.py::
        get_law_element_by_lido_id three-tier fallback (exact lido_id, then
        bwb_id+bwb_label_id, then bwb_id-only), returning the natural key
        (lido_id) rather than a surrogate row id."""
        stripped = _strip_law_lido_id(lido_id_url)
        if stripped is None:
            return None

        matched = self._by_lido_id.get(stripped)
        if matched:
            return matched

        parts = stripped.split("/")
        if len(parts) >= 2 and parts[0] and parts[1]:
            matched = self._by_bwb_label.get((parts[0], parts[1]))
            if matched:
                return matched

        if parts and parts[0]:
            return self._by_bwb_id.get(parts[0])
        return None


def resolve_case_law_links(conn: sqlite3.Connection) -> int:
    """For every case row, decompose its raw citations_outgoing (refereertAan)
    and legislations_cited (linkt) values: target=ecli entries become clean
    case-to-case citations (citations_outgoing is rewritten in place to hold
    only these); target=bwb entries and linkt URIs are resolved against
    law_element and written to case_law. Returns the number of case_law rows
    written.

    Resumable: a raw refereertAan value always contains "|" (every entry is
    at least linktype=...|target=...); the cleaned/rewritten form never does
    (a clean ECLI list, or empty). So citations_outgoing no longer containing
    "|" means this row was already fully handled by a previous run (its
    legislations_cited/case_law resolution happened in the same pass, same
    flush) and is skipped -- safe to interrupt and rerun."""
    read_cursor = conn.cursor()
    write_cursor = conn.cursor()

    law_index = _LawIndex(conn)

    case_law_rows: list[tuple] = []
    citation_updates: list[tuple[str, str]] = []
    written = 0

    def _flush_resolution() -> None:
        nonlocal written
        if case_law_rows:
            write_cursor.executemany(
                "INSERT INTO case_law (ecli, law_lido_id, source, jc_id, opschrift) "
                "VALUES (?, ?, ?, ?, ?)",
                case_law_rows,
            )
            written += len(case_law_rows)
            case_law_rows.clear()
        if citation_updates:
            write_cursor.executemany(
                "UPDATE metadata SET citations_outgoing = ? WHERE ecli = ?", citation_updates
            )
            citation_updates.clear()
        conn.commit()

    read_cursor.execute("SELECT ecli, citations_outgoing, legislations_cited FROM metadata")
    for ecli, raw_citations, raw_legislations in read_cursor:
        if raw_citations is None or "|" not in raw_citations:
            continue  # already processed by a previous, interrupted run

        clean_citations: list[str] = []

        for raw in raw_citations.split("\n"):
            if not raw:
                continue
            ref = _parse_referentie(raw)
            target = ref.get("target")
            if target == "ecli":
                uri = ref.get("uri") or ""
                if uri.startswith("ECLI:") and uri not in clean_citations:
                    clean_citations.append(uri)
            elif target == "bwb":
                lido_id_url = ref.get("lido-id")
                if lido_id_url:
                    matched = law_index.resolve(lido_id_url)
                    if matched:
                        case_law_rows.append(
                            (ecli, matched, "lido-ref", ref.get("uri"), ref.get("opschrift"))
                        )
            # celex / rvr-hogerberoep-eerdereaanleg / anything else: dropped

        for link in (raw_legislations or "").split("\n"):
            if not link:
                continue
            matched = law_index.resolve(link)
            if matched:
                case_law_rows.append((ecli, matched, "lido-linkt", None, None))

        citation_updates.append(("\n".join(clean_citations), ecli))

        if len(case_law_rows) >= _RESOLVE_BATCH_SIZE or len(citation_updates) >= _RESOLVE_BATCH_SIZE:
            _flush_resolution()

    _flush_resolution()
    return written


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
    pending_law: dict[str, dict[str, list[str]]] = {}
    total_written = 0
    total_law_written = 0
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
                predicate = triple.predicate.value

                if CASE_SUBJECT_MARKER in subject:
                    # Ensure the subject is tracked even if no predicate maps to a
                    # column, so _subject_to_row can still derive the ECLI from the URI.
                    if subject not in pending:
                        pending[subject] = defaultdict(list)

                    column = PREDICATE_MAP.get(predicate)
                    if column is None:
                        unknown_predicates.add(predicate)
                        continue

                    value = _extract_value(triple.object)
                    if not value:
                        continue

                    pending[subject][column].append(value)

                elif LAW_SUBJECT_MARKER in subject:
                    if subject not in pending_law:
                        pending_law[subject] = defaultdict(list)

                    column = LAW_PREDICATE_MAP.get(predicate)
                    if column is None:
                        continue

                    value = _extract_value(triple.object)
                    if not value:
                        continue

                    pending_law[subject][column].append(value)

            if len(pending) >= BATCH_SIZE:
                total_written += _flush(conn, pending)
                pending.clear()
            if len(pending_law) >= BATCH_SIZE:
                total_law_written += _flush_law(conn, pending_law)
                pending_law.clear()

    if pending:
        total_written += _flush(conn, pending)
    if pending_law:
        total_law_written += _flush_law(conn, pending_law)

    print(f"Inserted {total_written:,} case rows into {db_path}", file=sys.stderr)
    print(f"Inserted {total_law_written:,} law element rows into {db_path}", file=sys.stderr)
    if skipped_lines:
        print(f"Skipped {skipped_lines:,} unparseable N-Triple lines", file=sys.stderr)

    if unknown_predicates:
        print(
            "Unknown predicates on case subjects (add to PREDICATE_MAP if needed):",
            file=sys.stderr,
        )
        for p in sorted(unknown_predicates):
            print(f"  {p}", file=sys.stderr)

    print("Resolving case-law links...", file=sys.stderr)
    written = resolve_case_law_links(conn)
    print(f"Wrote {written:,} case_law row(s)", file=sys.stderr)

    conn.close()
