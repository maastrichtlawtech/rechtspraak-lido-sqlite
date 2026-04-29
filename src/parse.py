"""Stream-parse lido-export.ttl.gz and insert case metadata into SQLite."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

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
