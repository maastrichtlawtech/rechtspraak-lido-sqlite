#!/usr/bin/env python3
"""Inspect the N-Triples output of lido-export.ttl.gz to discover data structure.

Two modes:
  subjects  (default) – show subject URI patterns and their top predicates
  cases               – specifically look for court-case triples:
                        subjects with an ECLI identifier, and objects of
                        heeftJuriconnect / linkt / refereertAan

Usage:
    python inspect_lido.py                              # subject overview, 200K lines
    python inspect_lido.py --mode cases --lines 5000000
    python inspect_lido.py --mode cases --skip 10000000 --lines 2000000
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyoxigraph

sys.path.insert(0, str(Path(__file__).parent))
from src.parse import _iter_ntriples  # reuse the converter pipeline

# Predicates that link law provisions → court cases in lido data
CASE_LINK_PREDICATES = {
    "http://linkeddata.overheid.nl/terms/heeftJuriconnect",
    "http://linkeddata.overheid.nl/terms/linkt",
    "http://linkeddata.overheid.nl/terms/refereertAan",
}

ECLI_IDENTIFIER = "http://purl.org/dc/terms/identifier"


# ── mode: subjects ────────────────────────────────────────────────────────────

def mode_subjects(ttl_gz_path: Path, skip: int, max_lines: int) -> None:
    subject_prefixes: Counter[str] = Counter()
    subject_samples: dict[str, str] = {}
    predicate_by_prefix: dict[str, Counter[str]] = defaultdict(Counter)
    parsed = skipped = consumed = 0

    for raw_line in _iter_ntriples(ttl_gz_path):
        if consumed < skip:
            consumed += 1
            continue
        if parsed + skipped >= max_lines:
            break
        line = raw_line.strip()
        if not line or line.startswith(b"#"):
            consumed += 1
            continue

        try:
            triples = list(pyoxigraph.parse(line, pyoxigraph.RdfFormat.N_TRIPLES))
        except Exception:
            skipped += 1
            consumed += 1
            continue

        for triple in triples:
            parsed += 1
            subject = str(triple.subject)
            predicate = str(triple.predicate)
            prefix = _subject_prefix(subject)
            subject_prefixes[prefix] += 1
            if prefix not in subject_samples:
                subject_samples[prefix] = subject
            predicate_by_prefix[prefix][predicate] += 1
        consumed += 1

    print(f"\nSampled {parsed:,} triples (skip={skip:,}, {skipped:,} lines skipped)\n")
    print("Subject URI prefixes (sorted by frequency):")
    print("-" * 72)
    for prefix, count in subject_prefixes.most_common(20):
        print(f"  {count:>8,}  {prefix}")
        print(f"           example: {subject_samples[prefix]}")
    print()
    print("Predicates per subject prefix (top 5 each):")
    print("-" * 72)
    for prefix, _ in subject_prefixes.most_common(20):
        print(f"\n  {prefix}")
        for pred, cnt in predicate_by_prefix[prefix].most_common(5):
            print(f"    {cnt:>7,}  {pred}")


# ── mode: cases ───────────────────────────────────────────────────────────────

def mode_cases(ttl_gz_path: Path, skip: int, max_lines: int) -> None:
    """Look for court-case evidence: ECLI identifiers and case-link objects."""
    # subjects whose dcterms:identifier value looks like an ECLI
    ecli_subjects: dict[str, str] = {}         # subject URI → ECLI value
    # objects of law→case link predicates
    case_link_objects: list[tuple[str, str]] = []   # (predicate, object URI)
    # predicates seen on those case subjects
    case_predicates: Counter[str] = Counter()

    parsed = skipped = consumed = 0

    for raw_line in _iter_ntriples(ttl_gz_path):
        if consumed < skip:
            consumed += 1
            continue
        if parsed + skipped >= max_lines:
            break
        line = raw_line.strip()
        if not line or line.startswith(b"#"):
            consumed += 1
            continue

        try:
            triples = list(pyoxigraph.parse(line, pyoxigraph.RdfFormat.N_TRIPLES))
        except Exception:
            skipped += 1
            consumed += 1
            continue

        for triple in triples:
            parsed += 1
            subject = str(triple.subject)
            predicate = str(triple.predicate)
            obj = triple.object

            # Track case-link predicate objects (law → case URIs)
            if predicate in CASE_LINK_PREDICATES and len(case_link_objects) < 10:
                case_link_objects.append((predicate.split("/")[-1], str(obj)))

            # Track subjects that carry an ECLI identifier
            if predicate == ECLI_IDENTIFIER:
                val = obj.value if hasattr(obj, "value") else str(obj)
                if val.startswith("ECLI:") and subject not in ecli_subjects:
                    ecli_subjects[subject] = val
                    if len(ecli_subjects) >= 5:
                        break   # enough examples

            # Track predicates used on known case subjects
            if subject in ecli_subjects:
                case_predicates[predicate] += 1

        consumed += 1

    print(f"\nSampled {parsed:,} triples (skip={skip:,}, {skipped:,} lines skipped)\n")

    print("=" * 72)
    print("Subjects with ECLI dcterms:identifier values (case subjects):")
    print("-" * 72)
    if ecli_subjects:
        for uri, ecli in list(ecli_subjects.items())[:5]:
            print(f"  ECLI   : {ecli}")
            print(f"  Subject: {uri}")
            print()
    else:
        print("  (none found in this range)")

    print()
    print("=" * 72)
    print("Objects of case-link predicates (law→case references):")
    print("-" * 72)
    if case_link_objects:
        for pred, obj in case_link_objects:
            print(f"  [{pred}] → {obj}")
    else:
        print("  (none found in this range)")

    print()
    print("=" * 72)
    print("Predicates seen on those case subjects:")
    print("-" * 72)
    for pred, cnt in case_predicates.most_common(20):
        print(f"  {cnt:>6,}  {pred}")


# ── helpers ───────────────────────────────────────────────────────────────────

def _subject_prefix(uri: str) -> str:
    rest = uri.split("://", 1)[-1] if "://" in uri else uri
    parts = rest.split("/")
    return "/".join(parts[:2]) if len(parts) > 1 else rest


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input", "-i", type=Path, default=Path("data/lido-export.ttl.gz"))
    parser.add_argument("--mode", choices=["subjects", "cases"], default="subjects")
    parser.add_argument("--skip", "-s", type=int, default=0,
                        help="Skip this many N-Triple lines before sampling")
    parser.add_argument("--lines", "-n", type=int, default=200_000,
                        help="Maximum N-Triple lines to sample (default: 200 000)")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"File not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if args.mode == "cases":
        mode_cases(args.input, args.skip, args.lines)
    else:
        mode_subjects(args.input, args.skip, args.lines)


if __name__ == "__main__":
    main()
