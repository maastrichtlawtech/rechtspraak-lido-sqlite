#!/usr/bin/env python3
"""Inspect the N-Triples output of lido-export.ttl.gz to discover subject URI patterns.

Run this before the full build to verify what subject URIs and predicates are
present in the file, so PREDICATE_MAP and CASE_SUBJECT_MARKER in src/parse.py
can be confirmed or corrected.

Usage:
    python inspect_lido.py                          # reads data/lido-export.ttl.gz
    python inspect_lido.py --input data/lido-export.ttl.gz --lines 50000
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyoxigraph

sys.path.insert(0, str(Path(__file__).parent))
from src.parse import _iter_ntriples  # reuse the converter pipeline


def inspect(ttl_gz_path: Path, max_lines: int) -> None:
    subject_prefixes: Counter[str] = Counter()
    subject_samples: dict[str, str] = {}          # prefix → one full subject URI
    predicate_by_prefix: dict[str, Counter[str]] = defaultdict(Counter)
    parsed = skipped = 0

    for raw_line in _iter_ntriples(ttl_gz_path):
        if parsed + skipped >= max_lines:
            break
        line = raw_line.strip()
        if not line or line.startswith(b"#"):
            continue

        try:
            triples = list(pyoxigraph.parse(line, pyoxigraph.RdfFormat.N_TRIPLES))
        except Exception:
            skipped += 1
            continue

        for triple in triples:
            parsed += 1
            subject = str(triple.subject)
            predicate = str(triple.predicate)

            # Bucket subjects by their authority+first-path-segment
            prefix = _subject_prefix(subject)
            subject_prefixes[prefix] += 1
            if prefix not in subject_samples:
                subject_samples[prefix] = subject
            predicate_by_prefix[prefix][predicate] += 1

    # ── report ──────────────────────────────────────────────────────────────
    print(f"\nSampled {parsed:,} triples ({skipped:,} lines skipped)\n")

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


def _subject_prefix(uri: str) -> str:
    """Return scheme+host+first path segment, used to group related subjects."""
    # Strip scheme
    rest = uri.split("://", 1)[-1] if "://" in uri else uri
    # Take host + first path segment
    parts = rest.split("/")
    return "/".join(parts[:2]) if len(parts) > 1 else rest


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--input", "-i", type=Path, default=Path("data/lido-export.ttl.gz"),
        help="Path to lido-export.ttl.gz",
    )
    parser.add_argument(
        "--lines", "-n", type=int, default=200_000,
        help="Maximum N-Triple lines to sample (default: 200 000)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"File not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    inspect(args.input, args.lines)


if __name__ == "__main__":
    main()
