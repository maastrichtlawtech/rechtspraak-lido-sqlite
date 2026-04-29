#!/usr/bin/env python3
"""Sanity-check the lido SQLite database.

Picks a handful of ECLIs from the metadata table and runs the exact query
that fetch_eclis_via_sqlite() uses in rechtspraak-extractor, then prints
the results so you can verify the columns are populated correctly.

Usage:
    python test_query.py                     # uses data/lido.db
    python test_query.py --db /path/to/lido.db
    python test_query.py --db data/lido.db --ecli ECLI:NL:HR:2010:BN2349
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

COLUMNS = [
    "ecli", "date_publication", "language", "instance", "jurisdiction_city",
    "date_decision", "case_number", "document_type", "procedure_type",
    "domains", "referenced_legislation_titles", "alternative_publications",
    "title", "full_text", "summary", "citing", "cited_by", "legislations_cited",
    "predecessor_successor_cases", "url_publications", "info", "source",
]


def run(db_path: Path, eclis: list[str]) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    if not eclis:
        # Pick up to 5 random ECLIs from the table
        rows = conn.execute("SELECT ecli FROM metadata LIMIT 5").fetchall()
        eclis = [r["ecli"] for r in rows]
        if not eclis:
            print("Database is empty — has the parse step completed?", file=sys.stderr)
            sys.exit(1)
        print(f"No ECLIs specified; sampling: {eclis}\n")

    placeholders = ",".join("?" * len(eclis))
    query = f"""
        SELECT {', '.join(COLUMNS)}
        FROM metadata
        WHERE ecli IN ({placeholders})
    """

    rows = conn.execute(query, eclis).fetchall()
    print(f"Rows returned: {len(rows)} / {len(eclis)} requested\n")

    for row in rows:
        print("=" * 72)
        for col in COLUMNS:
            value = row[col]
            if value and len(str(value)) > 120:
                value = str(value)[:120] + " …"
            print(f"  {col:<35} {value!r}")
    print("=" * 72)

    # Summary stats
    total = conn.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
    filled = {
        col: conn.execute(
            f"SELECT COUNT(*) FROM metadata WHERE {col} != '' AND {col} IS NOT NULL"
        ).fetchone()[0]
        for col in ("ecli", "instance", "date_decision", "domains", "full_text", "summary")
    }
    print(f"\nTotal rows in metadata: {total:,}")
    print("Fill rates for key columns:")
    for col, count in filled.items():
        pct = 100 * count / total if total else 0
        print(f"  {col:<20} {count:>8,}  ({pct:.1f}%)")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=Path("data/lido.db"), help="Path to SQLite database")
    parser.add_argument("--ecli", action="append", dest="eclis", default=[], metavar="ECLI",
                        help="ECLI to look up (repeat for multiple; omit to sample 5 rows)")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    run(args.db, args.eclis)


if __name__ == "__main__":
    main()
