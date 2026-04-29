#!/usr/bin/env python3
"""Build a SQLite metadata database from the lido-export.ttl.gz linked-data file.

Usage:
    # Download and build in one step:
    python build_lido_sqlite.py --download

    # Build from a file you already have:
    python build_lido_sqlite.py --input data/lido-export.ttl.gz

    # Custom paths:
    python build_lido_sqlite.py --input /path/to/lido-export.ttl.gz --output /path/to/lido.db
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.download import download
from src.parse import parse_into_db

DEFAULT_INPUT = Path("data/lido-export.ttl.gz")
DEFAULT_OUTPUT = Path("data/lido.db")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", "-i", type=Path, default=DEFAULT_INPUT, help="Path to lido-export.ttl.gz")
    parser.add_argument("--output", "-o", type=Path, default=DEFAULT_OUTPUT, help="Path to output SQLite database")
    parser.add_argument("--download", action="store_true", help="Download the source file before parsing")
    args = parser.parse_args()

    if args.download:
        if args.input.exists():
            print(f"{args.input} already exists, skipping download.", file=sys.stderr)
        else:
            print(f"Downloading to {args.input} …", file=sys.stderr)
            download(args.input)

    if not args.input.exists():
        print(
            f"Error: {args.input} not found. Run with --download to fetch it first.",
            file=sys.stderr,
        )
        sys.exit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    parse_into_db(args.input, args.output)


if __name__ == "__main__":
    main()
