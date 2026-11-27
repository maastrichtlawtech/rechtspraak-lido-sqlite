# rechtspraak-ldio-sqlite

Self-hosted pipeline that downloads the Dutch [LiDO linked-data export](https://data.overheid.nl/dataset/linked-data-overheid) (`https://linkeddata.overheid.nl/export/lido-export.nt.gz`, an N-Triples dump of about 4 GB) and converts it into a local SQLite database.

The resulting database is a drop-in backend for the [`rechtspraak-extractor`](https://github.com/maastrichtlawtech/rechtspraak-extractor) package: its `fetch_eclis_via_sqlite()` function queries the `metadata` table built here, avoiding any dependency on a live SPARQL endpoint or the Rechtspraak API.

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | ≥ 3.10 | |
| `serdi` **or** `rapper` | any | Optional. Only needed to build from the legacy Turtle export (`lido-export.ttl.gz`), which contains syntax violations that strict parsers reject. The current `.nt.gz` export needs no converter |

Install the system converter (legacy `.ttl.gz` input only):

```bash
# macOS
brew install serd          # provides serdi (recommended)
# or
brew install raptor        # provides rapper

# Ubuntu / Debian
sudo apt install serdi
# or
sudo apt install raptor2-utils
```

---

## Installation

```bash
pip install rechtspraak-lido-sqlite
```

Or from source:

```bash
git clone https://github.com/shashankmc/rechtspraak-lido-sqlite
cd rechtspraak-lido-sqlite
pip install -e .
```

---

## Quick start

```bash
# 1. Download (~3 GB compressed) and build the SQLite database in one step
build-lido-sqlite --download

# 2. Verify the result
test-lido-query

# 3. Inspect the first N lines (useful for debugging)
inspect-lido --mode cases --lines 500000
```

All three commands are also available as `make` targets:

```bash
make install-tools   # brew install serd (legacy .ttl.gz input only)
make download        # download + build  →  data/lido.db
make build           # build from an already-downloaded file
```

---

## CLI reference

### `build-lido-sqlite`

```
build-lido-sqlite [--input PATH] [--output PATH] [--download]

  --input   PATH   Source .nt.gz file (or legacy .ttl.gz)  (default: data/lido-export.nt.gz)
  --output  PATH   SQLite output file   (default: data/lido.db)
  --download       Download the source file before building
```

### `inspect-lido`

Shows subject URI patterns and predicates from a sample of the file — useful for verifying the predicate map or understanding the data structure.

```
inspect-lido [--input PATH] [--mode subjects|cases] [--skip N] [--lines N]

  --mode subjects   Show subject URI distribution and top predicates (default)
  --mode cases      Find ECLI subjects and case-link objects
  --skip  N         Skip the first N N-Triple lines before sampling
  --lines N         Maximum lines to sample (default: 200 000)
```

### `test-lido-query`

Runs the exact SQL query used by `fetch_eclis_via_sqlite()` and prints results plus per-column fill rates.

```
test-lido-query [--db PATH] [--ecli ECLI ...]

  --db    PATH   SQLite database  (default: data/lido.db)
  --ecli  ECLI   ECLI to look up (repeat for multiple; omit to sample 5 rows)
```

---

## Integration with rechtspraak-extractor

```python
from rechtspraak_extractor import rechtspraak_metadata as rm

df = rm.fetch_eclis_via_sqlite(
    ecli_list=["ECLI:NL:HR:2010:BN2349", "ECLI:NL:RBAMS:2023:1234"],
    sqlite_db_path="data/lido.db",
    columns=rm.METADATA_COLUMNS,
)
print(df.head())
```

The `metadata` table has 25 columns matching the `MAP_RS` keys in `rechtspraak-extractor`:

| Column | Source predicate | Notes |
|---|---|---|
| `ecli` | `dcterms:identifier` | |
| `issued` | `dcterms:issued` | date of publication on Rechtspraak.nl |
| `language` | `dcterms:language` | |
| `creator` | `dcterms:creator` / `lx:creator` | name of court |
| `date_decision` | `lido:heeftUitspraakdatum` | date of court decision |
| `zaaknummer` | `lido:heeftZaaknummer` | internal case number |
| `type` | `dcterms:type` | `uitspraak` or `conclusie` |
| `procedure` | `lido:heeftProceduresoort` | procedure type |
| `spatial` | `dcterms:spatial` | court municipality |
| `subject` | `lido:heeftRechtsgebied` | area of law |
| `relation` | `dcterms:isReplacedBy` / `dcterms:replaces` | predecessor/successor cases |
| `references` | — | applicable legislation titles; empty (not in lido) |
| `hasVersion` | `dcterms:hasVersion` / `lx:hasVersion` | alternative publishers |
| `link` | constructed from ECLI | deeplink to Rechtspraak.nl |
| `title` | `dcterms:title` | |
| `inhoudsindicatie` | — | case summary; empty (not in lido) |
| `info` | `lido:heeftBron` | source information |
| `full_text` | — | full case text; empty (not in lido) |
| `jurisdiction_country` | — | country; empty (added by downstream script) |
| `source` | — | `"Rechtspraak"` (static) |
| `citations_incoming` | — | cases citing this case; empty (reverse relation, not in lido) |
| `citations_outgoing` | `lido:refereertAan` | cases cited by this case |
| `legislations_cited` | `lido:linkt` | legislation cited |
| `summary` | — | empty (not in lido) |
| `bwb_id` | — | BWB legislation ID; empty (not in lido) |

---

## How it works

1. **Download** — `src/download.py` streams `lido-export.nt.gz` from `linkeddata.overheid.nl` with a progress bar. It retries on HTTP 429/5xx and fails if the response is not gzip, because the server answers missing files with an HTML error page and HTTP 200.
2. **Read** — `src/parse.py` streams the decompressed N-Triples lines directly. For the legacy Turtle export (`.ttl.gz`) it instead pipes the input through `serdi -l` (lax mode) or `rapper -q`, working around syntax violations in that file (invalid escape sequences, non-IRI characters).
3. **Parse** — Each N-Triple line is parsed by pyoxigraph. Triples whose subject matches `linkeddata.overheid.nl/terms/jurisprudentie/id/ECLI:…` are accumulated per case.
4. **Insert** — Cases are batch-inserted into the `metadata` table in SQLite (10 000 rows per transaction).

---

## Development

```bash
pip install -e .
python test_query.py --db data/lido.db
```

---

## License

MIT
