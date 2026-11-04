-- Adds the three columns rechtspraak_extractor==1.6.0's fetch_eclis_via_sqlite()
-- expects but this schema didn't have: jurisdiction_city, predecessor_successor_cases,
-- url_publications. Its SQL query (rechtspraak_metadata.py, the query inside
-- fetch_eclis_via_sqlite) selects these three names verbatim; without them the
-- query raises "no such column" for every lookup, which fetch_eclis_via_sqlite
-- swallows into an empty DataFrame -- so with fallback_to_api=True every call
-- silently falls through to the live API instead of reading this database at
-- all, with no visible error anywhere.
--
-- Each of the three is a GENERATED ALWAYS AS (...) VIRTUAL column aliasing a
-- column this schema already populates under a different name, rather than a
-- second column requiring its own population logic in src/parse.py:
--   jurisdiction_city          -> spatial   (court municipality, already captured)
--   predecessor_successor_cases -> relation (dcterms:isReplacedBy/dcterms:replaces, already captured)
--   url_publications           -> link      (constructed deeplink, already captured)
-- This keeps one source of truth per fact and needs no change to the insert
-- logic in src/parse.py -- generated columns can't be (and don't need to be)
-- written to directly.
--
-- Requires SQLite >= 3.31 (generated columns). Ships with Python's sqlite3
-- module on any reasonably current system; if this ever needs to run against
-- an older SQLite, replace the three GENERATED columns with plain TEXT columns
-- and populate them alongside their source column in src/parse.py instead.
--
-- Known residual gap this schema change does NOT fix: fetch_eclis_via_sqlite's
-- query also never selects "spatial" itself (only the aliased
-- jurisdiction_city would come through, and only once rechtspraak_extractor
-- is on a version whose query knows to ask for it under whichever name).
-- That part can only be fixed on the rechtspraak_extractor side.

CREATE TABLE IF NOT EXISTS metadata (
    ecli                 TEXT PRIMARY KEY,
    issued               TEXT DEFAULT '',
    language             TEXT DEFAULT 'nl',
    creator              TEXT DEFAULT '',
    date_decision        TEXT DEFAULT '',
    zaaknummer           TEXT DEFAULT '',
    type                 TEXT DEFAULT '',
    procedure            TEXT DEFAULT '',
    spatial              TEXT DEFAULT '',
    subject              TEXT DEFAULT '',
    relation             TEXT DEFAULT '',
    "references"         TEXT DEFAULT '',
    hasVersion           TEXT DEFAULT '',
    link                 TEXT DEFAULT '',
    title                TEXT DEFAULT '',
    inhoudsindicatie     TEXT DEFAULT '',
    info                 TEXT DEFAULT '',
    full_text            TEXT DEFAULT '',
    jurisdiction_country TEXT DEFAULT '',
    source               TEXT DEFAULT 'Rechtspraak',
    citations_incoming   TEXT DEFAULT '',
    citations_outgoing   TEXT DEFAULT '',
    legislations_cited   TEXT DEFAULT '',
    summary              TEXT DEFAULT '',
    bwb_id               TEXT DEFAULT '',

    -- Added: aliases expected by rechtspraak_extractor==1.6.0's
    -- fetch_eclis_via_sqlite() query. See file header.
    jurisdiction_city           TEXT GENERATED ALWAYS AS (spatial) VIRTUAL,
    predecessor_successor_cases TEXT GENERATED ALWAYS AS (relation) VIRTUAL,
    url_publications             TEXT GENERATED ALWAYS AS (link) VIRTUAL
);

-- Legislation hierarchy (Wet/Boek/Titeldeel/Hoofdstuk/Artikel/...), ported from
-- case-law-explorer's airflow/dags/lido/tasks/laws_to_sqlite.py -- previously
-- built by a second, independent DAG that re-downloaded and re-parsed the same
-- export. lido_id is the LIDO bwb/id/ URI with the fixed prefix stripped.
CREATE TABLE IF NOT EXISTS law_element (
    id           INTEGER PRIMARY KEY,
    type         TEXT,
    bwb_id       TEXT,
    bwb_label_id TEXT,
    lido_id      TEXT UNIQUE,
    jc_id        TEXT,
    number       TEXT,
    title        TEXT
);

-- Resolved case -> law_element links, produced by resolve_case_law_links()
-- after both metadata and law_element are fully populated (a case's citation
-- may be parsed before or after the law element it points to, since the
-- export isn't subject-sorted). Natural-keyed (ecli, law_lido_id) rather than
-- surrogate ids, so downstream consumers can join straight against ecli/lido_id.
CREATE TABLE IF NOT EXISTS case_law (
    id          INTEGER PRIMARY KEY,
    ecli        TEXT,
    law_lido_id TEXT,
    source      TEXT,  -- 'lido-ref' (refereertAan) | 'lido-linkt' (linkt)
    jc_id       TEXT,
    opschrift   TEXT
);

-- BWB alias titles (e.g. "BW" -> BWBR0005289), from a separate download
-- (BWBIdList.xml.zip), unrelated to the ttl.gz export. See bwbidlist.py.
CREATE TABLE IF NOT EXISTS law_alias (
    id     INTEGER PRIMARY KEY,
    alias  TEXT,
    bwb_id TEXT,
    source TEXT  -- 'opschrift' | 'bwbidlist'
);
