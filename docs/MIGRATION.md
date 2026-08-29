# Migration from FCC License View

This is for people who ran the 2017 loader. It covers what changed and what to
expect; the evidence behind the decision — the FCC's response, the ULS
publication format, the LMS boundary — is recorded once in
[the 2026 research notes](RESEARCH-2026.md) rather than repeated here.

The 2017 loader downloaded one `fcc_lic_vw.csv`, created a hand-written Postgres table, converted degree/minute/second coordinates in PL/pgSQL, and required PostGIS. That path cannot be repaired by changing its URL: the FCC has confirmed License View is no longer maintained.

Version 0.3 therefore changes both the publication and storage model.

| Legacy behavior | 2026 behavior |
|---|---|
| Retired License View CSV | Live FCC ULS weekly license archives |
| One denormalized table | 89 lossless raw record tables plus normalized query tables |
| Python 2, `requests`, `psycopg2` | Python 3.11+ standard library only |
| Config embedded in `load.py` | CLI flags and stable defaults |
| Required Postgres/PostGIS | Portable SQLite with FTS5 and RTree |
| Destructive table replacement | Per-archive replacement with content-hash reuse |
| No source provenance | URL, timestamps, ETag, SHA-256, bytes, and counts |
| Human SQL only | CLI plus privacy-filtered, bounded MCP tools/resources/prompts |

## Compatibility boundaries

The normalized `licenses`, `entities`, `locations`, `antennas`, `frequencies`, and `emissions` tables cover the same practical query families as the old `fcclicenses` table. Their identifiers are intentionally closer to the official ULS names.

The old flat file could multiply one license across locations, antennas, frequencies, and emissions. The new model keeps those relations separate, so queries express the intended cardinality rather than inheriting accidental row multiplication.

Coordinates are converted to decimal WGS84 values during normalization and the original DMS components remain in `raw_lo`. RTree supplies the bounding-box phase of proximity queries and a Haversine calculation supplies exact great-circle distance; no spatial extension is required.

## Raw fidelity and schema drift

The importer keeps every field described by the FCC's published record definitions; the checked-in `uls_schema.json` defines all 89 layouts. Raw values remain text so identifiers, leading zeroes, source date formats, and precision are not altered.

An FCC record with fewer fields is padded with nulls. Newly added trailing fields are serialized in `extra_fields_json`, making publication drift visible and recoverable instead of shifting columns or dropping data. Completely new record types go to `raw_unknown` until their official definitions are reviewed. [The research notes](RESEARCH-2026.md) give a worked example of drift already observed in the published archives.

Some FCC free-text fields span unescaped physical lines. The logical-record parser recognizes the member's two-character record prefix, joins continuation text back into the preceding field with newlines, and skips physical blank separators. `raw_parser_version` in source provenance prevents an older line-oriented import from being mistaken for a current verified snapshot.

## Broadcast limitation

License View combined more than ULS. Its sample in this repository includes CDBS broadcast stations; the modern replacement for those station records is LMS, not ULS. ULS still contains broadcast auxiliary services (`l_LMbcast.zip`).

If you previously queried this database for full-power AM/FM/TV stations, that is the one query family version 0.3 cannot answer. It is labelled rather than silently approximated, and LMS is not mixed in.

## Legacy files

`load.py` remains available for historical comparison. It is Python 2 code, points at the retired publication, and must not be used for a current refresh. `sample-fcc.csv` is likewise a legacy License View sample rather than a current test fixture.
