# FCC spectrum data in 2026: what replaced License View

Date: August 28, 2026
Audience: Spectrum Wrangler maintainers and data/agent developers

This document records the research that informed the FCC data migration. It is
an archived record of what was verified on that date, not a maintained status
page: directory listings, archive counts, and observed drift are stated as they
were found in August 2026. For what is loaded on your machine right now, run
`spectrum-wrangler status`. For what the migration means if you used the 2017
loader, see [migration notes](MIGRATION.md).

## Executive answer

FCC License View has not merely moved. In a July 1, 2025 response, the FCC Wireless Telecommunications Bureau stated that the requested License View database “is no longer maintained.” The repository therefore cannot become reliable again by finding a new URL for the old CSV. [FCC FOIA response 2025-000790](https://cdn.muckrock.com/foia_files/2025/07/01/FOIA_Response_2025-000790.pdf)

For wireless licensing, the supported bulk source is the FCC Universal Licensing System (ULS) public-access publication. ULS provides service-specific ZIP archives containing pipe-delimited, two-character record files such as `HD.dat`, `EN.dat`, `LO.dat`, and `FR.dat`. Complete files are produced weekly and transaction files carry weekday changes. [FCC ULS Database Public Access Files](https://wireless.fcc.gov/uls/documentation/pa_intro24.pdf)

Spectrum Wrangler 0.3 now ingests the live [FCC complete-file directory](https://data.fcc.gov/download/pub/uls/complete/), retains all 89 documented record layouts, builds fast normalized indexes, records file-level provenance, and exposes the result through a read-only query surface — at the time of writing an MCP server, since replaced by the CLI and skill (see the later note below).

## What changed in the publication

License View presented a convenient denormalized row model. ULS publishes the underlying relational structure instead. A license header, entities, locations, antennas, frequency assignments, emissions, special conditions, paths, leases, and service-specific facts live in separate record files. The FCC identifies `unique_system_identifier` as a durable join key; technical records additionally use call sign and service-dependent location, antenna, frequency, path, or segment numbers. [FCC ULS public-access documentation](https://wireless.fcc.gov/uls/documentation/pa_intro24.pdf)

The official directory currently lists 14 complete license archives. During verification on August 28, 2026, all 14 resolved successfully and reported `Last-Modified` dates of Sunday, August 23, 2026. Spectrum Wrangler stores those server dates alongside retrieval time, ETag, SHA-256, compressed byte size, and per-table row counts. The committed manifest is therefore an audit record, not a claim that retrieval time equals publication time.

The checked-in schema derives from the FCC Public Access Database Definitions dated April 17, 2025, available through the [FCC Public Access Files download hub](https://www.fcc.gov/wireless/data/public-access-files-database-downloads). Because data can evolve before documentation, each raw table also has `extra_fields_json`; unknown trailing fields remain recoverable instead of being dropped or shifting named columns.

That safeguard is already necessary. In the August 23 snapshots, `EC.dat` has 10 fields in five archives while the April 2025 definition names eight; the two undocumented trailing values are retained as JSON. FCC free-text fields can also continue across physical lines without CSV-style quoting, so the loader reconstructs logical records before measuring their shape. These are direct observations from the [current FCC archives](https://data.fcc.gov/download/pub/uls/complete/), not guessed field meanings, and per-source drift metrics make discrepancies visible without inventing names.

## Coverage is strong but not identical

ULS is the correct replacement for License View's wireless records, including land mobile, microwave, market-based, cellular, paging, amateur, GMRS, aircraft, ship, coast/aviation ground, and broadcast-auxiliary licensing.

It is not a complete reconstruction of every source License View once combined. The legacy sample in this repository includes `CDBS` AM/FM/TV station rows. Modern broadcast station licensing is handled by the separate FCC Licensing and Management System (LMS), whose public interface includes database files. [FCC LMS FAQ](https://enterpriseefiling.fcc.gov/dataentry/api/download/faq)

The LMS endpoint redirected to FCC system maintenance during final verification, so this release does not add an unverified broadcast importer. The MCP guide and status output explicitly say that full-power broadcast coverage is absent. That limitation is preferable to silently describing a ULS-only result as the old cross-system dataset.

## Why the agent surface uses MCP and SQLite

> **Later note.** The MCP server described below was removed after this was
> written. It duplicated every parameter declaration in a third place, needed a
> client restart to pick up changes, and had nowhere to carry the dataset
> judgment that turned out to matter most. The CLI plus
> [the skill](../skills/spectrum-wrangler/SKILL.md) replaced it. The reasoning
> about SQLite, the read-only authorizer, and bounded SQL still stands.

The Model Context Protocol lets servers publish discoverable, JSON-Schema-described tools that models can invoke. Its July 28, 2026 revision replaced the initialization handshake with stateless per-request metadata and added `server/discover` plus cache hints for list results. [MCP 2026-07-28 release](https://blog.modelcontextprotocol.io/posts/2026-07-28/)

Spectrum Wrangler exposes provenance/status first, exact schema discovery, structured callsign/license/frequency/proximity searches, and custom SQL. SQL runs through a SQLite URI opened in read-only mode, with `query_only`, an operation authorizer, a progress timeout, a 1,000-row ceiling, and a single-statement allow list. The structured tools return narrower projections. All original public FCC fields remain available locally in named `raw_*` tables; MCP denies raw contact/address/FRN and overflow fields unless its local operator explicitly enables sensitive access.

Docker PostgreSQL was evaluated but was not necessary. Python already supplies SQLite, the data is local and read-heavy, FTS5 covers text indexing, and RTree plus a Haversine check replaces the old PostGIS proximity query without a service lifecycle or added runtime packages.

## Operational recommendations

1. Run `spectrum_status` before analysis and include loaded FCC publication dates in conclusions.
2. Use normalized tables for common work and raw tables for source-level audit or uncommon fields.
3. Run a complete weekly refresh after the FCC publishes Sunday snapshots. Daily-delta ingestion is future work.
4. Keep the generated database local: ULS entity records can contain public phone, email, address, ZIP, and FRN fields.
5. Treat empty results as “not present in the loaded ULS snapshots,” not proof that no relevant FCC authorization exists.
6. Add LMS only as a separate, source-labeled ingestion domain after its current database files and schemas can be verified end to end.

## Bottom line

The repository is operational again against a current authoritative publication and is substantially more useful to agents than the old one-shot loader: the data is reproducible, relationally faithful, spatially searchable, fully schema-discoverable, and read-only at the FCC query boundary. Its one material continuity gap—modern broadcast station licensing—is explicit and isolated for future LMS work.
