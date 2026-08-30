# Changelog

## 0.3.2 — 2026-08-30

### Added
* Progressive disclosure on parse errors: bare `spectrum-wrangler` prints the
  full command surface, and an incomplete subcommand (`spectrum-wrangler
  nearby`) prints that command's arguments, defaults, and examples, with the
  specific error last. Help goes to stderr, piped stdout stays machine-clean,
  and the exit code remains 2.

## 0.3.1 — 2026-08-30

### Added
* `init` is now first-time setup: on an empty database it downloads and imports
  a small starter archive (paging, about 6.5 MB) so there is something to query
  immediately. `--archive` loads a different one instead, and an already-loaded
  database is left alone. The root `--help`, the missing-database error, and the
  `capabilities` guidance all point new users at it.
* `--format` and `--database` are accepted after the subcommand as well as
  before it, where habit tends to put them.
* `init` and `refresh` narrate the job: an upfront plan with expected sizes and
  time, a numbered stage per archive, live download and import progress with
  rates on an interactive terminal (spinner, in-place redraw, colour unless
  `NO_COLOR`), and a timed summary. Piped, every step is one plain line with no
  escape codes, so logs and agents are unaffected.

## 0.3.0 — 2026-08-30

The rewrite release: same mission as 2017, current FCC publication, nothing to
install. First release published to PyPI.

### Changed
* **Source.** FCC License View is retired; the tool now reads the FCC's ULS
  weekly complete archives directly. See [docs/RESEARCH-2026.md](docs/RESEARCH-2026.md).
* **Storage.** PostgreSQL and PostGIS replaced by SQLite with FTS5 and RTree —
  Python 3.11+ standard library only, no services, no third-party packages.
  See [docs/MIGRATION.md](docs/MIGRATION.md).
* The database, download cache, and provenance manifest now default to a
  per-user data directory (`~/Library/Application Support/spectrum-wrangler`
  on macOS, `$XDG_DATA_HOME/spectrum-wrangler` on Linux,
  `%LOCALAPPDATA%\spectrum-wrangler` on Windows), so the installed CLI works
  from any directory. A database already built under `./data` keeps working,
  and `$SPECTRUM_WRANGLER_DB` or `--database` overrides both.

### Added
* One CLI for people and agents: structured commands, a `capabilities`
  manifest generated from the same declarations as the argparse tree, output
  as table/JSON/NDJSON/CSV, distinct exit codes, and JSON errors on stderr.
* Read-only enforcement at the SQLite connection, with bounded single-statement
  SQL (`SELECT`/`WITH`/`EXPLAIN`, timeout, 1,000-row cap).
* Full-fidelity import: all 89 documented ULS record layouts kept verbatim in
  `raw_*` tables, common records normalized and indexed, publication drift
  preserved in `extra_fields_json`.
* Per-archive provenance: URL, FCC publication time, retrieval time, ETag,
  SHA-256, byte size, and record counts, written to `source-manifest.json`.
* An agent skill ([skills/spectrum-wrangler/SKILL.md](skills/spectrum-wrangler/SKILL.md))
  carrying the dataset judgment a tool schema cannot.
* Byte-count download progress on an interactive terminal.
* CI across Python 3.11–3.14 on Linux, macOS, and Windows.

### Removed
* The Model Context Protocol server, in favour of the CLI plus the skill.
* The PostgreSQL/PostGIS/Docker requirement.

## 0.2.x and earlier — 2017

The original License View loader: Python 2, `requests`, `psycopg2`, and a
PostGIS-backed import of the FCC's now-retired denormalized CSV. Kept for
reference as `load.py`.
