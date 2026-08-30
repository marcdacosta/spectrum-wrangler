![](docs/img/radiowave.png)

[![tests](https://github.com/marcdacosta/spectrum-wrangler/actions/workflows/tests.yml/badge.svg)](https://github.com/marcdacosta/spectrum-wrangler/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/spectrum-wrangler)](https://pypi.org/project/spectrum-wrangler/)

# Spectrum Wrangler

In the United States the airwaves are public property. The Federal Communications Commission administers them on the public's behalf, issuing licenses for transmitters that exceed certain power levels or operate on certain frequencies, and those licenses are public record: they say who is allowed to transmit, where, on what frequency, and with how much power. Taken together they are a map of who owns the air.

That map is worth reading because radio infrastructure is built to go unnoticed. Unmarked antennas on rooftops, encrypted channels, microwave links running over your head — the physical nervous system of police departments, railroads, utilities, carriers, and federal agencies is everywhere and announces itself nowhere, except here. The license record is the one place each grant of the public's spectrum to a particular interest is written down, and it makes pointed questions answerable: who transmits from this building, what a police department actually operates, how much of a band one company has quietly amassed in a city, which authorizations are about to lapse. When the air is allocated in public, the public can audit the allocation.

Spectrum Wrangler turns that record into something you can query. It downloads the current weekly archives of the FCC's [Universal Licensing System](https://www.fcc.gov/wireless/universal-licensing-system), builds a local SQLite database with clean WGS84 coordinates, a spatial index, and full-text search, and gives you — or an AI agent — one read-only CLI to investigate it with. Every import records where each file came from, when the FCC published it, and its hash, so every answer can cite its source.

The FCC snapshot published August 23, 2026 holds 5.2M licenses, 2.8M of them active, across 129 radio services — 6.6M licensee records, 3.3M transmitter locations (2.4M with usable coordinates), and 11.4M frequency assignments. Your own numbers will differ; `status` reports what you actually loaded.

# Getting started

Install it — Python 3.11 or newer is the only requirement:

    uv tool install spectrum-wrangler        # or: pipx install spectrum-wrangler

Or clone the repository and run it in place, no install step at all: `python3 -m spectrum_wrangler` from a checkout accepts every command written as `spectrum-wrangler` here.

Then initialize it — `init` creates the database and loads a small starter archive, so there is something to query within seconds — and look at what you have:

    spectrum-wrangler init
    spectrum-wrangler status

One archive is enough to learn the shape of the data:

| Archive | Download | Records | Database |
|---|---|---|---|
| `--archive paging` | 6.5 MB | 750K | 142 MB |
| `--archive amateur` | 198 MB | 10.5M | 2.3 GB (710 MB with `--normalized-only`) |
| everything | 1.25 GB | 105.9M | 23 GB |

`spectrum-wrangler refresh` with no arguments downloads and indexes all 14 current weekly archives; `sources` lists them. Downloads are cached and content-hashed, so re-running `refresh` re-imports only what actually changed. Use `--normalized-only` for a much smaller database when you do not need every raw FCC field.

The database, download cache, and provenance manifest live in a per-user data directory: `~/Library/Application Support/spectrum-wrangler` on macOS, `$XDG_DATA_HOME/spectrum-wrangler` (usually under `~/.local/share`) on Linux, `%LOCALAPPDATA%\spectrum-wrangler` on Windows. A database already built under `./data` in a repository checkout keeps being used, and `--database` or the `SPECTRUM_WRANGLER_DB` environment variable points anywhere else.

Generated archives and databases stay out of Git. [data/source-manifest.json](data/source-manifest.json) is committed instead, and records each archive's official URL, FCC publication time, retrieval time, ETag, SHA-256, byte size, record counts, and any observed schema drift.

## Requirements

* **Python 3.11 or newer, and nothing else.** SQLite, FTS5, and RTree ship with Python; there are no services to run and no third-party packages.
* **Disk: about 25 GB** for a full load — a 23 GB database plus a 1.25 GB download cache. `--normalized-only` cuts the database to roughly a third of that.
* **Memory: under 600 MB.** The importer streams, so memory tracks the widest record rather than the size of the archive. Measured peaks: 77 MB for the smallest archive, 537 MB for the largest (35.4M records). An 8 GB laptop is comfortable.
* **Time: roughly 15 minutes** of import for the full set on a modern laptop once downloaded, plus the download itself. Importing runs at about 130,000 records per second.

# Examples

[The querying guide](docs/QUERYING.md) has worked investigations, the join semantics behind these, and the traps in the data worth knowing before you publish a number.

## Look up a call sign

    spectrum-wrangler callsign W1AW

    [
      {
        "callsign": "W1AW",
        "display_name": "ARRL HQ OPERATORS CLUB",
        "radio_service_code": "HA",
        "state": "CT",
        "license_status": "A",
        "grant_date": "12/08/2020",
        "expired_date": "02/26/2031",
        "unique_system_id": 780866
      }
    ]

## Ranged queries

Search within 1000m of [40.7253319,-74.0076834](https://www.google.com/maps/search/40.7253319,-74.0076834?sa=X&ved=0ahUKEwjj06n16p_WAhWR8oMKHUMfA9oQ8gEIJzAA) and export the results:

    spectrum-wrangler --format csv \
      nearby 40.7253319 -74.0076834 --radius-km 1 > /tmp/antennas.csv

Or in SQL, against the same database:

    spectrum-wrangler sql \
      "SELECT radio_service_code, count(*) AS licenses
         FROM licenses GROUP BY 1 ORDER BY 2 DESC"

# How agents use it

Spectrum Wrangler is one CLI, used the same way by a person and by an agent. Every operation is declared once in the code, so the commands, the help text, and the machine-readable manifest cannot drift apart.

    spectrum-wrangler capabilities

`capabilities` describes every command, argument, output format, and exit code as JSON, so an agent can discover the whole surface in one call. Output adapts to the consumer — a table on a terminal, JSON when piped — and `--format table|json|ndjson|csv` always wins.

    # everything about one licence, in one call
    spectrum-wrangler license --callsign W1AW

    # who else transmits from this building
    spectrum-wrangler nearby 40.748444 -73.985694 --radius-km 0.1

    # who holds a band, and who holds a name
    spectrum-wrangler band 462 468 --group-by licensee
    spectrum-wrangler organization --name "NEW YORK CITY POLICE"

    # what expires this quarter, as CSV
    spectrum-wrangler --format csv expirations --start 2026-10-01 --end 2026-12-31 --state NY

Exit codes separate the cases a program must tell apart: `0` success, `1` understood but nothing matched, `2` bad request. Errors are JSON on stderr in machine formats. The `spectrum-wrangler-agent` entry point is the same CLI with output pinned to JSON whether or not it is attached to a terminal, and [AGENTS.md](AGENTS.md) is the in-repository briefing for agents working on or with the code.

The CLI is read-only and enforces it at the SQLite connection rather than by convention. Custom SQL accepts one `SELECT`, `WITH`, or `EXPLAIN`, runs with a timeout, and returns at most 1,000 rows.

## The skill

[`skills/spectrum-wrangler/SKILL.md`](skills/spectrum-wrangler/SKILL.md) is the companion to the CLI, and it carries the part a tool schema cannot: what will make you confidently wrong about this dataset. Joining on `unique_system_id` alone turns one call sign into 1.8 million rows. Dates are `MM/DD/YYYY` text, so sorting them sorts by month. Counting what one organization holds has no exact answer. Point any agent at it.

To install the skill for Claude Code, link it into your skills directory:

    mkdir -p ~/.claude/skills
    ln -s "$PWD/skills/spectrum-wrangler" ~/.claude/skills/spectrum-wrangler

# The data

Coverage is ULS wireless licensing: land mobile, microwave, cellular, paging, amateur, GMRS, aircraft, ship, coast and aviation ground, market-based, and broadcast auxiliary services. Every field of all 89 documented ULS record layouts is preserved verbatim in `raw_*` tables; the common records are also normalized into typed, indexed tables (`licenses`, `entities`, `locations`, `antennas`, `frequencies`, `emissions`, `amateur`). Undocumented trailing fields are kept in `extra_fields_json` rather than dropped, so FCC publication drift is visible instead of silent.

Limits worth knowing:

* Full-power AM/FM/TV station licensing lives in the FCC's separate Licensing and Management System (LMS) and is **not** in this database. ULS carries broadcast *auxiliary* only, and LMS is deliberately not mixed in.
* Rebuilds use the weekly complete files, so the data is up to a week behind; daily transaction deltas are not applied yet.
* Coordinates come from the FCC as published; about a quarter of location records have no usable coordinate.

## Public record, in bulk

Everything here is FCC public record, published for anyone to download, and Spectrum Wrangler queries all of it. There is no column gate: the database is a local file you can open with `sqlite3`, so a gate would restrict nothing while breaking legitimate lookups.

Be aware of what that means in bulk. Entity records carry licensee contact details, and the amateur archive alone is roughly 1.7 million individual licensees with names and mailing addresses. Public record, but a bulk extract of private individuals is a different thing from a single lookup — worth a thought before you republish one.

Read-only *is* enforced, at the SQLite connection rather than by convention: a write is refused by the engine.

# Development

    python3 -m unittest discover -s tests -v
    python3 -m compileall -q spectrum_wrangler tests

The suite is hermetic — no network, no fixtures on disk, under a second — and CI runs it on Python 3.11 through 3.14 across Linux, macOS, and Windows.

The only build dependency is the pinned `setuptools==80.9.0`. The application itself has no third-party runtime dependencies, and [the dependency policy](docs/DEPENDENCIES.md) explains what it takes to add one.

# History

This project began in 2017 as a loader for FCC License View, a denormalized CSV the FCC has since retired, and originally required PostgreSQL with PostGIS — which is what the community [Docker wrapper](https://github.com/brannondorsey/spectrum-wrangler-docker) existed to set up. Version 0.3 is a ground-up rewrite against the live ULS publication, with SQLite doing the spatial and text indexing and one CLI replacing an earlier MCP server; no container is needed. The evidence behind the migration is in [docs/RESEARCH-2026.md](docs/RESEARCH-2026.md), the notes for people who ran the 2017 loader are in [docs/MIGRATION.md](docs/MIGRATION.md), and `load.py` and `sample-fcc.csv` are that era's artifacts, kept for reference.
