![](docs/img/radiowave.png)

[![tests](https://github.com/marcdacosta/spectrum-wrangler/actions/workflows/tests.yml/badge.svg)](https://github.com/marcdacosta/spectrum-wrangler/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/spectrum-wrangler)](https://pypi.org/project/spectrum-wrangler/)

# What is Spectrum Wrangler?
Spectrum Wrangler will download the most recent version of the FCC's public licensing data, parse it into a local database, clean up the geo columns so they can be used to query on, and produce a spatial index on them.

# About the FCC licensing data
In the United States the Federal Communication Commission is responsible for regulating the use of the radio waves. The FCC issues licenses for radio transmitters that exceed certain power levels or which operate on certain frequencies. Those licenses are public, and they say who is allowed to transmit, where, on what frequency, and with how much power. Taken together they are a map of who owns the air.

The original version of this project used the [License View](http://reboot.fcc.gov/license-view/) database, which harmonized the schema of the various parts of the [Universal Licensing System](http://wireless.fcc.gov/uls/index.htm?job=transaction&page=weekly) (ULS) into one flat CSV. The FCC no longer maintains License View. Spectrum Wrangler now reads ULS directly, which is the same data with more detail and no denormalization: a license header, its licensee, its locations, its antennas, its frequency assignments, and its emissions all arrive as separate related records.

The FCC snapshot published August 23, 2026 holds 5.2M licenses, 2.8M of them active, across 129 radio services — 6.6M licensee records, 3.3M transmitter locations (2.4M with usable coordinates), and 11.4M frequency assignments. Your own numbers will differ; `status` reports what you actually loaded.

# How to use
* Install it: `uv tool install spectrum-wrangler`, `pipx install spectrum-wrangler`, or `pip install spectrum-wrangler`. Python 3.11 or newer is the only requirement, and a clone of this repository works identically without installing (see below).
* Run `spectrum-wrangler refresh` to download and index every current weekly archive.
* Run `spectrum-wrangler refresh --archive paging` first if you want a small database to try.
* Query it with `spectrum-wrangler <command>`, or point an AI agent at it (see below).
* Read [the querying guide](docs/QUERYING.md) for how the records join and what to watch out for.

The complete download is about 1.2 GB compressed and produces a database of roughly 23 GB once every source field is retained and indexed. Downloads are cached and content-hashed, so re-running `refresh` re-imports only what actually changed. Use `--normalized-only` for a much smaller database when you do not need every raw FCC field.

The database, download cache, and provenance manifest live in a per-user data directory: `~/Library/Application Support/spectrum-wrangler` on macOS, `$XDG_DATA_HOME/spectrum-wrangler` (usually under `~/.local/share`) on Linux, `%LOCALAPPDATA%\spectrum-wrangler` on Windows. A database already built under `./data` in a repository checkout keeps being used, and `--database` or the `SPECTRUM_WRANGLER_DB` environment variable points anywhere else.

Generated archives and databases stay out of Git. [data/source-manifest.json](data/source-manifest.json) is committed instead, and records each archive's official URL, FCC publication time, retrieval time, ETag, SHA-256, byte size, record counts, and any observed schema drift.

# Do I need Docker?
No. A project to Dockerize `spectrum-wrangler` can be found [here](https://github.com/brannondorsey/spectrum-wrangler-docker), and it made sense when this tool required a PostgreSQL server with the PostGIS extension installed and configured. That was the hard part, and a container solved it.

Version 0.3 has no services and no dependencies, so a container would add a step rather than remove one — and the expensive artifact is the 23 GB database, which has to live on your disk either way. The Docker wrapper targets the retired PostgreSQL/License View workflow and is kept as a historical reference, not the current path.

# Requirements
* **Python 3.11 or newer, and nothing else.** SQLite, FTS5, and RTree ship with Python. PostgreSQL, PostGIS, and Docker are no longer needed.
* **Disk: about 25 GB** for a full load — a 23 GB database plus a 1.25 GB download cache. `--normalized-only` cuts the database to roughly a third of that.
* **Memory: under 600 MB.** The importer streams, so memory tracks the widest record rather than the size of the archive. Measured peaks: 77 MB for the smallest archive, 537 MB for the largest (35.4M records). An 8 GB laptop is comfortable.
* **Time: roughly 15 minutes** of import for the full set on a modern laptop once downloaded, plus the download itself. Importing runs at about 130,000 records per second.

Install it from PyPI:

    uv tool install spectrum-wrangler        # or: pipx install spectrum-wrangler

Or clone the repository and run it in place, no install step at all:

    git clone https://github.com/marcdacosta/spectrum-wrangler.git
    cd spectrum-wrangler
    python3 -m spectrum_wrangler sources

The two are the same program: `python3 -m spectrum_wrangler` from a checkout accepts every command written as `spectrum-wrangler` in the examples here.

## Start small
One archive is enough to learn the shape of the data, and the smallest is a four-second import:

| Archive | Download | Records | Database |
|---|---|---|---|
| `--archive paging` | 6.5 MB | 750K | 142 MB |
| `--archive amateur` | 198 MB | 10.5M | 2.3 GB (710 MB with `--normalized-only`) |
| everything | 1.25 GB | 105.9M | 23 GB |

Run `spectrum-wrangler sources` to see the full list of 14 archives.

# Examples
Example raw data extract from the original License View publication can be found in `sample-fcc.csv`. [The querying guide](docs/QUERYING.md) has worked investigations and the join semantics behind these.

## Query to check what is loaded and how current it is

    spectrum-wrangler status

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
Query to search within 1000m of [40.7253319,-74.0076834](https://www.google.com/maps/search/40.7253319,-74.0076834?sa=X&ved=0ahUKEwjj06n16p_WAhWR8oMKHUMfA9oQ8gEIJzAA) and export the results:

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

Earlier versions shipped a Model Context Protocol server. It was removed in favour of the CLI plus the skill: it duplicated every parameter declaration in a third place, needed a client restart to pick up changes, and had nowhere to put the judgment that actually matters here. If you need to query this from a client with no shell, that is the one thing the MCP server did better.

# Current state
Version 0.3 is a rewrite of the ingest and query layers. The purpose has not changed: get the public licensing dataset, clean its geographic fields, index it, and make it useful to query.

What changed:

* **Source.** FCC License View is retired; ULS weekly complete files replace it. See [the 2026 publication research](docs/RESEARCH-2026.md).
* **Storage.** PostgreSQL and PostGIS are gone. SQLite with FTS5 and an RTree index does the same work with nothing to install. See [migration notes](docs/MIGRATION.md).
* **Fidelity.** Every field of all 89 documented ULS record layouts is preserved in `raw_*` tables; the common ones are also normalized into typed, indexed tables (`licenses`, `entities`, `locations`, `antennas`, `frequencies`, `emissions`, `amateur`). Raw records join on `unique_system_identifier`. Undocumented trailing fields are kept in `extra_fields_json` rather than dropped, so FCC publication drift is visible instead of silent.
* **Provenance.** Every import records where the file came from, when the FCC published it, when it was fetched, and its hash.

Known limits:

* Full-power AM/FM/TV station licensing moved to the FCC's separate Licensing and Management System (LMS) and is **not** in this database. ULS still carries broadcast auxiliary. LMS is deliberately not mixed in.
* Rebuilds use the weekly complete files. Daily transaction deltas are not applied yet.
* Coordinates come from the FCC as published; about a quarter of location records have no usable coordinate.
* `load.py` and `sample-fcc.csv` are the original Python 2 loader and a License View sample. They are kept for reference and do not run against current data.

## What this data contains
Everything here is FCC public record, published for anyone to download, and Spectrum Wrangler queries all of it. There is no column gate: the database is a local file you can open with `sqlite3`, so a gate would restrict nothing while breaking legitimate lookups.

Be aware of what that means in bulk. Entity records carry licensee contact details, and the amateur archive alone is roughly 1.7 million individual licensees with names and mailing addresses. Public record, but a bulk extract of private individuals is a different thing from a single lookup — worth a thought before you republish one.

Read-only *is* enforced, at the SQLite connection rather than by convention: a write is refused by the engine.

# Development

    python3 -m unittest discover -s tests -v
    python3 -m compileall -q spectrum_wrangler tests

The suite is hermetic — no network, no fixtures on disk, under a second — and CI runs it on Python 3.11 through 3.14 across Linux, macOS, and Windows.

The only build dependency is the pinned `setuptools==80.9.0`. The application itself has no third-party runtime dependencies, and [the dependency policy](docs/DEPENDENCIES.md) explains what it takes to add one.
