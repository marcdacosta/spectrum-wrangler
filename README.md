![](docs/img/radiowave.png)

# What is Spectrum Wrangler?
Spectrum Wrangler will download the most recent version of the FCC's public licensing data, parse it into a local database, clean up the geo columns so they can be used to query on, and produce a spatial index on them.

# About the FCC licensing data
In the United States the Federal Communication Commission is responsible for regulating the use of the radio waves. The FCC issues licenses for radio transmitters that exceed certain power levels or which operate on certain frequencies. Those licenses are public, and they say who is allowed to transmit, where, on what frequency, and with how much power. Taken together they are a map of who owns the air.

The original version of this project used the [License View](http://reboot.fcc.gov/license-view/) database, which harmonized the schema of the various parts of the [Universal Licensing System](http://wireless.fcc.gov/uls/index.htm?job=transaction&page=weekly) (ULS) into one flat CSV. The FCC no longer maintains License View. Spectrum Wrangler now reads ULS directly, which is the same data with more detail and no denormalization: a license header, its licensee, its locations, its antennas, its frequency assignments, and its emissions all arrive as separate related records.

The FCC snapshot published August 23, 2026 holds 5.2M licenses, 2.8M of them active, across 129 radio services — 6.6M licensee records, 3.3M transmitter locations (2.4M with usable coordinates), and 11.4M frequency assignments. Your own numbers will differ; `status` reports what you actually loaded.

# How to use
* Install Python 3.11 or newer. There is nothing else to install.
* Run `python3 -m spectrum_wrangler refresh` to download and index every current weekly archive.
* Run `python3 -m spectrum_wrangler refresh --archive paging` first if you want a small database to try.
* Query it with `python3 -m spectrum_wrangler <command>`, or point an AI agent at it (see below).
* Read [the querying guide](docs/QUERYING.md) for how the records join and what to watch out for.

The complete download is about 1.2 GB compressed and produces a database of roughly 23 GB once every source field is retained and indexed. Downloads are cached and content-hashed, so re-running `refresh` re-imports only what actually changed. Use `--normalized-only` for a much smaller database when you do not need every raw FCC field.

Generated archives and databases stay out of Git. [data/source-manifest.json](data/source-manifest.json) is committed instead, and records each archive's official URL, FCC publication time, retrieval time, ETag, SHA-256, byte size, record counts, and any observed schema drift.

# Do I need Docker?
No. A project to Dockerize `spectrum-wrangler` can be found [here](https://github.com/brannondorsey/spectrum-wrangler-docker), and it made sense when this tool required a PostgreSQL server with the PostGIS extension installed and configured. That was the hard part, and a container solved it.

Version 0.3 has no services and no dependencies, so a container would add a step rather than remove one — and the expensive artifact is the 23 GB database, which has to live on your disk either way. The Docker wrapper targets the retired PostgreSQL/License View workflow and is kept as a historical reference, not the current path.

# Requirements
* **Python 3.11 or newer, and nothing else.** SQLite, FTS5, and RTree ship with Python. PostgreSQL, PostGIS, and Docker are no longer needed.
* **Disk: about 25 GB** for a full load — a 23 GB database plus a 1.25 GB download cache. `--normalized-only` cuts the database to roughly a third of that.
* **Memory: under 600 MB.** The importer streams, so memory tracks the widest record rather than the size of the archive. Measured peaks: 77 MB for the smallest archive, 537 MB for the largest (35.4M records). An 8 GB laptop is comfortable.
* **Time: roughly 15 minutes** of import for the full set on a modern laptop once downloaded, plus the download itself. Importing runs at about 130,000 records per second.

Nothing needs installing. Clone the repository and run it in place:

    git clone https://github.com/marcdacosta/spectrum-wrangler.git
    cd spectrum-wrangler
    python3 -m spectrum_wrangler sources

`pip install -e .` additionally puts `spectrum-wrangler`, `spectrum-wrangler-agent`, and `spectrum-wrangler-mcp` on your PATH. Every example here uses the `python3 -m` form so it works either way.

## Start small
One archive is enough to learn the shape of the data, and the smallest is a four-second import:

| Archive | Download | Records | Database |
|---|---|---|---|
| `--archive paging` | 6.5 MB | 750K | 142 MB |
| `--archive amateur` | 198 MB | 10.5M | 2.3 GB (710 MB with `--normalized-only`) |
| everything | 1.25 GB | 105.9M | 23 GB |

Run `python3 -m spectrum_wrangler sources` to see the full list of 14 archives.

# Examples
Example raw data extract from the original License View publication can be found in `sample-fcc.csv`. [The querying guide](docs/QUERYING.md) has worked investigations and the join semantics behind these.

## Query to check what is loaded and how current it is

    python3 -m spectrum_wrangler status

## Look up a call sign

    python3 -m spectrum_wrangler callsign W1AW

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

    python3 -m spectrum_wrangler.agent --format csv \
      nearby 40.7253319 -74.0076834 --radius-km 1 > /tmp/antennas.csv

Or in SQL, against the same database:

    python3 -m spectrum_wrangler sql \
      "SELECT radio_service_code, count(*) AS licenses
         FROM licenses GROUP BY 1 ORDER BY 2 DESC"

# How agents use it
Spectrum Wrangler ships two read-only surfaces built for programs rather than people. Both share one query layer, so an agent and a person asking the same question get the same answer.

**A Model Context Protocol server.** The checked-in [`.mcp.json`](.mcp.json) starts it; the equivalent command is `python3 -m spectrum_wrangler mcp`. It speaks MCP `2026-07-28` and still accepts the `2025-11-25`, `2025-06-18`, and `2024-11-05` handshakes. Thirteen tools cover provenance (`spectrum_status`), discovery (`describe_schema`, `list_radio_services`), point lookups (`lookup_callsign`, `search_licenses`, `search_text`, `search_frequency`, `search_nearby_sites`), whole records (`get_license_record`), aggregates (`survey_band`, `summarize_geography`, `list_expirations`), and bounded SQL (`query_spectrum_sql`). It also publishes guide, schema, and provenance resources and an `investigate_spectrum` prompt.

**An agent CLI**, for agents that drive a shell instead of MCP:

    python3 -m spectrum_wrangler.agent capabilities

`capabilities` describes every command, argument, output format, and exit code, so an agent can discover the whole surface in one call. The commands mirror the MCP tools:

    # everything about one license, in one call
    python3 -m spectrum_wrangler.agent license --callsign W1AW

    # who holds assignments across a band
    python3 -m spectrum_wrangler.agent band 462 468 --group-by licensee

    # full-text search, streamed as one JSON object per line
    python3 -m spectrum_wrangler.agent --format ndjson text 'fire AND department'

    # what expires this quarter, as CSV
    python3 -m spectrum_wrangler.agent --format csv \
      expirations --start 2026-10-01 --end 2026-12-31 --state NY

Results come back as `json` (an envelope with a row count), `ndjson`, or `csv`. Exit codes separate the cases a program needs to tell apart: `0` success, `1` the request was understood but nothing matched, `2` the request was wrong. Errors are JSON on stderr.

Both surfaces are read-only and enforce it at the SQLite connection, not by convention. Custom SQL accepts one `SELECT`, `WITH`, or `EXPLAIN` statement, runs with a timeout, and returns at most 1,000 rows.

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

FCC records are public but contain contact information. The normalized tables used by the CLI, the agent CLI, and MCP hold no phone, email, or street address. Raw contact, address, ZIP, and FRN fields are denied to custom SQL unless a local operator explicitly passes `--allow-sensitive`. Keep the generated database local.

# Development

    python3 -m unittest discover -s tests -v
    python3 -m compileall -q spectrum_wrangler tests

The only build dependency is the pinned `setuptools==80.9.0`. The application itself has no third-party runtime dependencies, and [the dependency policy](docs/DEPENDENCIES.md) explains what it takes to add one.
