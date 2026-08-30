"""The command line surface, for people and for agents.

Every operation is declared once in OPERATIONS. The argparse subcommands, the
machine-readable `capabilities` manifest, and the help text are all generated
from that list, so an operation cannot appear in one surface and go missing
from another.

Output adapts to the consumer: a table when stdout is a terminal, JSON when it
is piped. `--format` always wins over that guess.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from . import __version__
from .db import connect, initialize
from .progress import Reporter, human_bytes, human_count, human_duration, human_rate
from .query import (
    band_survey,
    callsign,
    database_status,
    describe_schema,
    execute_readonly_sql,
    expirations,
    frequency,
    geography,
    license_record,
    nearby,
    organization,
    radio_services,
    search_licenses,
    text_search,
)
from .uls import (
    LICENSE_ARCHIVES,
    download_archive,
    import_archive,
    imported_counts,
    list_official_archives,
    rebuild_indexes,
    resolve_archives,
    write_manifest,
)


DB_FILENAME = "spectrum-wrangler.sqlite3"
DB_ENV_VAR = "SPECTRUM_WRANGLER_DB"
REPOSITORY_DB = Path("data") / DB_FILENAME


def data_home() -> Path:
    """The per-user data directory, so an installed CLI works from anywhere."""
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "spectrum-wrangler"
    if os.name == "nt":
        local = os.environ.get("LOCALAPPDATA")
        return (Path(local) if local else Path.home() / "AppData" / "Local") / "spectrum-wrangler"
    xdg = os.environ.get("XDG_DATA_HOME")
    return (Path(xdg) if xdg else Path.home() / ".local" / "share") / "spectrum-wrangler"


def default_database() -> Path:
    """Resolve the database path when --database is not given.

    $SPECTRUM_WRANGLER_DB wins; a database already built under ./data (the
    repository-checkout workflow) is used next; otherwise the per-user data
    directory. The download cache and provenance manifest always live beside
    whichever database is chosen.
    """
    override = os.environ.get(DB_ENV_VAR)
    if override:
        return Path(override).expanduser()
    if REPOSITORY_DB.exists():
        return REPOSITORY_DB
    return data_home() / DB_FILENAME


EXIT_OK = 0
EXIT_EMPTY = 1
EXIT_ERROR = 2

GUIDANCE = (
    "Run `status` first to learn which FCC snapshots are loaded and how current "
    "they are, then cite those dates. Prefer a structured command over `sql`. "
    "An empty result means 'not in the loaded snapshots', never 'no such FCC "
    "authorization exists'. Full-power AM/FM/TV licensing is in the FCC's "
    "separate LMS system and is not in this database. If no database exists yet, "
    "`init` creates one with a small starter archive and `refresh` downloads the "
    "complete weekly dataset."
)


@dataclass(frozen=True)
class Param:
    name: str
    type: Callable[[str], Any] = str
    positional: bool = False
    optional_positional: bool = False
    default: Any = None
    choices: tuple[str, ...] | None = None
    help: str = ""
    exclusive: str | None = None


@dataclass(frozen=True)
class Operation:
    name: str
    summary: str
    run: Callable[[Any, argparse.Namespace], Any]
    params: tuple[Param, ...] = ()
    exclusive_required: tuple[str, ...] = ()
    examples: tuple[str, ...] = ()
    rows_key: str | None = None
    tabular: bool = True


LIMIT = Param("limit", int, default=100, help="maximum rows, 1-1000")


OPERATIONS: tuple[Operation, ...] = (
    Operation(
        "status", "Show which FCC snapshots are loaded, when published, and row counts.",
        lambda c, a: database_status(c),
        tabular=False,
        examples=("status",),
    ),
    Operation(
        "schema", "List queryable tables, or describe one table's columns.",
        lambda c, a: describe_schema(c, a.table),
        params=(Param("table", optional_positional=True, help="exact table name"),),
        tabular=False,
        examples=("schema", "schema raw_fr"),
    ),
    Operation(
        "callsign", "Look up an exact call sign.",
        lambda c, a: callsign(c, a.callsign),
        params=(Param("callsign", positional=True, help="e.g. W1AW"),),
        examples=("callsign W1AW",),
    ),
    Operation(
        "license",
        "Assemble one licence with its licensee, locations, antennas, frequencies, "
        "and emissions attached.",
        lambda c, a: license_record(c, unique_system_id=a.id, callsign_value=a.callsign),
        params=(
            Param("callsign", help="e.g. W1AW", exclusive="who"),
            Param("id", int, help="unique_system_identifier", exclusive="who"),
        ),
        exclusive_required=("who",),
        tabular=False,
        examples=("license --callsign W1AW",),
    ),
    Operation(
        "search", "Filter licences by call sign, licensee, state, service, or status.",
        lambda c, a: search_licenses(
            c, callsign_text=a.callsign, entity_name=a.name, state=a.state,
            service=a.service, status=a.status, limit=a.limit,
        ),
        params=(
            Param("callsign", help="partial call sign"),
            Param("name", help="licensee name, matched loosely"),
            Param("state", help="two-letter state"),
            Param("service", help="radio service code, e.g. HA"),
            Param("status", help="licence status, e.g. A for active"),
            LIMIT,
        ),
        examples=('search --name "CITY OF NEW YORK" --state NY --status A',),
    ),
    Operation(
        "text", "Full-text search over call sign, licensee, service, and state.",
        lambda c, a: text_search(c, a.query, a.limit),
        params=(
            Param("query", positional=True, help="FTS5 query, e.g. 'fire AND department'"),
            LIMIT,
        ),
        examples=("text 'fire AND department'",),
    ),
    Operation(
        "organization",
        "Report what one licensee holds, and how confidently it can be identified.",
        lambda c, a: organization(c, frn=a.frn, name=a.name, limit=a.limit),
        params=(
            Param("frn", help="FCC Registration Number", exclusive="who"),
            Param("name", help="licensee name", exclusive="who"),
            LIMIT,
        ),
        exclusive_required=("who",),
        tabular=False,
        examples=('organization --name "NEW YORK CITY POLICE"',),
    ),
    Operation(
        "frequency", "Find assignments near a centre frequency.",
        lambda c, a: frequency(c, a.center_mhz, a.tolerance_khz, a.limit),
        params=(
            Param("center_mhz", float, positional=True, help="MHz"),
            Param("tolerance_khz", float, default=12.5, help="half-width in kHz"),
            LIMIT,
        ),
        examples=("frequency 931 --tolerance-khz 500",),
    ),
    Operation(
        "band", "Summarize who holds assignments across a frequency range.",
        lambda c, a: band_survey(
            c, a.low_mhz, a.high_mhz, group_by=a.group_by, state=a.state, limit=a.limit,
        ),
        params=(
            Param("low_mhz", float, positional=True, help="MHz"),
            Param("high_mhz", float, positional=True, help="MHz"),
            Param("group_by", default="service",
                  choices=("service", "state", "licensee", "class_station"),
                  help="dimension to group by"),
            Param("state", help="restrict to one state"),
            LIMIT,
        ),
        rows_key="groups",
        examples=("band 462 468 --group-by licensee",),
    ),
    Operation(
        "nearby", "Find licensed transmitter sites near a coordinate.",
        lambda c, a: nearby(c, a.latitude, a.longitude, a.radius_km, a.limit),
        params=(
            Param("latitude", float, positional=True, help="decimal degrees, WGS84"),
            Param("longitude", float, positional=True, help="decimal degrees, WGS84"),
            Param("radius_km", float, default=10.0, help="great-circle radius in km"),
            LIMIT,
        ),
        examples=(
            "nearby 40.7128 -74.0060 --radius-km 5",
            "nearby 40.748444 -73.985694 --radius-km 0.1    # a single building",
        ),
    ),
    Operation(
        "geography", "Count licensed sites by state or county.",
        lambda c, a: geography(c, level=a.level, service=a.service, state=a.state,
                               limit=a.limit),
        params=(
            Param("level", default="state", choices=("state", "county"),
                  help="aggregation level"),
            Param("service", help="radio service code"),
            Param("state", help="restrict to one state"),
            LIMIT,
        ),
        rows_key="areas",
        examples=("geography --level county --state NY",),
    ),
    Operation(
        "services", "List the radio service codes present, with licence counts.",
        lambda c, a: radio_services(c, a.limit),
        params=(Param("limit", int, default=200, help="maximum rows, 1-1000"),),
        rows_key="services",
        examples=("services",),
    ),
    Operation(
        "expirations", "List licences expiring inside a date window.",
        lambda c, a: expirations(
            c, start=a.start, end=a.end, service=a.service,
            state=a.state, status=a.status, limit=a.limit,
        ),
        params=(
            Param("start", help="YYYY-MM-DD or MM/DD/YYYY"),
            Param("end", help="YYYY-MM-DD or MM/DD/YYYY"),
            Param("service", help="radio service code"),
            Param("state", help="two-letter state"),
            Param("status", default="A", help="licence status, e.g. A for active"),
            LIMIT,
        ),
        examples=("expirations --start 2026-10-01 --end 2026-12-31 --state NY",),
    ),
    Operation(
        "sql", "Run one bounded read-only SELECT, WITH, or EXPLAIN statement.",
        lambda c, a: execute_readonly_sql(
            c,
            a.sql if a.sql is not None else sys.stdin.read(),
            limit=a.limit, timeout_ms=a.timeout_ms,
        ),
        params=(
            Param("sql", optional_positional=True,
                  help="query text; reads stdin when omitted"),
            Param("limit", int, default=200, help="maximum rows, 1-1000"),
            Param("timeout_ms", int, default=5000, help="statement timeout"),
        ),
        rows_key="rows",
        examples=('sql "SELECT radio_service_code, count(*) FROM licenses GROUP BY 1"',),
    ),
)


# ---------------------------------------------------------------- output ----

def _rows_of(payload: Any, rows_key: str | None) -> list[dict[str, Any]] | None:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and rows_key:
        value = payload.get(rows_key)
        if isinstance(value, list):
            return value
    return None


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _table(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("(no matching records)")
        return
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    widths = {
        column: min(max(len(column), *(len(_cell(row.get(column))) for row in rows)), 40)
        for column in columns
    }
    print("  ".join(c.upper()[: widths[c]].ljust(widths[c]) for c in columns))
    print("  ".join("-" * widths[c] for c in columns))
    for row in rows:
        print("  ".join(_cell(row.get(c))[: widths[c]].ljust(widths[c]) for c in columns))


def _write_json(value: Any) -> None:
    json.dump(value, sys.stdout, indent=2, sort_keys=True, ensure_ascii=False)
    sys.stdout.write("\n")


def emit(command: str, payload: Any, rows_key: str | None, output_format: str) -> int:
    rows = _rows_of(payload, rows_key)
    count = len(rows) if rows is not None else None

    if output_format == "table":
        _table(rows) if rows is not None else _write_json(payload)
    elif output_format == "json":
        envelope: dict[str, Any] = {"ok": True, "command": command, "data": payload}
        if count is not None:
            envelope["row_count"] = count
        _write_json(envelope)
    elif output_format == "ndjson":
        if rows is None:
            raise ValueError(f"`{command}` returns a nested record; use --format json")
        for row in rows:
            sys.stdout.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")
    else:
        if rows is None:
            raise ValueError(f"`{command}` returns a nested record; use --format json")
        columns: list[str] = []
        for row in rows:
            for key in row:
                if key not in columns:
                    columns.append(key)
        writer = csv.DictWriter(sys.stdout, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return EXIT_EMPTY if count == 0 else EXIT_OK


# --------------------------------------------------- non-query commands ----

STARTER_ARCHIVE = "paging"


def cmd_init(args: argparse.Namespace) -> int:
    """First-time setup: create the schema, and load a starter archive if empty."""
    database = Path(args.database)
    with connect(database) as connection:
        initialize(connection)
        active_sources = connection.execute(
            "SELECT count(*) FROM sources WHERE active=1"
        ).fetchone()[0]
    if active_sources and not args.archive:
        _write_json({
            "database": str(database.resolve()),
            "initialized": True,
            "active_sources": active_sources,
            "hint": "data already loaded; `spectrum-wrangler refresh` updates it",
        })
        return EXIT_OK
    requested = args.archive or [STARTER_ARCHIVE]
    if not args.archive:
        reporter = Reporter()
        reporter.say(reporter.bold("first run: no data loaded yet"))
        reporter.say(reporter.dim(
            "  loading the starter archive (paging, about 6.5 MB) — "
            "under a minute, so there is something to query right away"))
        reporter.say(reporter.dim(
            "  `spectrum-wrangler refresh` afterwards downloads the complete "
            "dataset (1.25 GB, roughly 23 GB on disk)"))
    results = _load_archives(database, _cache_for(args, database),
                             _manifest_for(args, database), resolve_archives(requested))
    _write_json({
        "database": str(database.resolve()),
        "initialized": True,
        "imports": results,
        "next": "`spectrum-wrangler refresh` downloads the complete dataset "
                "(about 1.25 GB compressed, 23 GB database)",
    })
    return EXIT_OK


def cmd_sources(args: argparse.Namespace) -> int:
    found = list_official_archives()
    _write_json({
        "official_directory": found,
        "reviewed_license_archives": list(LICENSE_ARCHIVES),
        "new_or_unreviewed": sorted(set(found) - set(LICENSE_ARCHIVES)),
        "missing": sorted(set(LICENSE_ARCHIVES) - set(found)),
    })
    return EXIT_OK


def _cache_for(args: argparse.Namespace, database: Path) -> Path:
    given = getattr(args, "cache", None)
    return given if given is not None else database.parent / "cache" / "uls"


def _manifest_for(args: argparse.Namespace, database: Path) -> Path:
    given = getattr(args, "manifest", None)
    return given if given is not None else database.parent / "source-manifest.json"


def _database_bytes(database: Path) -> int:
    return sum(
        candidate.stat().st_size
        for suffix in ("", "-wal", "-shm")
        if (candidate := Path(str(database) + suffix)).exists()
    )


def _load_archives(
    database: Path, cache: Path, manifest: Path, names: list[str], *,
    force_download: bool = False, normalized_only: bool = False,
    force_import: bool = False,
) -> list[dict[str, Any]]:
    """Download and import the named archives; shared by `init` and `refresh`."""
    official = set(list_official_archives())
    missing = [name for name in names if name not in official]
    if missing:
        raise RuntimeError(f"FCC directory does not currently list: {', '.join(missing)}")
    reporter = Reporter()
    total = len(names)
    started = time.monotonic()
    reporter.say(f"loading {total} archive{'s' if total != 1 else ''} into {database}")
    if total == len(LICENSE_ARCHIVES):
        reporter.say(reporter.dim(
            "  the complete set downloads about 1.25 GB and builds a roughly "
            "23 GB database — 15 minutes of importing plus the download"))
    reporter.say(reporter.dim(
        "  cached downloads and unchanged imports are reused automatically"))
    results = []
    normalized_changed = False
    with connect(database) as connection:
        initialize(connection)
        for position, name in enumerate(names, start=1):
            reporter.stage(position, total, name)
            reporter.begin("downloading")

            def on_download(done: int, expected: int) -> None:
                span = f" of {human_bytes(expected)}" if expected else ""
                rate = human_rate(done, reporter.elapsed, "B")
                reporter.update(f"{human_bytes(done)}{span}  {rate}")

            download = download_archive(name, cache, force=force_download,
                                        on_progress=on_download)
            if download.cached:
                reporter.done("cached", f"{human_bytes(download.byte_size)}, "
                                        "checksum verified")
            else:
                rate = human_rate(download.byte_size, reporter.elapsed, "B")
                reporter.done("downloaded",
                              f"{human_bytes(download.byte_size)} ({rate})")
            counts = None if force_import else imported_counts(
                connection, download, require_raw=not normalized_only
            )
            if counts is None:
                normalized_exists = None if force_import else imported_counts(
                    connection, download, require_raw=False
                )
                raw_only = normalized_exists is not None and not normalized_only
                reporter.begin("completing raw tables" if raw_only else "importing")
                seen: dict[str, int] = {}

                def on_import(label: str, done: int) -> None:
                    seen[label] = done
                    rows = sum(seen.values())
                    rate = human_rate(rows, reporter.elapsed, "rows")
                    reporter.update(f"{human_count(rows)} rows  {rate}  ({label})")

                counts = import_archive(
                    connection, download,
                    full_raw=not normalized_only, normalized=not raw_only,
                    on_progress=on_import,
                )
                rows = sum(counts.values())
                rate = human_rate(rows, reporter.elapsed, "rows")
                reporter.done("completed raw tables" if raw_only else "imported",
                              f"{human_count(rows)} rows ({rate})")
                disposition = "raw-completed" if raw_only else "imported"
                normalized_changed = normalized_changed or not raw_only
            else:
                reporter.done("unchanged", "this snapshot is already imported")
                disposition = "unchanged"
            results.append({"archive": name, "sha256": download.sha256,
                            "counts": counts, "status": disposition})
        if normalized_changed:
            reporter.begin("indexing")
            rebuild_indexes(connection)
            reporter.done("indexed", "search and spatial indexes rebuilt")
        write_manifest(connection, manifest)
    imported = sum(r["status"] in ("imported", "raw-completed") for r in results)
    unchanged = len(results) - imported
    reporter.say(reporter.bold(
        f"done in {human_duration(time.monotonic() - started)}: "
        f"{imported} imported, {unchanged} unchanged; "
        f"database {human_bytes(_database_bytes(database))}"))
    return results


def cmd_refresh(args: argparse.Namespace) -> int:
    database = Path(args.database)
    results = _load_archives(
        database, _cache_for(args, database), _manifest_for(args, database),
        resolve_archives(args.archive),
        force_download=args.force_download, normalized_only=args.normalized_only,
        force_import=args.force_import,
    )
    _write_json({"database": str(database.resolve()), "imports": results})
    return EXIT_OK


def capabilities_manifest() -> dict[str, Any]:
    return {
        "tool": "spectrum-wrangler",
        "version": __version__,
        "authority": "Federal Communications Commission, Universal Licensing System",
        "read_only_queries": True,
        "formats": ["table", "json", "ndjson", "csv"],
        "exit_codes": {
            str(EXIT_OK): "success",
            str(EXIT_EMPTY): "request understood, zero matching records",
            str(EXIT_ERROR): "invalid request or database error",
        },
        "environment": {
            DB_ENV_VAR: "database path used when --database is not given",
        },
        "guidance": GUIDANCE,
        "commands": [
            {
                "name": operation.name,
                "summary": operation.summary,
                "returns": "rows" if operation.rows_key or operation.tabular else "record",
                "examples": list(operation.examples),
                "parameters": [
                    {
                        "name": parameter.name,
                        "flag": (
                            parameter.name
                            if parameter.positional or parameter.optional_positional
                            else f"--{parameter.name.replace('_', '-')}"
                        ),
                        "type": parameter.type.__name__,
                        "positional": parameter.positional or parameter.optional_positional,
                        "required": parameter.positional,
                        "default": parameter.default,
                        "choices": list(parameter.choices) if parameter.choices else None,
                        "help": parameter.help,
                    }
                    for parameter in operation.params
                ],
            }
            for operation in OPERATIONS
        ],
    }


# ---------------------------------------------------------------- parser ----

def _flag_help(parameter: Param) -> str:
    """The declared help, with the declared default appended so the two agree."""
    if parameter.default is None:
        return parameter.help
    suffix = f"default: {parameter.default}"
    return f"{parameter.help} ({suffix})" if parameter.help else suffix


def _accept_global_flags(sub: argparse.ArgumentParser) -> None:
    """Accept --database and --format after the subcommand, where habit puts them.

    SUPPRESS keeps an absent flag from overwriting the value the root parser
    already resolved; a trailing flag wins over a leading one.
    """
    sub.add_argument("--database", default=argparse.SUPPRESS, type=Path,
                     help="database path; same as the flag before the subcommand")
    sub.add_argument("--format", dest="output_format", default=argparse.SUPPRESS,
                     choices=("table", "json", "ndjson", "csv"),
                     help="output format; same as the flag before the subcommand")


def _add_params(sub: argparse.ArgumentParser, operation: Operation) -> None:
    groups = {
        key: sub.add_mutually_exclusive_group(required=True)
        for key in operation.exclusive_required
    }
    for parameter in operation.params:
        target = groups.get(parameter.exclusive or "", sub)
        if parameter.positional:
            target.add_argument(parameter.name, type=parameter.type, help=parameter.help)
        elif parameter.optional_positional:
            target.add_argument(parameter.name, nargs="?", type=parameter.type,
                                help=parameter.help)
        else:
            target.add_argument(
                f"--{parameter.name.replace('_', '-')}", dest=parameter.name,
                type=parameter.type, default=parameter.default,
                choices=parameter.choices, help=_flag_help(parameter),
            )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="spectrum-wrangler",
        description="Query the FCC's public spectrum licensing data locally.",
        epilog="first run: `spectrum-wrangler init` creates the database and loads a "
               "small starter archive; `spectrum-wrangler refresh` downloads the "
               "complete FCC dataset.",
    )
    root.add_argument("--version", action="version", version=__version__)
    root.add_argument(
        "--database", default=None, type=Path,
        help=f"SQLite database path (default: ${DB_ENV_VAR} if set, "
             f"./{REPOSITORY_DB} if already built, else {data_home() / DB_FILENAME})",
    )
    root.add_argument(
        "--format", dest="output_format",
        choices=("table", "json", "ndjson", "csv"), default=None,
        help="default: table on a terminal, json when piped",
    )
    commands = root.add_subparsers(dest="command", required=True)

    for operation in OPERATIONS:
        epilog = None
        if operation.examples:
            epilog = "examples:\n  " + "\n  ".join(
                f"spectrum-wrangler {example}" for example in operation.examples
            )
        sub = commands.add_parser(
            operation.name, help=operation.summary, description=operation.summary,
            epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        _add_params(sub, operation)
        _accept_global_flags(sub)
        sub.set_defaults(operation=operation)

    caps = commands.add_parser(
        "capabilities",
        help="describe every command, argument, format, and exit code as JSON",
        description="Describe every command, argument, format, and exit code as JSON.",
    )
    caps.set_defaults(func=lambda a: emit(
        "capabilities", capabilities_manifest(), None, a.output_format))
    _accept_global_flags(caps)

    init = commands.add_parser(
        "init",
        help="first-time setup: create the database and load a small starter archive",
        description="First-time setup: create or migrate the schema, and if no data is "
                    "loaded yet download a small starter archive (paging, about 6.5 MB) "
                    "so there is something to query. Run `refresh` for the complete "
                    "dataset. Safe to re-run; an already-loaded database is left alone.",
    )
    init.add_argument("--archive", action="append", default=[],
                      help="load this alias or archive instead of the starter; "
                           "repeat for several")
    init.set_defaults(func=cmd_init)
    _accept_global_flags(init)

    sources = commands.add_parser(
        "sources", help="compare the FCC's live directory with the reviewed archive set")
    sources.set_defaults(func=cmd_sources)
    _accept_global_flags(sources)

    refresh = commands.add_parser(
        "refresh", help="download and import current weekly ULS license archives")
    refresh.add_argument("--archive", action="append", default=[],
                         help="alias or archive name; repeat for several")
    refresh.add_argument("--cache", default=None, type=Path,
                         help="download cache directory (default: cache/uls beside the database)")
    refresh.add_argument("--manifest", default=None, type=Path,
                         help="provenance manifest path (default: source-manifest.json beside the database)")
    refresh.add_argument("--force-download", action="store_true",
                         help="re-download archives even when the cached copy's hash matches")
    refresh.add_argument("--normalized-only", action="store_true",
                         help="skip lossless raw tables to save space")
    refresh.add_argument("--force-import", action="store_true",
                         help="re-import archives even when this content hash is already loaded")
    refresh.set_defaults(func=cmd_refresh)
    _accept_global_flags(refresh)
    return root


def _run_operation(args: argparse.Namespace) -> int:
    operation: Operation = args.operation
    database = Path(args.database)
    if not database.expanduser().exists():
        raise ValueError(
            f"database not found: {database}; run `spectrum-wrangler init` to create "
            f"it with a small starter dataset, `spectrum-wrangler refresh` for the "
            f"complete one, or point at an existing database with --database or "
            f"${DB_ENV_VAR}"
        )
    with connect(database, read_only=True) as connection:
        payload = operation.run(connection, args)
    return emit(operation.name, payload, operation.rows_key, args.output_format)


def main(argv: list[str] | None = None, *, default_format: str | None = None) -> None:
    args = parser().parse_args(argv)
    if args.database is None:
        args.database = default_database()
    if args.output_format is None:
        args.output_format = default_format or ("table" if sys.stdout.isatty() else "json")
    try:
        if hasattr(args, "operation"):
            raise SystemExit(_run_operation(args))
        raise SystemExit(args.func(args))
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        if args.output_format in ("json", "ndjson", "csv"):
            json.dump({"ok": False, "command": args.command, "error": str(error)},
                      sys.stderr, ensure_ascii=False)
            sys.stderr.write("\n")
        else:
            print(f"error: {error}", file=sys.stderr)
        raise SystemExit(EXIT_ERROR) from error


if __name__ == "__main__":
    main()
