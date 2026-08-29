"""Dependency-free command line surface."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from . import __version__
from .db import connect, initialize
from .query import (
    callsign,
    database_status,
    describe_schema,
    execute_readonly_sql,
    frequency,
    nearby,
    search_licenses,
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


DEFAULT_DB = Path("data/spectrum-wrangler.sqlite3")
DEFAULT_CACHE = Path("data/cache/uls")
DEFAULT_MANIFEST = Path("data/source-manifest.json")


def _print_json(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False))


def cmd_init(args: argparse.Namespace) -> int:
    with connect(args.database) as connection:
        initialize(connection)
    _print_json({"database": str(Path(args.database).resolve()), "initialized": True})
    return 0


def cmd_sources(args: argparse.Namespace) -> int:
    found = list_official_archives()
    _print_json({
        "official_directory": found,
        "reviewed_license_archives": list(LICENSE_ARCHIVES),
        "new_or_unreviewed": sorted(set(found) - set(LICENSE_ARCHIVES)),
        "missing": sorted(set(LICENSE_ARCHIVES) - set(found)),
    })
    return 0


def cmd_refresh(args: argparse.Namespace) -> int:
    names = resolve_archives(args.archive)
    official = set(list_official_archives())
    missing = [name for name in names if name not in official]
    if missing:
        raise RuntimeError(f"FCC directory does not currently list: {', '.join(missing)}")
    results = []
    normalized_changed = False
    with connect(args.database) as connection:
        initialize(connection)
        for name in names:
            print(f"downloading {name}", file=sys.stderr, flush=True)
            download = download_archive(name, args.cache, force=args.force_download)
            counts = None if args.force_import else imported_counts(
                connection, download, require_raw=not args.normalized_only
            )
            if counts is None:
                normalized_exists = None if args.force_import else imported_counts(
                    connection, download, require_raw=False
                )
                raw_only = normalized_exists is not None and not args.normalized_only
                action = "completing raw tables for" if raw_only else "importing"
                print(f"{action} {name} ({download.byte_size:,} bytes)", file=sys.stderr, flush=True)
                counts = import_archive(
                    connection,
                    download,
                    full_raw=not args.normalized_only,
                    normalized=not raw_only,
                )
                disposition = "raw-completed" if raw_only else "imported"
                normalized_changed = normalized_changed or not raw_only
            else:
                print(f"unchanged {name}; retaining verified import", file=sys.stderr, flush=True)
                disposition = "unchanged"
            results.append({"archive": name, "sha256": download.sha256, "counts": counts, "status": disposition})
        if normalized_changed:
            print("rebuilding search and spatial indexes", file=sys.stderr, flush=True)
            rebuild_indexes(connection)
        write_manifest(connection, args.manifest)
    _print_json({"database": str(Path(args.database).resolve()), "imports": results})
    return 0


def cmd_callsign(args: argparse.Namespace) -> int:
    with connect(args.database, read_only=True) as connection:
        _print_json(callsign(connection, args.callsign))
    return 0


def cmd_frequency(args: argparse.Namespace) -> int:
    with connect(args.database, read_only=True) as connection:
        _print_json(frequency(connection, args.center_mhz, args.tolerance_khz, args.limit))
    return 0


def cmd_nearby(args: argparse.Namespace) -> int:
    with connect(args.database, read_only=True) as connection:
        _print_json(nearby(connection, args.latitude, args.longitude, args.radius_km, args.limit))
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    with connect(args.database, read_only=True) as connection:
        _print_json(search_licenses(
            connection,
            callsign_text=args.callsign,
            entity_name=args.name,
            state=args.state,
            service=args.service,
            status=args.status,
            limit=args.limit,
        ))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    with connect(args.database, read_only=True) as connection:
        _print_json(database_status(connection))
    return 0


def cmd_schema(args: argparse.Namespace) -> int:
    with connect(args.database, read_only=True) as connection:
        _print_json(describe_schema(connection, args.table))
    return 0


def cmd_sql(args: argparse.Namespace) -> int:
    sql = args.sql if args.sql is not None else sys.stdin.read()
    with connect(args.database, read_only=True) as connection:
        _print_json(execute_readonly_sql(
            connection,
            sql,
            limit=args.limit,
            timeout_ms=args.timeout_ms,
            allow_sensitive=args.allow_sensitive,
        ))
    return 0


def cmd_mcp(args: argparse.Namespace) -> int:
    from .mcp_server import serve
    serve(Path(args.database), allow_sensitive=args.allow_sensitive)
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="spectrum-wrangler")
    root.add_argument("--version", action="version", version=__version__)
    root.add_argument("--database", default=DEFAULT_DB, type=Path)
    commands = root.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="create an empty indexed database")
    init.set_defaults(func=cmd_init)

    sources = commands.add_parser("sources", help="compare FCC's live directory with the reviewed archive set")
    sources.set_defaults(func=cmd_sources)

    refresh = commands.add_parser("refresh", help="download and stream current weekly ULS license archives")
    refresh.add_argument("--archive", action="append", default=[], help="alias/archive; repeat, or use all")
    refresh.add_argument("--cache", default=DEFAULT_CACHE, type=Path)
    refresh.add_argument("--manifest", default=DEFAULT_MANIFEST, type=Path)
    refresh.add_argument("--force-download", action="store_true")
    refresh.add_argument(
        "--normalized-only",
        action="store_true",
        help="skip lossless raw tables to save space (not recommended for agent use)",
    )
    refresh.add_argument("--force-import", action="store_true")
    refresh.set_defaults(func=cmd_refresh)

    lookup = commands.add_parser("callsign", help="look up an exact callsign")
    lookup.add_argument("callsign")
    lookup.set_defaults(func=cmd_callsign)

    freq = commands.add_parser("frequency", help="find assignments around a center frequency")
    freq.add_argument("center_mhz", type=float)
    freq.add_argument("--tolerance-khz", type=float, default=12.5)
    freq.add_argument("--limit", type=int, default=100)
    freq.set_defaults(func=cmd_frequency)

    near = commands.add_parser("nearby", help="find licensed sites near a coordinate")
    near.add_argument("latitude", type=float)
    near.add_argument("longitude", type=float)
    near.add_argument("--radius-km", type=float, default=10.0)
    near.add_argument("--limit", type=int, default=100)
    near.set_defaults(func=cmd_nearby)

    search = commands.add_parser("search", help="search normalized FCC licenses")
    search.add_argument("--callsign")
    search.add_argument("--name")
    search.add_argument("--state")
    search.add_argument("--service")
    search.add_argument("--status")
    search.add_argument("--limit", type=int, default=100)
    search.set_defaults(func=cmd_search)

    status = commands.add_parser("status", help="show loaded source provenance and counts")
    status.set_defaults(func=cmd_status)

    schema = commands.add_parser("schema", help="list queryable tables or describe one table")
    schema.add_argument("table", nargs="?")
    schema.set_defaults(func=cmd_schema)

    sql = commands.add_parser("sql", help="execute one bounded read-only SQLite query")
    sql.add_argument("sql", nargs="?", help="query text; reads stdin when omitted")
    sql.add_argument("--limit", type=int, default=200)
    sql.add_argument("--timeout-ms", type=int, default=5000)
    sql.add_argument(
        "--allow-sensitive",
        action="store_true",
        help="permit raw FCC contact/address fields in this local CLI result",
    )
    sql.set_defaults(func=cmd_sql)

    mcp = commands.add_parser("mcp", help="serve privacy-filtered MCP over stdio")
    mcp.add_argument(
        "--allow-sensitive",
        action="store_true",
        help="expose raw FCC contact/address/FRN fields to this local MCP client",
    )
    mcp.set_defaults(func=cmd_mcp)
    return root


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    try:
        raise SystemExit(args.func(args))
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
