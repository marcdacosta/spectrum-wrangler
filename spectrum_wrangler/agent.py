"""Agent-facing command line surface over the loaded FCC license database.

The ordinary `spectrum-wrangler` CLI is written for a person reading output in
a terminal. This one is written for a program: every command is discoverable
through `capabilities`, every result uses one envelope, machine formats are
first class, and exit codes distinguish "no matching records" from "the request
was wrong". It is read-only and shares its query layer with the MCP server, so
the CLI and an MCP client answer questions identically.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Callable

from . import __version__
from .db import connect
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
    radio_services,
    search_licenses,
    text_search,
)


DEFAULT_DB = Path("data/spectrum-wrangler.sqlite3")

EXIT_OK = 0
EXIT_EMPTY = 1
EXIT_ERROR = 2

GUIDANCE = (
    "Call `status` first to learn which FCC snapshots are loaded and how current "
    "they are, and cite those dates in any conclusion. Prefer a structured "
    "command over `sql`. Treat an empty result (exit code 1) as 'not present in "
    "the loaded snapshots', not as proof that no FCC authorization exists. "
    "Full-power AM/FM/TV station licensing lives in the separate FCC LMS system "
    "and is not in this database."
)


def _rows_of(payload: Any) -> list[dict[str, Any]] | None:
    """Return the tabular part of a payload, or None when it is not tabular."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("rows", "services", "areas", "groups"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return None


def _emit(command: str, payload: Any, output_format: str) -> int:
    rows = _rows_of(payload)
    count = len(rows) if rows is not None else None

    if output_format == "json":
        envelope = {"ok": True, "command": command, "data": payload}
        if count is not None:
            envelope["row_count"] = count
        json.dump(envelope, sys.stdout, indent=2, sort_keys=True, ensure_ascii=False)
        sys.stdout.write("\n")
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


def _open(args: argparse.Namespace) -> sqlite3.Connection:
    database = Path(args.database)
    if not database.expanduser().exists():
        raise ValueError(
            f"database does not exist: {database}; run `spectrum-wrangler refresh` first"
        )
    return connect(database, read_only=True)


# Derived from the live parser rather than a hand-written table, so the
# manifest an agent reads can never drift from the flags actually accepted.
def _capabilities(parser_root: argparse.ArgumentParser) -> dict[str, Any]:
    commands = []
    subparsers = next(
        action for action in parser_root._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    for name, sub in sorted(subparsers.choices.items()):
        arguments = []
        for action in sub._actions:
            if action.dest in {"help", "func"}:
                continue
            arguments.append({
                "name": action.dest,
                "flags": list(action.option_strings) or [action.dest],
                "required": bool(action.required),
                "default": action.default,
                "help": action.help or "",
            })
        commands.append({"name": name, "description": sub.description or "", "arguments": arguments})
    return {
        "tool": "spectrum-wrangler-agent",
        "version": __version__,
        "read_only": True,
        "authority": "Federal Communications Commission, Universal Licensing System",
        "formats": ["json", "ndjson", "csv"],
        "exit_codes": {
            str(EXIT_OK): "success",
            str(EXIT_EMPTY): "request understood, zero matching records",
            str(EXIT_ERROR): "invalid request or database error",
        },
        "guidance": GUIDANCE,
        "commands": commands,
    }


def build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="spectrum-wrangler-agent",
        description="Read-only, machine-readable query surface over the FCC ULS database.",
    )
    root.add_argument("--version", action="version", version=__version__)
    root.add_argument("--database", default=DEFAULT_DB, type=Path)
    root.add_argument(
        "--format",
        dest="output_format",
        choices=("json", "ndjson", "csv"),
        default="json",
        help="json envelope (default), one JSON object per row, or CSV",
    )
    commands = root.add_subparsers(dest="command", required=True)

    capabilities = commands.add_parser(
        "capabilities", description="Describe every command, argument, format, and exit code."
    )
    capabilities.set_defaults(func=lambda args: _capabilities(root))

    status = commands.add_parser(
        "status", description="Loaded FCC snapshots, publication dates, hashes, and row counts."
    )
    status.set_defaults(func=lambda args: _with_db(args, database_status))

    schema = commands.add_parser(
        "schema", description="List queryable tables, or describe one table's columns."
    )
    schema.add_argument("table", nargs="?")
    schema.set_defaults(func=lambda args: _with_db(args, lambda c: describe_schema(c, args.table)))

    record = commands.add_parser(
        "license",
        description="Assemble one complete license: header, licensee, locations, "
                    "antennas, frequencies, and emissions.",
    )
    identity = record.add_mutually_exclusive_group(required=True)
    identity.add_argument("--callsign")
    identity.add_argument("--id", type=int, dest="unique_system_id", help="unique_system_identifier")
    record.set_defaults(func=lambda args: _with_db(args, lambda c: license_record(
        c, unique_system_id=args.unique_system_id, callsign_value=args.callsign,
    )))

    lookup = commands.add_parser("callsign", description="Look up an exact callsign.")
    lookup.add_argument("callsign")
    lookup.set_defaults(func=lambda args: _with_db(args, lambda c: callsign(c, args.callsign)))

    search = commands.add_parser(
        "search", description="Filter licenses by callsign, licensee, state, service, or status."
    )
    search.add_argument("--callsign")
    search.add_argument("--name", help="licensee display name substring")
    search.add_argument("--state")
    search.add_argument("--service", help="radio service code, e.g. HA")
    search.add_argument("--status", help="license status code, e.g. A for active")
    search.add_argument("--limit", type=int, default=100)
    search.set_defaults(func=lambda args: _with_db(args, lambda c: search_licenses(
        c,
        callsign_text=args.callsign,
        entity_name=args.name,
        state=args.state,
        service=args.service,
        status=args.status,
        limit=args.limit,
    )))

    text = commands.add_parser(
        "text", description="Full-text search over callsign, licensee, service, and state."
    )
    text.add_argument("query", help="FTS5 query, e.g. 'fire AND department'")
    text.add_argument("--limit", type=int, default=100)
    text.set_defaults(func=lambda args: _with_db(args, lambda c: text_search(c, args.query, args.limit)))

    freq = commands.add_parser(
        "frequency", description="Find assignments near a centre frequency."
    )
    freq.add_argument("center_mhz", type=float)
    freq.add_argument("--tolerance-khz", type=float, default=12.5)
    freq.add_argument("--limit", type=int, default=100)
    freq.set_defaults(func=lambda args: _with_db(args, lambda c: frequency(
        c, args.center_mhz, args.tolerance_khz, args.limit,
    )))

    band = commands.add_parser(
        "band", description="Summarize who holds assignments across a frequency range."
    )
    band.add_argument("low_mhz", type=float)
    band.add_argument("high_mhz", type=float)
    band.add_argument("--group-by", choices=("service", "state", "licensee", "class_station"), default="service")
    band.add_argument("--state")
    band.add_argument("--limit", type=int, default=100)
    band.set_defaults(func=lambda args: _with_db(args, lambda c: band_survey(
        c, args.low_mhz, args.high_mhz, group_by=args.group_by, state=args.state, limit=args.limit,
    )))

    near = commands.add_parser("nearby", description="Find licensed sites near a WGS84 coordinate.")
    near.add_argument("latitude", type=float)
    near.add_argument("longitude", type=float)
    near.add_argument("--radius-km", type=float, default=10.0)
    near.add_argument("--limit", type=int, default=100)
    near.set_defaults(func=lambda args: _with_db(args, lambda c: nearby(
        c, args.latitude, args.longitude, args.radius_km, args.limit,
    )))

    services = commands.add_parser(
        "services", description="Enumerate radio service codes present, with license counts."
    )
    services.add_argument("--limit", type=int, default=200)
    services.set_defaults(func=lambda args: _with_db(args, lambda c: radio_services(c, args.limit)))

    expiring = commands.add_parser(
        "expirations", description="List licenses expiring inside a date window."
    )
    expiring.add_argument("--start", help="YYYY-MM-DD or MM/DD/YYYY")
    expiring.add_argument("--end", help="YYYY-MM-DD or MM/DD/YYYY")
    expiring.add_argument("--service")
    expiring.add_argument("--state")
    expiring.add_argument("--status", default="A")
    expiring.add_argument("--limit", type=int, default=100)
    expiring.set_defaults(func=lambda args: _with_db(args, lambda c: expirations(
        c,
        start=args.start,
        end=args.end,
        service=args.service,
        state=args.state,
        status=args.status,
        limit=args.limit,
    )))

    geo = commands.add_parser("geography", description="Count licensed sites by state or county.")
    geo.add_argument("--level", choices=("state", "county"), default="state")
    geo.add_argument("--service")
    geo.add_argument("--state")
    geo.add_argument("--limit", type=int, default=100)
    geo.set_defaults(func=lambda args: _with_db(args, lambda c: geography(
        c, level=args.level, service=args.service, state=args.state, limit=args.limit,
    )))

    sql = commands.add_parser(
        "sql", description="Run one bounded read-only SELECT/WITH/EXPLAIN statement."
    )
    sql.add_argument("sql", nargs="?", help="query text; reads stdin when omitted")
    sql.add_argument("--limit", type=int, default=200)
    sql.add_argument("--timeout-ms", type=int, default=5000)
    sql.add_argument(
        "--allow-sensitive",
        action="store_true",
        help="permit raw FCC contact/address/FRN fields in this local result",
    )
    sql.set_defaults(func=lambda args: _with_db(args, lambda c: execute_readonly_sql(
        c,
        args.sql if args.sql is not None else sys.stdin.read(),
        limit=args.limit,
        timeout_ms=args.timeout_ms,
        allow_sensitive=args.allow_sensitive,
    )))
    return root


def _with_db(args: argparse.Namespace, work: Callable[[sqlite3.Connection], Any]) -> Any:
    with _open(args) as connection:
        return work(connection)


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        payload = args.func(args)
        raise SystemExit(_emit(args.command, payload, args.output_format))
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        json.dump(
            {"ok": False, "command": args.command, "error": str(error)},
            sys.stderr,
            ensure_ascii=False,
        )
        sys.stderr.write("\n")
        raise SystemExit(EXIT_ERROR) from error


if __name__ == "__main__":
    main()
