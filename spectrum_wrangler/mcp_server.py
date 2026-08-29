"""Dependency-free, privacy-filtered Model Context Protocol server over stdio."""

from __future__ import annotations

import argparse
import json
import os
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


LATEST_PROTOCOL = "2026-07-28"
LEGACY_PROTOCOLS = {"2025-11-25", "2025-06-18", "2024-11-05"}
LATEST_LEGACY_PROTOCOL = "2025-11-25"
SUPPORTED_PROTOCOLS = {LATEST_PROTOCOL, *LEGACY_PROTOCOLS}
DEFAULT_DB = Path(os.environ.get("SPECTRUM_WRANGLER_DB", "data/spectrum-wrangler.sqlite3"))


class MethodNotFound(ValueError):
    pass


def _annotations(
    *, read_only: bool = True, idempotent: bool = True, open_world: bool = False
) -> dict[str, bool]:
    return {
        "readOnlyHint": read_only,
        "destructiveHint": False,
        "idempotentHint": idempotent,
        "openWorldHint": open_world,
    }


TOOLS: list[dict[str, Any]] = [
    {
        "name": "spectrum_status",
        "description": "Show loaded FCC source provenance, dates, hashes, and table counts. Call this first.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "annotations": _annotations(),
    },
    {
        "name": "describe_schema",
        "description": "List queryable normalized/raw tables or describe the exact columns in one table.",
        "inputSchema": {
            "type": "object",
            "properties": {"table": {"type": "string", "description": "Optional exact table name."}},
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "query_spectrum_sql",
        "description": "Run one bounded read-only SELECT/WITH/EXPLAIN query. Raw FCC contact/address identifiers are denied unless the local operator explicitly enables sensitive access.",
        "inputSchema": {
            "type": "object",
            "required": ["sql"],
            "properties": {
                "sql": {"type": "string", "description": "A single read-only SQLite statement."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 200},
                "timeout_ms": {"type": "integer", "minimum": 1, "maximum": 30000, "default": 5000},
            },
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "lookup_callsign",
        "description": "Look up all current and historical normalized license rows for an exact FCC call sign.",
        "inputSchema": {
            "type": "object",
            "required": ["callsign"],
            "properties": {"callsign": {"type": "string", "minLength": 1}},
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "search_licenses",
        "description": "Search licenses by partial call sign/name and exact state, radio-service code, or status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "callsign_text": {"type": "string"},
                "entity_name": {"type": "string"},
                "state": {"type": "string"},
                "service": {"type": "string"},
                "status": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
            },
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "search_frequency",
        "description": "Find licensed assignments around a center frequency in MHz.",
        "inputSchema": {
            "type": "object",
            "required": ["center_mhz"],
            "properties": {
                "center_mhz": {"type": "number", "minimum": 0},
                "tolerance_khz": {"type": "number", "minimum": 0, "default": 12.5},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
            },
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "search_nearby_sites",
        "description": "Find licensed transmitter sites within a great-circle radius of a WGS84 coordinate.",
        "inputSchema": {
            "type": "object",
            "required": ["latitude", "longitude"],
            "properties": {
                "latitude": {"type": "number", "minimum": -90, "maximum": 90},
                "longitude": {"type": "number", "minimum": -180, "maximum": 180},
                "radius_km": {"type": "number", "exclusiveMinimum": 0, "maximum": 1000, "default": 10},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
            },
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "get_license_record",
        "description": "Assemble one complete license: header, licensee, locations, antennas, frequency assignments, and emissions. Use this instead of six separate joins.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "callsign": {"type": "string", "minLength": 1},
                "unique_system_id": {"type": "integer", "minimum": 1},
            },
            "oneOf": [{"required": ["callsign"]}, {"required": ["unique_system_id"]}],
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "search_text",
        "description": "Full-text search over call sign, licensee name, radio-service code, and state using FTS5 syntax such as 'fire AND department'.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "minLength": 1},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
            },
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "survey_band",
        "description": "Summarize assignments across a frequency range, grouped by radio service, state, licensee, or class of station.",
        "inputSchema": {
            "type": "object",
            "required": ["low_mhz", "high_mhz"],
            "properties": {
                "low_mhz": {"type": "number", "minimum": 0},
                "high_mhz": {"type": "number", "minimum": 0},
                "group_by": {
                    "type": "string",
                    "enum": ["service", "state", "licensee", "class_station"],
                    "default": "service",
                },
                "state": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
            },
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "list_radio_services",
        "description": "Enumerate the radio service codes present in the loaded snapshots with license counts. ULS bulk files do not publish code descriptions.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 200}},
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "list_expirations",
        "description": "List licenses whose expiration date falls inside a window. Dates accept YYYY-MM-DD or the FCC MM/DD/YYYY form.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start": {"type": "string"},
                "end": {"type": "string"},
                "service": {"type": "string"},
                "state": {"type": "string"},
                "status": {"type": "string", "default": "A"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
            },
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
    {
        "name": "summarize_geography",
        "description": "Count licensed transmitter sites by state or county, optionally filtered to one radio service.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "level": {"type": "string", "enum": ["state", "county"], "default": "state"},
                "service": {"type": "string"},
                "state": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
            },
            "additionalProperties": False,
        },
        "annotations": _annotations(),
    },
]


GUIDE = """# Spectrum Wrangler agent guide

1. Call `spectrum_status` to establish which official FCC snapshots are loaded and how current they are.
2. Prefer a structured tool over SQL. `lookup_callsign`, `search_licenses`, `search_text`, `search_frequency`, and `search_nearby_sites` answer point questions; `get_license_record` returns one license with every related record attached; `survey_band`, `summarize_geography`, `list_radio_services`, and `list_expirations` answer aggregate questions.
3. Use `describe_schema` before custom SQL. Normalized tables are fast; raw tables preserve source fidelity, but the agent SQL surface denies contact/address/FRN columns.
4. Join raw ULS records chiefly on `unique_system_identifier`. Depending on the service, also use call sign, location number, antenna number, frequency number, path number, or segment number.
5. Treat an empty result as "not present in the loaded snapshots," not proof that no FCC record exists. Check `spectrum_status` for source coverage.
6. License View is no longer maintained. This database uses current ULS publications; full-power broadcast licensing is now a separate LMS data domain.

All tools are read-only. SQL is limited to one SELECT/WITH/EXPLAIN statement, 1,000 returned rows, and a 30-second maximum timeout. Raw personal-contact fields are denied by default; a local operator can deliberately enable them when launching the server.
"""


class McpServer:
    def __init__(
        self,
        database: Path,
        *,
        allow_sensitive: bool = False,
    ) -> None:
        self.database = database.expanduser().resolve()
        self.allow_sensitive = allow_sensitive

    def handle(self, request: dict[str, Any]) -> dict[str, Any] | None:
        method = request.get("method")
        request_id = request.get("id")
        if request_id is None:
            return None
        params = request.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            return self._error(request_id, -32602, "params must be an object")
        metadata = params.get("_meta", {})
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            return self._error(request_id, -32602, "params._meta must be an object")
        requested_protocol = metadata.get("io.modelcontextprotocol/protocolVersion")
        if requested_protocol is not None and requested_protocol not in SUPPORTED_PROTOCOLS:
            return self._error(
                request_id,
                -32022,
                "Unsupported protocol version",
                {"supported": sorted(SUPPORTED_PROTOCOLS, reverse=True), "requested": requested_protocol},
            )
        try:
            result = self._dispatch(method, params)
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except MethodNotFound as error:
            return self._error(request_id, -32601, str(error))
        except KeyError as error:
            return self._error(request_id, -32602, f"Missing required argument: {error.args[0]}")
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
            return self._error(request_id, -32602, str(error))
        except Exception as error:  # keep the stdio transport alive after an unexpected tool error
            print(f"unexpected MCP error: {error}", file=sys.stderr)
            return self._error(request_id, -32603, "Internal server error")

    def _dispatch(self, method: str | None, params: dict[str, Any]) -> dict[str, Any]:
        if method == "server/discover":
            return {
                "resultType": "complete",
                "supportedVersions": sorted(SUPPORTED_PROTOCOLS, reverse=True),
                "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {
                        "name": "spectrum-wrangler",
                        "version": __version__,
                    }
                },
                "instructions": "Call spectrum_status first. All operations are provenance-aware, read-only, and privacy-filtered.",
                "ttlMs": 300_000,
                "cacheScope": "private",
            }
        if method == "initialize":
            requested = params.get("protocolVersion")
            # initialize is a legacy-era handshake. Modern clients use per-request
            # metadata and may optionally probe with server/discover instead.
            protocol = requested if requested in LEGACY_PROTOCOLS else LATEST_LEGACY_PROTOCOL
            return {
                "protocolVersion": protocol,
                "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
                "serverInfo": {"name": "spectrum-wrangler", "version": __version__},
                "instructions": "Call spectrum_status first. All tools are read-only and provenance-aware.",
            }
        if method == "ping":
            return {}
        if method == "tools/list":
            return {
                "resultType": "complete",
                "tools": TOOLS,
                "ttlMs": 300_000,
                "cacheScope": "private",
            }
        if method == "tools/call":
            return self._call_tool(params["name"], params.get("arguments") or {})
        if method == "resources/list":
            return {
                "resultType": "complete",
                "resources": self._resources(),
                "ttlMs": 300_000,
                "cacheScope": "private",
            }
        if method == "resources/read":
            return {
                "resultType": "complete",
                "contents": [self._read_resource(params["uri"])],
                "ttlMs": 60_000,
                "cacheScope": "private",
            }
        if method == "prompts/list":
            return {
                "resultType": "complete",
                "prompts": [
                    {
                        "name": "investigate_spectrum",
                        "description": "Start a provenance-aware FCC spectrum investigation.",
                        "arguments": [{"name": "question", "description": "The spectrum question to investigate.", "required": True}],
                    }
                ],
                "ttlMs": 300_000,
                "cacheScope": "private",
            }
        if method == "prompts/get":
            question = (params.get("arguments") or {})["question"]
            if params.get("name") != "investigate_spectrum":
                raise ValueError("Unknown prompt")
            description = "FCC spectrum investigation"
            text = (
                f"Use Spectrum Wrangler to answer this question: {question}\n"
                "Start with spectrum_status, cite loaded source dates, distinguish allocations from license assignments, and state coverage limitations."
            )
            return {"resultType": "complete", "description": description, "messages": [{
                "role": "user", "content": {"type": "text", "text": text},
            }]}
        raise MethodNotFound(f"Method not found: {method}")

    def _call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        handlers: dict[str, Callable[[Any, dict[str, Any]], Any]] = {
            "spectrum_status": lambda db, _a: database_status(db),
            "describe_schema": lambda db, a: describe_schema(db, a.get("table")),
            "query_spectrum_sql": lambda db, a: execute_readonly_sql(
                db,
                a["sql"],
                limit=a.get("limit", 200),
                timeout_ms=a.get("timeout_ms", 5000),
                allow_sensitive=self.allow_sensitive,
            ),
            "lookup_callsign": lambda db, a: callsign(db, a["callsign"]),
            "search_licenses": lambda db, a: search_licenses(db, **a),
            "search_frequency": lambda db, a: frequency(
                db, a["center_mhz"], a.get("tolerance_khz", 12.5), a.get("limit", 100)
            ),
            "search_nearby_sites": lambda db, a: nearby(
                db, a["latitude"], a["longitude"], a.get("radius_km", 10), a.get("limit", 100)
            ),
            "get_license_record": lambda db, a: license_record(
                db,
                unique_system_id=a.get("unique_system_id"),
                callsign_value=a.get("callsign"),
            ),
            "search_text": lambda db, a: text_search(db, a["query"], a.get("limit", 100)),
            "survey_band": lambda db, a: band_survey(
                db,
                a["low_mhz"],
                a["high_mhz"],
                group_by=a.get("group_by", "service"),
                state=a.get("state"),
                limit=a.get("limit", 100),
            ),
            "list_radio_services": lambda db, a: radio_services(db, a.get("limit", 200)),
            "list_expirations": lambda db, a: expirations(db, **a),
            "summarize_geography": lambda db, a: geography(db, **a),
        }
        try:
            if name not in handlers:
                raise ValueError(f"Unknown tool: {name}")
            with connect(self.database, read_only=True) as connection:
                value = handlers[name](connection, arguments)
            return {
                "resultType": "complete",
                "content": [{"type": "text", "text": json.dumps(value, ensure_ascii=False)}],
                "structuredContent": value,
                "isError": False,
            }
        except Exception as error:
            return {
                "resultType": "complete",
                "content": [{"type": "text", "text": json.dumps({"error": str(error)})}],
                "isError": True,
            }

    def _resources(self) -> list[dict[str, str]]:
        return [
            {"uri": "spectrum://guide", "name": "Agent query guide", "mimeType": "text/markdown"},
            {"uri": "spectrum://schema", "name": "Queryable schema", "mimeType": "application/json"},
            {"uri": "spectrum://sources", "name": "Loaded source provenance", "mimeType": "application/json"},
        ]

    def _read_resource(self, uri: str) -> dict[str, str]:
        if uri == "spectrum://guide":
            return {"uri": uri, "mimeType": "text/markdown", "text": GUIDE}
        with connect(self.database, read_only=True) as connection:
            if uri == "spectrum://schema":
                value = describe_schema(connection)
            elif uri == "spectrum://sources":
                value = database_status(connection)
            else:
                raise ValueError(f"Unknown resource: {uri}")
        return {"uri": uri, "mimeType": "application/json", "text": json.dumps(value, ensure_ascii=False)}

    @staticmethod
    def _error(
        request_id: Any,
        code: int,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        error: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        return {"jsonrpc": "2.0", "id": request_id, "error": error}


def serve(
    database: Path,
    *,
    allow_sensitive: bool = False,
) -> None:
    server = McpServer(database, allow_sensitive=allow_sensitive)
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if not isinstance(request, dict):
                raise ValueError("JSON-RPC message must be an object")
            response = server.handle(request)
        except (json.JSONDecodeError, ValueError) as error:
            response = McpServer._error(None, -32700, str(error))
        if response is not None:
            print(json.dumps(response, separators=(",", ":"), ensure_ascii=False), flush=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="spectrum-wrangler-mcp")
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument(
        "--allow-sensitive",
        action="store_true",
        help="expose raw FCC contact/address/FRN fields to this local MCP client",
    )
    args = parser.parse_args(argv)
    if not args.database.expanduser().exists():
        parser.error(f"database does not exist: {args.database}; run `spectrum-wrangler refresh` first")
    serve(args.database, allow_sensitive=args.allow_sensitive)


if __name__ == "__main__":
    main()
