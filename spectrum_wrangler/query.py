"""Bounded read-only queries shared by the CLI and MCP server."""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from pathlib import Path
from typing import Any


MAX_LIMIT = 1_000
MAX_SQL_LENGTH = 100_000
READ_PREFIX = re.compile(r"^\s*(?:--[^\n]*\n\s*|/\*.*?\*/\s*)*(select|with|explain)\b", re.I | re.S)
SENSITIVE_COLUMN_PARTS = (
    "address",
    "attention",
    "certifier_",
    "email",
    "fax",
    "fields_json",
    "first_name",
    "frn",
    "last_name",
    "licensee_id",
    "middle_initial",
    "phone",
    "po_box",
    "social_security",
    "zip_code",
)
RETIRED_SCOPE_TABLES = frozenset({
    "experiments",
    "experiment_runs",
    "research_sources",
    "research_items",
    "research_fts",
})


def _is_retired_scope_table(name: str | None) -> bool:
    value = (name or "").lower()
    return value in RETIRED_SCOPE_TABLES or value.startswith("research_fts_")


def _limit(value: int) -> int:
    if not 1 <= value <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    return value


def row_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    return [dict(row) for row in cursor]


def callsign(connection: sqlite3.Connection, value: str) -> list[dict[str, Any]]:
    return row_dicts(connection.execute(
        "SELECT l.*, a.operator_class, a.group_code, e.display_name, e.state "
        "FROM licenses l LEFT JOIN amateur a USING(unique_system_id) "
        "LEFT JOIN entities e ON e.unique_system_id=l.unique_system_id AND e.entity_type='L' "
        "WHERE l.callsign=? ORDER BY " + _sortable("l.last_action_date") + " DESC",
        (value.upper(),),
    ))


def search_licenses(
    connection: sqlite3.Connection,
    *,
    callsign_text: str | None = None,
    entity_name: str | None = None,
    state: str | None = None,
    service: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Search normalized license and licensee fields with bound parameters."""
    limit = _limit(limit)
    where: list[str] = []
    parameters: list[Any] = []
    if callsign_text:
        where.append("l.callsign LIKE ?")
        parameters.append(f"%{callsign_text.upper()}%")
    if entity_name:
        where.append("e.display_name LIKE ?")
        parameters.append(f"%{entity_name}%")
    if state:
        where.append("e.state = ?")
        parameters.append(state.upper())
    if service:
        where.append("l.radio_service_code = ?")
        parameters.append(service.upper())
    if status:
        where.append("l.license_status = ?")
        parameters.append(status.upper())
    predicate = " AND ".join(where) if where else "1=1"
    parameters.append(limit)
    return row_dicts(connection.execute(
        "SELECT DISTINCT l.unique_system_id,l.callsign,l.license_status,l.radio_service_code,"
        "l.grant_date,l.expired_date,l.last_action_date,e.display_name,e.state "
        "FROM licenses l LEFT JOIN entities e "
        "ON e.unique_system_id=l.unique_system_id AND e.entity_type='L' "
        f"WHERE {predicate} ORDER BY {_sortable('l.last_action_date')} DESC,l.callsign LIMIT ?",
        parameters,
    ))


def frequency(connection: sqlite3.Connection, center_mhz: float, tolerance_khz: float = 12.5, limit: int = 100) -> list[dict[str, Any]]:
    if not math.isfinite(center_mhz) or center_mhz < 0:
        raise ValueError("center_mhz must be a finite non-negative number")
    if not math.isfinite(tolerance_khz) or tolerance_khz < 0:
        raise ValueError("tolerance_khz must be a finite non-negative number")
    limit = _limit(limit)
    delta = tolerance_khz / 1000.0
    return row_dicts(connection.execute(
        "SELECT f.frequency_assigned_mhz,f.frequency_upper_mhz,f.callsign,f.class_station_code,"
        "f.power_output_w,f.erp_w,l.radio_service_code,l.license_status,e.display_name,lo.state,lo.county "
        "FROM frequencies f JOIN licenses l USING(unique_system_id) "
        "LEFT JOIN entities e ON e.unique_system_id=l.unique_system_id AND e.entity_type='L' "
        "LEFT JOIN locations lo ON lo.unique_system_id=f.unique_system_id AND lo.location_number=f.location_number "
        "WHERE f.frequency_assigned_mhz BETWEEN ? AND ? "
        "ORDER BY abs(f.frequency_assigned_mhz-?) LIMIT ?",
        (center_mhz - delta, center_mhz + delta, center_mhz, limit),
    ))


def nearby(connection: sqlite3.Connection, latitude: float, longitude: float, radius_km: float, limit: int = 100) -> list[dict[str, Any]]:
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError("latitude/longitude are out of range")
    if not math.isfinite(radius_km) or not 0 < radius_km <= 1_000:
        raise ValueError("radius_km must be greater than 0 and at most 1000")
    limit = _limit(limit)
    lat_delta = radius_km / 111.32
    lon_scale = max(math.cos(math.radians(latitude)), 0.01)
    lon_delta = radius_km / (111.32 * lon_scale)
    rows = row_dicts(connection.execute(
        "SELECT lo.id,lo.unique_system_id,lo.callsign,lo.latitude,lo.longitude,lo.county,lo.state,"
        "l.radio_service_code,l.license_status,e.display_name "
        "FROM location_rtree r JOIN locations lo ON lo.id=r.location_id "
        "JOIN licenses l USING(unique_system_id) "
        "LEFT JOIN entities e ON e.unique_system_id=l.unique_system_id AND e.entity_type='L' "
        "WHERE r.max_latitude>=? AND r.min_latitude<=? AND r.max_longitude>=? AND r.min_longitude<=?",
        (latitude - lat_delta, latitude + lat_delta, longitude - lon_delta, longitude + lon_delta),
    ))
    for row in rows:
        row["distance_km"] = _haversine(latitude, longitude, row["latitude"], row["longitude"])
    rows = [row for row in rows if row["distance_km"] <= radius_km]
    rows.sort(key=lambda row: row["distance_km"])
    return rows[:limit]


def describe_schema(connection: sqlite3.Connection, table: str | None = None) -> dict[str, Any]:
    """Describe queryable user tables without exposing SQLite internals."""
    tables = [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type IN ('table','view') "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts_%' "
            "AND name NOT LIKE 'location_rtree_%' ORDER BY name"
        )
        if not _is_retired_scope_table(row[0])
    ]
    if table:
        if table not in tables:
            raise ValueError(f"Unknown table or view: {table}")
        return {"table": table, "columns": _table_columns(connection, table)}
    groups = {
        "normalized": [name for name in tables if not name.startswith("raw_") and name != "uls_raw_catalog"],
        "raw": [name for name in tables if name.startswith("raw_")],
    }
    return {
        "groups": groups,
        "raw_record_definitions": connection.execute("SELECT count(*) FROM uls_raw_catalog").fetchone()[0],
        "hint": "Call describe_schema with a table name for exact columns; raw_* tables preserve every FCC field.",
    }


def _table_columns(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    escaped = table.replace('"', '""')
    return [
        {"name": row[1], "type": row[2], "not_null": bool(row[3]), "primary_key": bool(row[5])}
        for row in connection.execute(f'PRAGMA table_info("{escaped}")')
    ]


def database_status(connection: sqlite3.Connection) -> dict[str, Any]:
    """Return provenance and inexpensive table counts for agent orientation."""
    path_value = connection.execute("PRAGMA database_list").fetchone()[2]
    source_rows = row_dicts(connection.execute(
        "SELECT source_key,url,retrieved_at,last_modified,sha256,byte_size,raw_parser_version,record_counts_json,schema_drift_json "
        "FROM sources WHERE active=1 ORDER BY retrieved_at DESC"
    ))
    for row in source_rows:
        row["record_counts"] = json.loads(row.pop("record_counts_json"))
        row["schema_drift"] = json.loads(row.pop("schema_drift_json"))
    normalized = {}
    for table in ("licenses", "entities", "locations", "antennas", "frequencies", "emissions"):
        normalized[table] = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    return {
        "database": path_value,
        "database_bytes": Path(path_value).stat().st_size if path_value else None,
        "schema_version": connection.execute(
            "SELECT value FROM metadata WHERE key='schema_version'"
        ).fetchone()[0],
        "normalized_counts": normalized,
        "raw_record_definitions": connection.execute("SELECT count(*) FROM uls_raw_catalog").fetchone()[0],
        "sources": source_rows,
    }


def execute_readonly_sql(
    connection: sqlite3.Connection,
    sql: str,
    *,
    limit: int = 200,
    timeout_ms: int = 5_000,
    allow_sensitive: bool = False,
) -> dict[str, Any]:
    """Execute one bounded read-only statement on a read-only connection."""
    limit = _limit(limit)
    if not 1 <= timeout_ms <= 30_000:
        raise ValueError("timeout_ms must be between 1 and 30000")
    if not sql or len(sql) > MAX_SQL_LENGTH:
        raise ValueError(f"sql must be between 1 and {MAX_SQL_LENGTH} characters")
    if not READ_PREFIX.match(sql):
        raise ValueError("Only SELECT, WITH, and EXPLAIN statements are allowed")

    denied = {
        sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_CREATE_INDEX, sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_CREATE_TEMP_INDEX,
        sqlite3.SQLITE_CREATE_TEMP_TABLE, sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
        sqlite3.SQLITE_CREATE_TEMP_VIEW, sqlite3.SQLITE_CREATE_TRIGGER, sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_DROP_INDEX, sqlite3.SQLITE_DROP_TABLE, sqlite3.SQLITE_DROP_TEMP_INDEX,
        sqlite3.SQLITE_DROP_TEMP_TABLE, sqlite3.SQLITE_DROP_TEMP_TRIGGER, sqlite3.SQLITE_DROP_TEMP_VIEW,
        sqlite3.SQLITE_DROP_TRIGGER, sqlite3.SQLITE_DROP_VIEW, sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_REINDEX, sqlite3.SQLITE_ANALYZE, sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH,
    }

    def authorize(action: int, arg1: str | None, arg2: str | None, _db: str | None, _trigger: str | None) -> int:
        if action == sqlite3.SQLITE_READ and _is_retired_scope_table(arg1):
            return sqlite3.SQLITE_DENY
        if (
            not allow_sensitive
            and action == sqlite3.SQLITE_READ
            and (arg1 or "").startswith("raw_")
            and any(part in (arg2 or "").lower() for part in SENSITIVE_COLUMN_PARTS)
        ):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_DENY if action in denied else sqlite3.SQLITE_OK

    deadline = time.monotonic() + timeout_ms / 1000.0
    connection.set_authorizer(authorize)
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 10_000)
    started = time.monotonic()
    try:
        cursor = connection.execute(sql)
        rows = cursor.fetchmany(limit + 1)
        truncated = len(rows) > limit
        rows = rows[:limit]
        return {
            "columns": [column[0] for column in cursor.description or ()],
            "rows": [dict(row) for row in rows],
            "row_count": len(rows),
            "truncated": truncated,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        }
    finally:
        connection.set_progress_handler(None, 0)
        connection.set_authorizer(None)


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_km = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * earth_km * math.asin(math.sqrt(a))


# FCC publishes dates as MM/DD/YYYY text. Sorting or comparing that form
# lexicographically is wrong, so every date predicate goes through this.
def _sortable(column: str) -> str:
    return (
        f"CASE WHEN length({column})=10 "
        f"THEN substr({column},7,4)||substr({column},1,2)||substr({column},4,2) END"
    )


def _as_date(value: str | None, label: str) -> str | None:
    """Accept YYYY-MM-DD or MM/DD/YYYY and return sortable YYYYMMDD."""
    if not value:
        return None
    text = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text.replace("-", "")
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", text):
        return text[6:] + text[:2] + text[3:5]
    raise ValueError(f"{label} must be YYYY-MM-DD or MM/DD/YYYY")


def license_record(
    connection: sqlite3.Connection,
    *,
    unique_system_id: int | None = None,
    callsign_value: str | None = None,
) -> dict[str, Any]:
    """Assemble one license and every related normalized record.

    Agents otherwise need six separate joins to answer "tell me about this
    license", which is the most common question this dataset receives.
    """
    if (unique_system_id is None) == (callsign_value is None):
        raise ValueError("provide exactly one of unique_system_id or callsign")
    if unique_system_id is None:
        ids = [
            row["unique_system_id"]
            for row in connection.execute(
                "SELECT unique_system_id FROM licenses WHERE callsign=? "
                f"ORDER BY {_sortable('last_action_date')} DESC LIMIT 25",
                (str(callsign_value).upper(),),
            )
        ]
        if not ids:
            return {"found": False, "callsign": str(callsign_value).upper(), "licenses": []}
    else:
        ids = [int(unique_system_id)]

    records = []
    for usi in ids:
        header = connection.execute(
            "SELECT * FROM licenses WHERE unique_system_id=?", (usi,)
        ).fetchone()
        if header is None:
            continue
        record: dict[str, Any] = {"license": dict(header)}
        record["amateur"] = row_dicts(
            connection.execute("SELECT * FROM amateur WHERE unique_system_id=?", (usi,))
        )
        for name, order in (
            ("entities", "entity_type"),
            ("locations", "location_number"),
            ("antennas", "location_number,antenna_number"),
            ("frequencies", "frequency_assigned_mhz"),
            ("emissions", "frequency_assigned_mhz,emission_sequence_id"),
        ):
            record[name] = row_dicts(connection.execute(
                f"SELECT * FROM {name} WHERE unique_system_id=? ORDER BY {order}", (usi,)
            ))
        records.append(record)
    return {
        "found": bool(records),
        "unique_system_ids": [record["license"]["unique_system_id"] for record in records],
        "licenses": records,
    }


def text_search(connection: sqlite3.Connection, query: str, limit: int = 100) -> list[dict[str, Any]]:
    """Full-text search across callsign, licensee name, service, and state."""
    if not query or not query.strip():
        raise ValueError("query must not be empty")
    limit = _limit(limit)
    try:
        return row_dicts(connection.execute(
            "SELECT f.callsign,f.display_name,f.radio_service_code,f.state,"
            "f.unique_system_id,l.license_status,l.grant_date,l.expired_date "
            "FROM license_fts f JOIN licenses l ON l.unique_system_id=f.unique_system_id "
            "WHERE license_fts MATCH ? ORDER BY rank LIMIT ?",
            (query, limit),
        ))
    except sqlite3.OperationalError as error:
        raise ValueError(f"invalid FTS5 query: {error}") from error


def band_survey(
    connection: sqlite3.Connection,
    low_mhz: float,
    high_mhz: float,
    *,
    group_by: str = "service",
    state: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Summarize who is assigned across a frequency range."""
    if not math.isfinite(low_mhz) or not math.isfinite(high_mhz):
        raise ValueError("low_mhz and high_mhz must be finite numbers")
    if low_mhz < 0 or high_mhz <= low_mhz:
        raise ValueError("high_mhz must be greater than a non-negative low_mhz")
    if high_mhz - low_mhz > 1_000:
        raise ValueError("band width must be 1000 MHz or less")
    limit = _limit(limit)
    dimensions = {
        "service": "l.radio_service_code",
        "state": "lo.state",
        "licensee": "e.display_name",
        "class_station": "f.class_station_code",
    }
    if group_by not in dimensions:
        raise ValueError(f"group_by must be one of {sorted(dimensions)}")
    column = dimensions[group_by]
    parameters: list[Any] = [low_mhz, high_mhz]
    predicate = "f.frequency_assigned_mhz BETWEEN ? AND ?"
    if state:
        predicate += " AND lo.state = ?"
        parameters.append(state.upper())
    parameters.append(limit)
    groups = row_dicts(connection.execute(
        f"SELECT {column} AS group_value,count(*) AS assignments,"
        "count(DISTINCT f.unique_system_id) AS licenses,"
        "min(f.frequency_assigned_mhz) AS min_mhz,max(f.frequency_assigned_mhz) AS max_mhz,"
        "max(f.power_output_w) AS max_power_output_w,max(f.erp_w) AS max_erp_w "
        "FROM frequencies f JOIN licenses l USING(unique_system_id) "
        "LEFT JOIN entities e ON e.unique_system_id=f.unique_system_id AND e.entity_type='L' "
        "LEFT JOIN locations lo ON lo.unique_system_id=f.unique_system_id "
        "AND lo.location_number=f.location_number "
        f"WHERE {predicate} GROUP BY 1 ORDER BY assignments DESC LIMIT ?",
        parameters,
    ))
    return {
        "low_mhz": low_mhz,
        "high_mhz": high_mhz,
        "group_by": group_by,
        "state": state.upper() if state else None,
        "groups": groups,
    }


def radio_services(connection: sqlite3.Connection, limit: int = 200) -> dict[str, Any]:
    """Enumerate the radio service codes present, with counts and examples.

    ULS bulk archives carry the code but not its prose description, so this
    reports observed codes rather than inventing FCC definitions for them.
    """
    limit = _limit(limit)
    return {
        "note": (
            "Service code descriptions are not published in the ULS bulk archives. "
            "See FCC service-code documentation to expand these codes."
        ),
        "services": row_dicts(connection.execute(
            "SELECT l.radio_service_code,count(*) AS licenses,"
            "sum(CASE WHEN l.license_status='A' THEN 1 ELSE 0 END) AS active,"
            "min(l.source_archive) AS example_archive,"
            "max(l.callsign) AS example_callsign "
            "FROM licenses l WHERE l.radio_service_code IS NOT NULL "
            "GROUP BY 1 ORDER BY licenses DESC LIMIT ?",
            (limit,),
        )),
    }


def expirations(
    connection: sqlite3.Connection,
    *,
    start: str | None = None,
    end: str | None = None,
    service: str | None = None,
    state: str | None = None,
    status: str | None = "A",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List licenses whose expiration date falls inside a window."""
    limit = _limit(limit)
    sortable = _sortable("l.expired_date")
    where = [f"{sortable} IS NOT NULL"]
    parameters: list[Any] = []
    lower = _as_date(start, "start")
    upper = _as_date(end, "end")
    if lower:
        where.append(f"{sortable} >= ?")
        parameters.append(lower)
    if upper:
        where.append(f"{sortable} <= ?")
        parameters.append(upper)
    if service:
        where.append("l.radio_service_code = ?")
        parameters.append(service.upper())
    if state:
        where.append("e.state = ?")
        parameters.append(state.upper())
    if status:
        where.append("l.license_status = ?")
        parameters.append(status.upper())
    parameters.append(limit)
    return row_dicts(connection.execute(
        "SELECT DISTINCT l.unique_system_id,l.callsign,l.radio_service_code,l.license_status,"
        "l.grant_date,l.expired_date,e.display_name,e.state "
        "FROM licenses l LEFT JOIN entities e "
        "ON e.unique_system_id=l.unique_system_id AND e.entity_type='L' "
        f"WHERE {' AND '.join(where)} ORDER BY {sortable} ASC,l.callsign LIMIT ?",
        parameters,
    ))


def geography(
    connection: sqlite3.Connection,
    *,
    level: str = "state",
    service: str | None = None,
    state: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Count licensed sites by state or county."""
    if level not in {"state", "county"}:
        raise ValueError("level must be 'state' or 'county'")
    limit = _limit(limit)
    columns = "lo.state" if level == "state" else "lo.state,lo.county"
    where = ["lo.state IS NOT NULL"]
    parameters: list[Any] = []
    if service:
        where.append("l.radio_service_code = ?")
        parameters.append(service.upper())
    if state:
        where.append("lo.state = ?")
        parameters.append(state.upper())
    parameters.append(limit)
    return {
        "level": level,
        "service": service.upper() if service else None,
        "areas": row_dicts(connection.execute(
            f"SELECT {columns},count(*) AS sites,"
            "count(DISTINCT lo.unique_system_id) AS licenses "
            "FROM locations lo JOIN licenses l USING(unique_system_id) "
            f"WHERE {' AND '.join(where)} GROUP BY {columns} "
            "ORDER BY sites DESC LIMIT ?",
            parameters,
        )),
    }
