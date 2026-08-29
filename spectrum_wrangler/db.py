"""SQLite schema, migrations, and read-only connection helpers."""

from __future__ import annotations

import json
import re
import sqlite3
from importlib.resources import files
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 6
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    source_key TEXT NOT NULL UNIQUE,
    authority TEXT NOT NULL,
    url TEXT NOT NULL,
    retrieved_at TEXT NOT NULL,
    published_at TEXT,
    etag TEXT,
    last_modified TEXT,
    sha256 TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    record_counts_json TEXT NOT NULL DEFAULT '{}',
    schema_drift_json TEXT NOT NULL DEFAULT '{}',
    raw_parser_version INTEGER NOT NULL DEFAULT 1,
    license_text TEXT,
    notes TEXT,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS licenses (
    unique_system_id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    source_archive TEXT NOT NULL,
    callsign TEXT,
    license_status TEXT,
    radio_service_code TEXT,
    grant_date TEXT,
    expired_date TEXT,
    cancellation_date TEXT,
    effective_date TEXT,
    last_action_date TEXT,
    common_carrier INTEGER,
    private_comm INTEGER,
    fixed INTEGER,
    mobile INTEGER,
    radiolocation INTEGER,
    satellite INTEGER,
    developmental INTEGER
);

CREATE INDEX IF NOT EXISTS licenses_callsign_idx ON licenses(callsign);
CREATE INDEX IF NOT EXISTS licenses_service_idx ON licenses(radio_service_code);
CREATE INDEX IF NOT EXISTS licenses_status_idx ON licenses(license_status);
CREATE INDEX IF NOT EXISTS licenses_source_archive_idx ON licenses(source_archive);

CREATE TABLE IF NOT EXISTS amateur (
    unique_system_id INTEGER PRIMARY KEY REFERENCES licenses(unique_system_id) ON DELETE CASCADE,
    callsign TEXT,
    operator_class TEXT,
    group_code TEXT,
    region_code INTEGER,
    trustee_callsign TEXT,
    previous_callsign TEXT,
    previous_operator_class TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY,
    unique_system_id INTEGER NOT NULL REFERENCES licenses(unique_system_id) ON DELETE CASCADE,
    callsign TEXT,
    entity_type TEXT NOT NULL,
    display_name TEXT,
    state TEXT,
    applicant_type TEXT,
    status_code TEXT,
    status_date TEXT,
    UNIQUE(unique_system_id, entity_type, display_name)
);

CREATE INDEX IF NOT EXISTS entities_license_idx ON entities(unique_system_id);
CREATE INDEX IF NOT EXISTS entities_state_idx ON entities(state);
CREATE INDEX IF NOT EXISTS entities_display_name_idx ON entities(display_name);

CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY,
    unique_system_id INTEGER NOT NULL REFERENCES licenses(unique_system_id) ON DELETE CASCADE,
    callsign TEXT,
    location_number INTEGER NOT NULL,
    location_type TEXT,
    location_class TEXT,
    site_status TEXT,
    county TEXT,
    state TEXT,
    radius_km REAL,
    ground_elevation_m REAL,
    latitude REAL,
    longitude REAL,
    tower_registration_number TEXT,
    support_height_m REAL,
    overall_height_m REAL,
    structure_type TEXT,
    location_name TEXT,
    status_code TEXT,
    status_date TEXT,
    UNIQUE(unique_system_id, location_number)
);

CREATE INDEX IF NOT EXISTS locations_license_idx ON locations(unique_system_id);
CREATE INDEX IF NOT EXISTS locations_state_idx ON locations(state);
CREATE VIRTUAL TABLE IF NOT EXISTS location_rtree USING rtree(
    location_id,
    min_latitude, max_latitude,
    min_longitude, max_longitude
);

CREATE TABLE IF NOT EXISTS antennas (
    id INTEGER PRIMARY KEY,
    unique_system_id INTEGER NOT NULL REFERENCES licenses(unique_system_id) ON DELETE CASCADE,
    callsign TEXT,
    location_number INTEGER,
    antenna_number INTEGER,
    antenna_type TEXT,
    height_to_tip_m REAL,
    height_to_center_m REAL,
    make TEXT,
    model TEXT,
    tilt_deg REAL,
    polarization TEXT,
    beamwidth_deg REAL,
    gain_dbi REAL,
    azimuth_deg REAL,
    haat_m REAL,
    maximum_erp_w REAL,
    status_code TEXT,
    status_date TEXT,
    UNIQUE(unique_system_id, location_number, antenna_number)
);

CREATE INDEX IF NOT EXISTS antennas_license_idx ON antennas(unique_system_id);

CREATE TABLE IF NOT EXISTS frequencies (
    id INTEGER PRIMARY KEY,
    unique_system_id INTEGER NOT NULL REFERENCES licenses(unique_system_id) ON DELETE CASCADE,
    callsign TEXT,
    location_number INTEGER,
    antenna_number INTEGER,
    frequency_number INTEGER,
    class_station_code TEXT,
    frequency_assigned_mhz REAL,
    frequency_upper_mhz REAL,
    carrier_frequency_mhz REAL,
    power_output_w REAL,
    erp_w REAL,
    eirp_w REAL,
    status_code TEXT,
    status_date TEXT,
    UNIQUE(unique_system_id, location_number, antenna_number, frequency_number, frequency_assigned_mhz)
);

CREATE INDEX IF NOT EXISTS frequencies_license_idx ON frequencies(unique_system_id);
CREATE INDEX IF NOT EXISTS frequencies_assigned_idx ON frequencies(frequency_assigned_mhz);

CREATE TABLE IF NOT EXISTS emissions (
    id INTEGER PRIMARY KEY,
    unique_system_id INTEGER NOT NULL REFERENCES licenses(unique_system_id) ON DELETE CASCADE,
    callsign TEXT,
    location_number INTEGER,
    antenna_number INTEGER,
    frequency_number INTEGER,
    frequency_assigned_mhz REAL,
    emission_code TEXT,
    digital_mod_rate REAL,
    digital_mod_type TEXT,
    emission_sequence_id INTEGER,
    status_code TEXT,
    status_date TEXT
);

CREATE INDEX IF NOT EXISTS emissions_license_idx ON emissions(unique_system_id);
CREATE INDEX IF NOT EXISTS emissions_frequency_idx ON emissions(frequency_assigned_mhz);

CREATE VIRTUAL TABLE IF NOT EXISTS license_fts USING fts5(
    callsign,
    display_name,
    radio_service_code,
    state,
    unique_system_id UNINDEXED,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TABLE IF NOT EXISTS uls_raw_catalog (
    record_type TEXT PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    columns_json TEXT NOT NULL,
    definition_date TEXT NOT NULL,
    source_url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_unknown (
    source_archive TEXT NOT NULL,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    member_name TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    record_type TEXT,
    fields_json TEXT NOT NULL,
    PRIMARY KEY(source_archive, member_name, source_row)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS raw_unknown_record_type_idx ON raw_unknown(record_type);

"""


class ClosingConnection(sqlite3.Connection):
    """SQLite connection whose context manager also releases the file handle."""

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc_value, traceback))
        finally:
            self.close()


def quote_identifier(value: str) -> str:
    """Return a quoted SQLite identifier after a strict allow-list check."""
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def load_uls_schema() -> dict[str, Any]:
    """Load the checked-in FCC ULS record definition manifest."""
    resource = files("spectrum_wrangler").joinpath("uls_schema.json")
    return json.loads(resource.read_text(encoding="utf-8"))


def raw_table_name(record_type: str) -> str:
    code = record_type.lower()
    if not re.fullmatch(r"[a-z0-9]{2}", code):
        raise ValueError(f"Invalid ULS record type: {record_type!r}")
    return f"raw_{code}"


def initialize_raw_schema(connection: sqlite3.Connection) -> None:
    """Create lossless raw tables for all FCC-defined record types."""
    manifest = load_uls_schema()
    definition_date = manifest["definition_date"]
    source_url = manifest["source_url"]
    for record_type, columns in manifest["tables"].items():
        table_name = raw_table_name(record_type)
        column_sql = ",\n    ".join(f"{quote_identifier(column)} TEXT" for column in columns)
        connection.execute(
            f"""CREATE TABLE IF NOT EXISTS {quote_identifier(table_name)} (
    source_archive TEXT NOT NULL,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    source_row INTEGER NOT NULL,
    {column_sql},
    extra_fields_json TEXT,
    PRIMARY KEY(source_archive, source_row)
) WITHOUT ROWID"""
        )
        connection.execute(
            "INSERT INTO uls_raw_catalog(record_type,table_name,columns_json,definition_date,source_url) "
            "VALUES(?,?,?,?,?) ON CONFLICT(record_type) DO UPDATE SET "
            "table_name=excluded.table_name, columns_json=excluded.columns_json, "
            "definition_date=excluded.definition_date, source_url=excluded.source_url",
            (record_type, table_name, json.dumps(columns), definition_date, source_url),
        )

        if "unique_system_identifier" in columns:
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {quote_identifier(table_name + '_usi_idx')} "
                f"ON {quote_identifier(table_name)}(unique_system_identifier)"
            )
        call_column = "call_sign" if "call_sign" in columns else "callsign" if "callsign" in columns else None
        if call_column:
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {quote_identifier(table_name + '_callsign_idx')} "
                f"ON {quote_identifier(table_name)}({quote_identifier(call_column)})"
            )


def connect(path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    """Open a configured connection, optionally enforced read-only by SQLite."""
    db_path = Path(path).expanduser().resolve()
    if read_only:
        connection = sqlite3.connect(
            f"file:{db_path}?mode=ro", uri=True, factory=ClosingConnection
        )
        connection.execute("PRAGMA query_only = ON")
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(db_path, factory=ClosingConnection)
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA temp_store = MEMORY")
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    """Create or migrate the database schema without deleting loaded data."""
    connection.executescript(SCHEMA)
    source_columns = {row[1] for row in connection.execute("PRAGMA table_info(sources)")}
    if "active" not in source_columns:
        connection.execute("ALTER TABLE sources ADD COLUMN active INTEGER NOT NULL DEFAULT 1")
        connection.execute("UPDATE sources SET active=0")
        connection.execute(
            "UPDATE sources SET active=1 WHERE id IN (SELECT max(id) FROM sources GROUP BY url)"
        )
    if "schema_drift_json" not in source_columns:
        connection.execute(
            "ALTER TABLE sources ADD COLUMN schema_drift_json TEXT NOT NULL DEFAULT '{}'"
        )
    if "raw_parser_version" not in source_columns:
        connection.execute(
            "ALTER TABLE sources ADD COLUMN raw_parser_version INTEGER NOT NULL DEFAULT 1"
        )
    # Versions 4-5 briefly created unrelated research and local-radio tables.
    # They are no longer part of the public schema. Existing copies are left
    # untouched so upgrading never deletes a local user's data.
    initialize_raw_schema(connection)
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(SCHEMA_VERSION),),
    )
    connection.commit()
