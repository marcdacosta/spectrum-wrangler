"""Download and normalize FCC Universal Licensing System archives."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence

from .db import load_uls_schema, quote_identifier, raw_table_name


FCC_AUTHORITY = "Federal Communications Commission, Universal Licensing System"
FCC_COMPLETE_URL = "https://data.fcc.gov/download/pub/uls/complete/"
FCC_DOCUMENTATION_URL = "https://www.fcc.gov/wireless/data/public-access-files-database-downloads"
USER_AGENT = "spectrum-wrangler/0.3 (+https://github.com/marcdacosta/spectrum-wrangler)"
RAW_PARSER_VERSION = 2

# Current FCC weekly license archives. The names are stable; the directory
# listing is still inspected so a vanished or newly added archive is visible.
LICENSE_ARCHIVES = (
    "l_LMbcast.zip",
    "l_LMcomm.zip",
    "l_LMpriv.zip",
    "l_aircr.zip",
    "l_amat.zip",
    "l_cell.zip",
    "l_coast.zip",
    "l_frc.zip",
    "l_gmrs.zip",
    "l_market.zip",
    "l_mdsitfs.zip",
    "l_micro.zip",
    "l_paging.zip",
    "l_ship.zip",
)

ARCHIVE_ALIASES = {
    "amateur": "l_amat.zip",
    "aircraft": "l_aircr.zip",
    "cellular": "l_cell.zip",
    "coast": "l_coast.zip",
    "commercial": "l_LMcomm.zip",
    "frc": "l_frc.zip",
    "gmrs": "l_gmrs.zip",
    "land-mobile-broadcast": "l_LMbcast.zip",
    "market": "l_market.zip",
    "mds-itfs": "l_mdsitfs.zip",
    "microwave": "l_micro.zip",
    "paging": "l_paging.zip",
    "private": "l_LMpriv.zip",
    "ship": "l_ship.zip",
}


@dataclass(frozen=True)
class Download:
    path: Path
    url: str
    sha256: str
    byte_size: int
    retrieved_at: str
    last_modified: str | None
    etag: str | None
    cached: bool = False


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _value(columns: Sequence[str], index: int) -> str | None:
    if index >= len(columns):
        return None
    value = columns[index].strip()
    return value or None


def _integer(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def _number(value: str | None) -> float | None:
    try:
        number = float(value) if value is not None else None
        return number if number is None or math.isfinite(number) else None
    except ValueError:
        return None


def _boolean(value: str | None) -> int | None:
    if value is None:
        return None
    return 1 if value.upper() == "Y" else 0


def _dms(degrees: str | None, minutes: str | None, seconds: str | None, direction: str | None) -> float | None:
    deg = _number(degrees)
    minute = _number(minutes)
    second = _number(seconds)
    if deg is None or minute is None or second is None:
        return None
    sign = -1 if (direction or "").upper() in {"S", "W"} else 1
    return sign * (deg + minute / 60.0 + second / 3600.0)


def list_official_archives() -> list[str]:
    request = urllib.request.Request(FCC_COMPLETE_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        html = response.read().decode("utf-8", errors="replace")
    return sorted(set(re.findall(r'href="(l_[^"/]+\.zip)"', html, flags=re.IGNORECASE)))


def resolve_archives(requested: Sequence[str]) -> list[str]:
    if not requested or requested == ["all"]:
        return list(LICENSE_ARCHIVES)
    resolved: list[str] = []
    for value in requested:
        name = ARCHIVE_ALIASES.get(value, value)
        if not name.endswith(".zip"):
            raise ValueError(f"Unknown ULS archive or alias: {value}")
        if name not in LICENSE_ARCHIVES:
            raise ValueError(f"Archive is not in the reviewed license set: {name}")
        if name not in resolved:
            resolved.append(name)
    return resolved


def download_archive(
    name: str, cache_dir: str | Path, *, force: bool = False,
    on_progress: Callable[[int, int], None] | None = None,
) -> Download:
    destination = Path(cache_dir).expanduser().resolve() / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    metadata_path = destination.with_suffix(destination.suffix + ".json")
    if destination.exists() and metadata_path.exists() and not force:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        digest = _sha256(destination)
        if digest == metadata.get("sha256"):
            return Download(destination, metadata["url"], digest, destination.stat().st_size,
                            metadata["retrieved_at"], metadata.get("last_modified"),
                            metadata.get("etag"), cached=True)

    url = FCC_COMPLETE_URL + name
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    temporary = destination.with_suffix(destination.suffix + ".part")
    digest = hashlib.sha256()
    size = 0
    try:
        with urllib.request.urlopen(request, timeout=120) as response, temporary.open("wb") as output:
            last_modified = response.headers.get("Last-Modified")
            etag = response.headers.get("ETag")
            total = int(response.headers.get("Content-Length") or 0)
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
                size += len(chunk)
                if on_progress:
                    on_progress(size, total)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    if not zipfile.is_zipfile(temporary):
        temporary.unlink(missing_ok=True)
        raise ValueError(f"FCC response for {name} was not a valid ZIP archive")
    temporary.replace(destination)
    metadata = {
        "url": url,
        "retrieved_at": utc_now(),
        "last_modified": last_modified,
        "etag": etag,
        "sha256": digest.hexdigest(),
        "byte_size": size,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return Download(destination, url, digest.hexdigest(), size, metadata["retrieved_at"], last_modified, etag)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _lines(archive: zipfile.ZipFile, member: str) -> Iterator[list[str]]:
    """Yield logical ULS records, merging unescaped multiline text fields."""
    expected_record_type = Path(member).stem.upper()
    pending: list[str] | None = None
    with archive.open(member) as source:
        for raw in source:
            line = raw.decode("latin-1").rstrip("\r\n")
            if not line:
                continue
            values = line.split("|")
            if values[0].upper() == expected_record_type:
                if pending is not None:
                    yield pending
                pending = values
                continue
            if pending is None:
                continue
            continuation = line.strip("|").strip()
            if not continuation:
                continue
            last_content = len(pending) - 1
            while last_content > 0 and not pending[last_content]:
                last_content -= 1
            separator = "\n" if pending[last_content] else ""
            pending[last_content] += separator + continuation
    if pending is not None:
        yield pending


def _batch_insert(
    connection: sqlite3.Connection, sql: str, rows: Iterable[tuple], *,
    batch_size: int = 10_000, on_batch: Callable[[int], None] | None = None,
) -> int:
    batch: list[tuple] = []
    count = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            connection.executemany(sql, batch)
            count += len(batch)
            batch.clear()
            if on_batch:
                on_batch(count)
    if batch:
        connection.executemany(sql, batch)
        count += len(batch)
        if on_batch:
            on_batch(count)
    return count


def _hd_rows(lines: Iterable[list[str]], source_id: int, archive_name: str) -> Iterator[tuple]:
    for c in lines:
        unique_id = _integer(_value(c, 1))
        if unique_id is None:
            continue
        yield (
            unique_id, source_id, archive_name, _value(c, 4), _value(c, 5), _value(c, 6),
            _value(c, 7), _value(c, 8), _value(c, 9), _value(c, 42), _value(c, 43),
            _boolean(_value(c, 21)), _boolean(_value(c, 23)), _boolean(_value(c, 24)),
            _boolean(_value(c, 25)), _boolean(_value(c, 26)), _boolean(_value(c, 27)),
            _boolean(_value(c, 28)),
        )


def _am_rows(lines: Iterable[list[str]]) -> Iterator[tuple]:
    for c in lines:
        unique_id = _integer(_value(c, 1))
        if unique_id is not None:
            yield (unique_id, _value(c, 4), _value(c, 5), _value(c, 6), _integer(_value(c, 7)),
                   _value(c, 8), _value(c, 15), _value(c, 16))


def _en_rows(lines: Iterable[list[str]]) -> Iterator[tuple]:
    for c in lines:
        unique_id = _integer(_value(c, 1))
        entity_type = _value(c, 5)
        if unique_id is None or entity_type is None:
            continue
        entity_name = _value(c, 7)
        if not entity_name:
            parts = [part for part in (_value(c, 8), _value(c, 9), _value(c, 10), _value(c, 11)) if part]
            entity_name = " ".join(parts) or None
        # FRN is the FCC Registration Number: the licensee's durable public
        # identifier, and the only reliable way to group an organization whose
        # name is typed differently on every filing.
        yield (unique_id, _value(c, 4), entity_type, entity_name, _value(c, 22),
               _value(c, 17), _value(c, 23), _value(c, 25), _value(c, 26))


def _lo_rows(lines: Iterable[list[str]]) -> Iterator[tuple]:
    for c in lines:
        unique_id = _integer(_value(c, 1))
        location_number = _integer(_value(c, 8))
        if unique_id is None or location_number is None:
            continue
        latitude = _dms(_value(c, 19), _value(c, 20), _value(c, 21), _value(c, 22))
        longitude = _dms(_value(c, 23), _value(c, 24), _value(c, 25), _value(c, 26))
        yield (unique_id, _value(c, 4), location_number, _value(c, 6), _value(c, 7),
               _value(c, 9), _value(c, 13), _value(c, 14), _number(_value(c, 15)),
               _number(_value(c, 18)), latitude, longitude, _value(c, 37),
               _number(_value(c, 38)), _number(_value(c, 39)), _value(c, 40),
               _value(c, 42), _value(c, 48), _value(c, 49))


def _an_rows(lines: Iterable[list[str]]) -> Iterator[tuple]:
    for c in lines:
        unique_id, antenna_number, location_number = (_integer(_value(c, i)) for i in (1, 6, 7))
        if unique_id is None or antenna_number is None:
            continue
        yield (unique_id, _value(c, 4), location_number, antenna_number, _value(c, 9),
               _number(_value(c, 10)), _number(_value(c, 11)), _value(c, 12), _value(c, 13),
               _number(_value(c, 14)), _value(c, 15), _number(_value(c, 16)),
               _number(_value(c, 17)), _number(_value(c, 18)), _number(_value(c, 19)),
               _number(_value(c, 37)), _value(c, 34), _value(c, 35))


def _fr_rows(lines: Iterable[list[str]]) -> Iterator[tuple]:
    for c in lines:
        unique_id = _integer(_value(c, 1))
        frequency = _number(_value(c, 10))
        if unique_id is None or frequency is None:
            continue
        yield (unique_id, _value(c, 4), _integer(_value(c, 6)), _integer(_value(c, 7)),
               _integer(_value(c, 26)), _value(c, 8), frequency, _number(_value(c, 11)),
               _number(_value(c, 12)), _number(_value(c, 15)), _number(_value(c, 16)),
               _number(_value(c, 20)), _value(c, 27), _value(c, 28))


def _em_rows(lines: Iterable[list[str]]) -> Iterator[tuple]:
    for c in lines:
        unique_id = _integer(_value(c, 1))
        if unique_id is None:
            continue
        yield (unique_id, _value(c, 4), _integer(_value(c, 5)), _integer(_value(c, 6)),
               _integer(_value(c, 12)), _number(_value(c, 7)), _value(c, 9),
               _number(_value(c, 10)), _value(c, 11), _integer(_value(c, 15)),
               _value(c, 13), _value(c, 14))


MEMBERS: dict[str, tuple[str, str, Callable[..., Iterator[tuple]]]] = {
    "HD.dat": (
        "INSERT OR REPLACE INTO licenses VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        "licenses", _hd_rows,
    ),
    "AM.dat": (
        "INSERT OR REPLACE INTO amateur VALUES (?,?,?,?,?,?,?,?)",
        "amateur", _am_rows,
    ),
    "EN.dat": (
        "INSERT OR IGNORE INTO entities(unique_system_id,callsign,entity_type,display_name,frn,state,applicant_type,status_code,status_date) VALUES (?,?,?,?,?,?,?,?,?)",
        "entities", _en_rows,
    ),
    "LO.dat": (
        "INSERT OR REPLACE INTO locations(unique_system_id,callsign,location_number,location_type,location_class,site_status,county,state,radius_km,ground_elevation_m,latitude,longitude,tower_registration_number,support_height_m,overall_height_m,structure_type,location_name,status_code,status_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        "locations", _lo_rows,
    ),
    "AN.dat": (
        "INSERT OR REPLACE INTO antennas(unique_system_id,callsign,location_number,antenna_number,antenna_type,height_to_tip_m,height_to_center_m,make,model,tilt_deg,polarization,beamwidth_deg,gain_dbi,azimuth_deg,haat_m,maximum_erp_w,status_code,status_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        "antennas", _an_rows,
    ),
    "FR.dat": (
        "INSERT OR REPLACE INTO frequencies(unique_system_id,callsign,location_number,antenna_number,frequency_number,class_station_code,frequency_assigned_mhz,frequency_upper_mhz,carrier_frequency_mhz,power_output_w,erp_w,eirp_w,status_code,status_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        "frequencies", _fr_rows,
    ),
    "EM.dat": (
        "INSERT INTO emissions(unique_system_id,callsign,location_number,antenna_number,frequency_number,frequency_assigned_mhz,emission_code,digital_mod_rate,digital_mod_type,emission_sequence_id,status_code,status_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        "emissions", _em_rows,
    ),
}


def _raw_rows(
    lines: Iterable[list[str]],
    archive_name: str,
    source_id: int,
    column_count: int,
    metrics: dict[str, int],
) -> Iterator[tuple]:
    """Pad known fields and preserve any future FCC fields as JSON."""
    for source_row, values in enumerate(lines, start=1):
        observed = len(values)
        metrics["minimum_observed_fields"] = min(
            metrics.get("minimum_observed_fields", observed), observed
        )
        metrics["maximum_observed_fields"] = max(
            metrics.get("maximum_observed_fields", observed), observed
        )
        if observed > column_count:
            metrics["rows_with_extra_fields"] = metrics.get("rows_with_extra_fields", 0) + 1
        elif observed < column_count:
            metrics["rows_with_missing_trailing_fields"] = (
                metrics.get("rows_with_missing_trailing_fields", 0) + 1
            )
        known = [value if value != "" else None for value in values[:column_count]]
        known.extend([None] * (column_count - len(known)))
        extra = values[column_count:]
        yield (
            archive_name,
            source_id,
            source_row,
            *known,
            json.dumps(extra, ensure_ascii=False) if extra else None,
        )


def _unknown_rows(
    lines: Iterable[list[str]], archive_name: str, source_id: int, member_name: str
) -> Iterator[tuple]:
    for source_row, values in enumerate(lines, start=1):
        yield (
            archive_name,
            source_id,
            member_name,
            source_row,
            values[0] if values else None,
            json.dumps(values, ensure_ascii=False),
        )


def _import_raw_member(
    connection: sqlite3.Connection,
    archive: zipfile.ZipFile,
    member: str,
    archive_name: str,
    source_id: int,
    definitions: dict[str, list[str]],
    on_progress: Callable[[str, int], None] | None = None,
) -> tuple[str, int, dict[str, int]]:
    record_type = Path(member).stem.upper()
    on_batch = (lambda n: on_progress(record_type, n)) if on_progress else None
    columns = definitions.get(record_type)
    if columns is None:
        connection.execute(
            "DELETE FROM raw_unknown WHERE source_archive=? AND member_name=?",
            (archive_name, member),
        )
        sql = (
            "INSERT OR REPLACE INTO raw_unknown(source_archive,source_id,member_name,source_row,record_type,fields_json) "
            "VALUES(?,?,?,?,?,?)"
        )
        count = _batch_insert(
            connection,
            sql,
            _unknown_rows(_lines(archive, member), archive_name, source_id, member),
            on_batch=on_batch,
        )
        return f"raw.UNKNOWN.{record_type}", count, {"unknown_record_type_rows": count}

    table_name = raw_table_name(record_type)
    index_columns = [column for column in ("unique_system_identifier", "call_sign", "callsign") if column in columns]
    for column in index_columns:
        suffix = "usi" if column == "unique_system_identifier" else "callsign"
        connection.execute(f"DROP INDEX IF EXISTS {quote_identifier(table_name + '_' + suffix + '_idx')}")
    connection.execute(
        f"DELETE FROM {quote_identifier(table_name)} WHERE source_archive=?", (archive_name,)
    )
    insert_columns = ["source_archive", "source_id", "source_row", *columns, "extra_fields_json"]
    quoted_columns = ",".join(quote_identifier(column) for column in insert_columns)
    placeholders = ",".join("?" for _ in insert_columns)
    sql = f"INSERT OR REPLACE INTO {quote_identifier(table_name)}({quoted_columns}) VALUES({placeholders})"
    metrics: dict[str, int] = {"documented_fields": len(columns)}
    count = _batch_insert(
        connection,
        sql,
        _raw_rows(_lines(archive, member), archive_name, source_id, len(columns), metrics),
        on_batch=on_batch,
    )
    for column in index_columns:
        suffix = "usi" if column == "unique_system_identifier" else "callsign"
        connection.execute(
            f"CREATE INDEX {quote_identifier(table_name + '_' + suffix + '_idx')} "
            f"ON {quote_identifier(table_name)}({quote_identifier(column)})"
        )
    drift = {
        key: value
        for key, value in metrics.items()
        if key == "documented_fields"
        or key.startswith("rows_with_")
        or key in {"minimum_observed_fields", "maximum_observed_fields"}
    }
    if not any(key.startswith("rows_with_") for key in drift):
        drift = {}
    return f"raw.{record_type}", count, drift


def import_archive(
    connection: sqlite3.Connection,
    download: Download,
    *,
    replace: bool = True,
    full_raw: bool = True,
    normalized: bool = True,
    on_progress: Callable[[str, int], None] | None = None,
) -> dict[str, int]:
    """Stream one complete ULS archive into fast normalized and lossless raw tables."""
    archive_name = download.path.name
    source_key = f"uls-complete:{archive_name}:{download.sha256}"
    connection.execute("UPDATE sources SET active=0 WHERE url=?", (download.url,))
    connection.execute(
        "INSERT OR IGNORE INTO sources(source_key,authority,url,retrieved_at,published_at,etag,last_modified,sha256,byte_size,license_text,notes) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (source_key, FCC_AUTHORITY, download.url, download.retrieved_at, download.last_modified,
         download.etag, download.last_modified, download.sha256, download.byte_size,
         "United States government public access data",
         "All FCC fields are retained in raw_* tables; common fields are also normalized and indexed."),
    )
    source_id = connection.execute("SELECT id FROM sources WHERE source_key=?", (source_key,)).fetchone()[0]
    connection.execute(
        "UPDATE sources SET notes=?,active=1 WHERE id=?",
        ("All FCC fields are retained in raw_* tables; common fields are also normalized and indexed.", source_id),
    )
    if replace and normalized:
        connection.execute("DELETE FROM licenses WHERE source_archive=?", (archive_name,))
    if normalized:
        counts: dict[str, int] = {}
        schema_drift: dict[str, dict[str, int]] = {}
    else:
        existing = connection.execute(
            "SELECT record_counts_json,schema_drift_json FROM sources WHERE id=?", (source_id,)
        ).fetchone()
        counts = json.loads(existing[0])
        schema_drift = json.loads(existing[1])
    with zipfile.ZipFile(download.path) as archive:
        names = set(archive.namelist())
        if normalized:
            for member in ("HD.dat", "AM.dat", "EN.dat", "LO.dat", "AN.dat", "FR.dat", "EM.dat"):
                if member not in names:
                    continue
                sql, label, parser = MEMBERS[member]
                args = (_lines(archive, member), source_id, archive_name) if member == "HD.dat" else (_lines(archive, member),)
                counts[label] = _batch_insert(
                    connection, sql, parser(*args),
                    on_batch=(lambda n, label=label: on_progress(label, n))
                    if on_progress else None,
                )
        if full_raw:
            definitions = load_uls_schema()["tables"]
            data_members = sorted(
                name for name in names if not name.endswith("/") and name.lower().endswith(".dat")
            )
            for member in data_members:
                label, count, drift = _import_raw_member(
                    connection, archive, member, archive_name, source_id, definitions,
                    on_progress=on_progress,
                )
                counts[label] = count
                record_type = Path(member).stem.upper()
                if drift:
                    schema_drift[record_type] = drift
                else:
                    schema_drift.pop(record_type, None)
    if full_raw:
        connection.execute(
            "UPDATE sources SET record_counts_json=?,schema_drift_json=?,raw_parser_version=? WHERE id=?",
            (
                json.dumps(counts, sort_keys=True),
                json.dumps(schema_drift, sort_keys=True),
                RAW_PARSER_VERSION,
                source_id,
            ),
        )
    else:
        connection.execute(
            "UPDATE sources SET record_counts_json=?,schema_drift_json=? WHERE id=?",
            (json.dumps(counts, sort_keys=True), json.dumps(schema_drift, sort_keys=True), source_id),
        )
    connection.commit()
    return counts


def imported_counts(
    connection: sqlite3.Connection, download: Download, *, require_raw: bool = True
) -> dict[str, int] | None:
    """Return prior counts when this exact archive is already materialized."""
    source_key = f"uls-complete:{download.path.name}:{download.sha256}"
    row = connection.execute(
        "SELECT id,record_counts_json,raw_parser_version FROM sources WHERE source_key=?", (source_key,)
    ).fetchone()
    if row is None:
        return None
    license_count = connection.execute(
        "SELECT count(*) FROM licenses WHERE source_id=?", (row[0],)
    ).fetchone()[0]
    counts = json.loads(row[1])
    if not license_count or counts.get("licenses") != license_count:
        return None
    if require_raw:
        if row[2] != RAW_PARSER_VERSION:
            return None
        raw_hd_count = counts.get("raw.HD")
        if not raw_hd_count:
            return None
        materialized = connection.execute(
            "SELECT count(*) FROM raw_hd WHERE source_archive=?", (download.path.name,)
        ).fetchone()[0]
        if materialized != raw_hd_count:
            return None
    return counts


def rebuild_indexes(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM location_rtree")
    connection.execute(
        "INSERT INTO location_rtree(location_id,min_latitude,max_latitude,min_longitude,max_longitude) "
        "SELECT id,latitude,latitude,longitude,longitude FROM locations "
        "WHERE latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"
    )
    connection.execute("DELETE FROM license_fts")
    connection.execute(
        "INSERT INTO license_fts(callsign,display_name,radio_service_code,state,unique_system_id) "
        "SELECT l.callsign, group_concat(DISTINCT e.display_name), l.radio_service_code, "
        "group_concat(DISTINCT e.state), l.unique_system_id "
        "FROM licenses l LEFT JOIN entities e ON e.unique_system_id=l.unique_system_id "
        "GROUP BY l.unique_system_id"
    )
    connection.execute("INSERT INTO metadata(key,value) VALUES('indexes_rebuilt_at',?) "
                       "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (utc_now(),))
    connection.commit()


def write_manifest(connection: sqlite3.Connection, path: str | Path) -> None:
    rows = [dict(row) for row in connection.execute(
        "SELECT source_key,authority,url,retrieved_at,published_at,last_modified,etag,sha256,byte_size,raw_parser_version,record_counts_json,schema_drift_json,notes "
        "FROM sources WHERE active=1 ORDER BY retrieved_at,source_key"
    )]
    for row in rows:
        row["record_counts"] = json.loads(row.pop("record_counts_json"))
        row["schema_drift"] = json.loads(row.pop("schema_drift_json"))
    payload = {
        "generated_at": utc_now(),
        "fcc_documentation": FCC_DOCUMENTATION_URL,
        "data_profile": "lossless FCC raw tables plus indexed normalized query tables",
        "sources": rows,
    }
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
