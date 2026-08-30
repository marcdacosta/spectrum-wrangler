from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

from spectrum_wrangler.db import connect, initialize
from spectrum_wrangler.query import describe_schema, execute_readonly_sql, nearby
from spectrum_wrangler.uls import Download, _dms, import_archive, imported_counts, resolve_archives


class UlsTests(unittest.TestCase):
    def test_archive_aliases(self) -> None:
        self.assertEqual(resolve_archives(["amateur"]), ["l_amat.zip"])

    def test_dms_conversion_and_direction(self) -> None:
        self.assertAlmostEqual(_dms("40", "43", "31.2", "N"), 40.7253333333)
        self.assertAlmostEqual(_dms("74", "0", "27.7", "W"), -74.0076944444)
        self.assertIsNone(_dms(None, "0", "0", "N"))


class DatabaseTests(unittest.TestCase):
    def test_schema_and_spatial_query(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.sqlite3"
            with connect(path) as connection:
                initialize(connection)
                connection.execute(
                    "INSERT INTO sources(source_key,authority,url,retrieved_at,sha256,byte_size) VALUES(?,?,?,?,?,?)",
                    ("test", "test", "https://example.test", "2026-01-01T00:00:00Z", "0" * 64, 0),
                )
                connection.execute(
                    "INSERT INTO licenses(unique_system_id,source_id,source_archive,callsign,license_status,radio_service_code) VALUES(?,?,?,?,?,?)",
                    (1, 1, "test.zip", "TEST1", "A", "IG"),
                )
                connection.execute(
                    "INSERT INTO locations(unique_system_id,callsign,location_number,latitude,longitude) VALUES(?,?,?,?,?)",
                    (1, "TEST1", 1, 40.7128, -74.0060),
                )
                location_id = connection.execute("SELECT id FROM locations").fetchone()[0]
                connection.execute(
                    "INSERT INTO location_rtree VALUES(?,?,?,?,?)",
                    (location_id, 40.7128, 40.7128, -74.0060, -74.0060),
                )
                rows = nearby(connection, 40.7128, -74.0060, 1.0)
                self.assertEqual(rows[0]["callsign"], "TEST1")
                self.assertAlmostEqual(rows[0]["distance_km"], 0.0)

    def test_streaming_import_preserves_all_raw_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive_path = root / "l_test.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr(
                    "HD.dat",
                    "HD|42|FILE||N0CALL|A|HA|01/01/2020|01/01/2030|" + "|" * 50 + "\n",
                )
                archive.writestr("AM.dat", "AM|42|||N0CALL|T|I|2||||||||||\n")
                archive.writestr("EC.dat", "EC|42||N0CALL|||||Y|SCS\n")
                archive.writestr(
                    "CO.dat",
                    "CO|42||N0CALL|01/01/2020|First line||\n"
                    "continued text\n\n",
                )
                archive.writestr(
                    "EN.dat",
                    "EN|42|||N0CALL|L|LICENSEID|Example, Person|Person||Example||5551234567|"
                    "5551234568|private@example.test|1 Secret St|Brooklyn|NY|11201|||000|1234567890|I|"
                    "|A|01/01/2020|||\n",
                )
            download = Download(
                archive_path, "https://example.test/l_test.zip", "f" * 64, archive_path.stat().st_size,
                "2026-01-01T00:00:00+00:00", "2025-12-28T00:00:00Z", None,
            )
            with connect(root / "db.sqlite3") as connection:
                initialize(connection)
                counts = import_archive(connection, download)
                self.assertEqual(counts["licenses"], 1)
                row = connection.execute("SELECT * FROM entities").fetchone()
                self.assertEqual(row["display_name"], "Example, Person")
                self.assertEqual(row["state"], "NY")
                raw = connection.execute(
                    "SELECT phone,email,street_address,frn,zip_code FROM raw_en"
                ).fetchone()
                self.assertEqual(raw["phone"], "5551234567")
                self.assertEqual(raw["email"], "private@example.test")
                self.assertEqual(raw["street_address"], "1 Secret St")
                self.assertEqual(raw["frn"], "1234567890")
                self.assertEqual(raw["zip_code"], "11201")
                normalized_schema = " ".join(
                    item[1] for item in connection.execute("PRAGMA table_info(entities)")
                ).lower()
                # FRN is normalized: it is the organization key, not contact data.
                self.assertIn("frn", normalized_schema)
                # Bulk contact detail stays raw-only, because normalization is
                # about what queries need, not about withholding public record.
                for raw_only in ("phone", "email", "street_address", "zip_code"):
                    self.assertNotIn(raw_only, normalized_schema)
                # Every published field is queryable; there is no column gate.
                result = execute_readonly_sql(connection, "SELECT email FROM raw_en")
                self.assertEqual(result["rows"][0]["email"], "private@example.test")
                # Undocumented trailing fields are readable too: drift is only
                # useful if you can see what drifted.
                drift = execute_readonly_sql(
                    connection, "SELECT extra_fields_json FROM raw_en"
                )
                self.assertEqual(len(drift["rows"]), 1)
                self.assertEqual(counts["raw.HD"], 1)
                self.assertEqual(counts["raw.EN"], 1)
                self.assertEqual(counts["raw.CO"], 1)
                self.assertEqual(
                    connection.execute("SELECT description FROM raw_co").fetchone()[0],
                    "First line\ncontinued text",
                )
                self.assertEqual(
                    connection.execute("SELECT raw_parser_version FROM sources").fetchone()[0],
                    2,
                )
                drift = connection.execute(
                    "SELECT schema_drift_json FROM sources WHERE id=1"
                ).fetchone()[0]
                self.assertEqual(json.loads(drift)["EC"]["rows_with_extra_fields"], 1)
                self.assertEqual(imported_counts(connection, download), counts)

    def test_agent_sql_is_read_only_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.sqlite3"
            with connect(path) as connection:
                initialize(connection)
                connection.executemany(
                    "INSERT INTO metadata(key,value) VALUES(?,?)",
                    [("a", "1"), ("b", "2"), ("c", "3")],
                )
                connection.commit()
            with connect(path, read_only=True) as connection:
                result = execute_readonly_sql(
                    connection, "SELECT key,value FROM metadata ORDER BY key", limit=2
                )
                self.assertEqual(result["row_count"], 2)
                self.assertTrue(result["truncated"])
                with self.assertRaises(ValueError):
                    execute_readonly_sql(connection, "DELETE FROM metadata")

    def test_retired_scope_tables_are_not_created_or_exposed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.sqlite3"
            with connect(path) as connection:
                initialize(connection)
                tables = {
                    row[0] for row in connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type='table'"
                    )
                }
                self.assertNotIn("experiments", tables)
                self.assertNotIn("research_items", tables)

                # Preserve data from the short-lived v4-v5 scope on upgrade,
                # but keep it outside the public FCC query surface.
                connection.execute("CREATE TABLE experiments(id TEXT PRIMARY KEY, note TEXT)")
                connection.execute("INSERT INTO experiments VALUES('old-local-row', 'preserve me')")
                initialize(connection)
                self.assertEqual(
                    connection.execute("SELECT note FROM experiments").fetchone()[0],
                    "preserve me",
                )
                schema = describe_schema(connection)
                self.assertNotIn("experiments", schema["groups"]["normalized"])
                with self.assertRaises(sqlite3.DatabaseError):
                    execute_readonly_sql(connection, "SELECT * FROM experiments")

    def test_new_snapshot_replaces_archive_and_deactivates_prior_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_path = root / "first" / "l_test.zip"
            second_path = root / "second" / "l_test.zip"
            first_path.parent.mkdir()
            second_path.parent.mkdir()
            for path, unique_id, callsign in (
                (first_path, 42, "OLD1"),
                (second_path, 43, "NEW1"),
            ):
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr(
                        "HD.dat",
                        f"HD|{unique_id}|FILE||{callsign}|A|HA|01/01/2020|01/01/2030|"
                        + "|" * 50
                        + "\n",
                    )
            url = "https://example.test/l_test.zip"
            first = Download(
                first_path, url, "a" * 64, first_path.stat().st_size,
                "2026-01-01T00:00:00+00:00", "2025-12-28T00:00:00Z", None,
            )
            second = Download(
                second_path, url, "b" * 64, second_path.stat().st_size,
                "2026-01-08T00:00:00+00:00", "2026-01-04T00:00:00Z", None,
            )
            with connect(root / "db.sqlite3") as connection:
                initialize(connection)
                import_archive(connection, first)
                import_archive(connection, second)
                self.assertEqual(connection.execute(
                    "SELECT count(*) FROM sources WHERE active=1"
                ).fetchone()[0], 1)
                self.assertEqual(connection.execute(
                    "SELECT callsign FROM licenses"
                ).fetchone()[0], "NEW1")
                self.assertEqual(connection.execute(
                    "SELECT unique_system_identifier FROM raw_hd"
                ).fetchone()[0], "43")

