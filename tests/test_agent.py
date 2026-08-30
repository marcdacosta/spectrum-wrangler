from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from fixtures import build_fixture
from spectrum_wrangler.agent import EXIT_EMPTY, EXIT_ERROR, EXIT_OK, main
from spectrum_wrangler.db import connect
from spectrum_wrangler.query import (
    band_survey,
    expirations,
    geography,
    license_record,
    organization,
    radio_services,
    search_licenses,
    text_search,
)


class QueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "test.sqlite3"
        build_fixture(self.path)

    @contextlib.contextmanager
    def db(self):
        with connect(self.path, read_only=True) as connection:
            yield connection

    def test_license_record_assembles_every_relation(self) -> None:
        with self.db() as connection:
            record = license_record(connection, callsign_value="test1")
        self.assertTrue(record["found"])
        entry = record["licenses"][0]
        self.assertEqual(entry["license"]["callsign"], "TEST1")
        self.assertEqual(len(entry["locations"]), 1)
        self.assertEqual(len(entry["antennas"]), 1)
        self.assertEqual(entry["frequencies"][0]["frequency_assigned_mhz"], 462.5)
        self.assertEqual(entry["emissions"][0]["emission_code"], "11K0F3E")

    def test_license_record_reports_missing_callsign(self) -> None:
        with self.db() as connection:
            record = license_record(connection, callsign_value="NOPE")
        self.assertFalse(record["found"])
        self.assertEqual(record["licenses"], [])

    def test_license_record_requires_exactly_one_identifier(self) -> None:
        with self.db() as connection:
            with self.assertRaises(ValueError):
                license_record(connection)
            with self.assertRaises(ValueError):
                license_record(connection, unique_system_id=1, callsign_value="TEST1")

    def test_text_search_matches_and_rejects_bad_syntax(self) -> None:
        with self.db() as connection:
            hits = text_search(connection, "TEST1")
            self.assertEqual(hits[0]["callsign"], "TEST1")
            with self.assertRaises(ValueError):
                text_search(connection, "AND AND")
            with self.assertRaises(ValueError):
                text_search(connection, "  ")

    def test_band_survey_groups_and_validates(self) -> None:
        with self.db() as connection:
            survey = band_survey(connection, 462.0, 463.0, group_by="service")
            self.assertEqual(survey["groups"][0]["group_value"], "PW")
            self.assertEqual(survey["groups"][0]["assignments"], 1)
            with self.assertRaises(ValueError):
                band_survey(connection, 500.0, 400.0)
            with self.assertRaises(ValueError):
                band_survey(connection, 1.0, 2.0, group_by="nonsense")

    def test_expirations_order_by_real_date_not_text(self) -> None:
        """FCC dates are MM/DD/YYYY, so a text sort would misorder these."""
        with self.db() as connection:
            rows = expirations(connection, start="2020-01-01", end="2031-12-31", status=None)
        self.assertEqual(
            [row["expired_date"] for row in rows],
            ["02/02/2020", "09/15/2026", "01/10/2031"],
        )

    def test_expirations_window_and_date_validation(self) -> None:
        with self.db() as connection:
            rows = expirations(connection, start="2026-01-01", end="2026-12-31")
            self.assertEqual([row["callsign"] for row in rows], ["TEST1"])
            self.assertEqual(expirations(connection, start="09/01/2026", end="09/30/2026")[0]["callsign"], "TEST1")
            with self.assertRaises(ValueError):
                expirations(connection, start="2026")

    def test_search_orders_by_real_last_action_date(self) -> None:
        with self.db() as connection:
            rows = search_licenses(connection, state="NY")
        self.assertEqual([row["callsign"] for row in rows], ["TEST1", "TEST2", "TEST3"])

    def test_organization_reports_the_identity_spread(self) -> None:
        """One body files under two spellings; the report must show both."""
        with self.db() as connection:
            org = organization(connection, name="City of Test")
        self.assertTrue(org["found"])
        self.assertEqual(org["licenses"], 2)
        self.assertEqual(org["name_variants_count"], 2)
        self.assertEqual(org["matched_by"], "name")
        self.assertEqual(
            sorted(v["display_name"] for v in org["name_variants"]),
            ["CITY OF TEST", "City of Test"],
        )

    def test_organization_by_frn_is_exact(self) -> None:
        with self.db() as connection:
            org = organization(connection, frn="0001234567")
        self.assertEqual(org["matched_by"], "frn")
        self.assertEqual(org["licenses"], 2)
        self.assertEqual(org["distinct_frns"], 1)
        self.assertEqual([s["radio_service_code"] for s in org["services"]], ["PW"])

    def test_organization_warns_when_a_name_spans_several_frns(self) -> None:
        with self.db() as connection:
            org = organization(connection, name="e")
        self.assertGreater(org["distinct_frns"], 1)
        self.assertTrue(any("different FRNs" in c for c in org["caveats"]))

    def test_organization_warns_about_records_carrying_no_frn(self) -> None:
        with connect(self.path) as connection:
            connection.execute(
                "INSERT INTO entities(unique_system_id,callsign,entity_type,display_name,frn,state)"
                " VALUES(2,'TEST2','L','City of Test - Annex',NULL,'NY')"
            )
            connection.commit()
        with self.db() as connection:
            org = organization(connection, name="City of Test")
        self.assertEqual(org["records_without_frn"], 1)
        self.assertTrue(any("no FRN" in c for c in org["caveats"]))

    def test_organization_requires_exactly_one_identifier(self) -> None:
        with self.db() as connection:
            with self.assertRaises(ValueError):
                organization(connection)
            with self.assertRaises(ValueError):
                organization(connection, frn="1", name="x")

    def test_organization_reports_an_unknown_frn(self) -> None:
        with self.db() as connection:
            self.assertFalse(organization(connection, frn="0000000000")["found"])

    def test_geography_and_services(self) -> None:
        with self.db() as connection:
            areas = geography(connection, level="county")["areas"]
            self.assertEqual(areas[0]["county"], "KINGS")
            services = radio_services(connection)["services"]
            self.assertEqual({row["radio_service_code"] for row in services}, {"PW", "HA"})
            self.assertEqual(
                next(row for row in services if row["radio_service_code"] == "PW")["active"], 1
            )
            with self.assertRaises(ValueError):
                geography(connection, level="planet")


class AgentCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "test.sqlite3"
        build_fixture(self.path)

    def run_cli(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                main(["--database", str(self.path), *argv])
                code = EXIT_OK
            except SystemExit as exit_code:
                code = int(exit_code.code or 0)
        return code, out.getvalue(), err.getvalue()

    def test_capabilities_describes_every_command(self) -> None:
        code, out, _ = self.run_cli("capabilities")
        self.assertEqual(code, EXIT_OK)
        payload = json.loads(out)["data"]
        names = {command["name"] for command in payload["commands"]}
        self.assertEqual(
            names,
            {
                "band", "callsign", "expirations", "frequency", "geography",
                "license", "nearby", "organization", "schema", "search", "services",
                "sql", "status", "text",
            },
        )
        self.assertTrue(payload["read_only_queries"])
        self.assertIn("status", payload["guidance"])

    def test_json_envelope_carries_row_count(self) -> None:
        code, out, _ = self.run_cli("search", "--state", "NY")
        self.assertEqual(code, EXIT_OK)
        envelope = json.loads(out)
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["command"], "search")
        self.assertEqual(envelope["row_count"], 3)

    def test_empty_result_exits_one(self) -> None:
        code, out, _ = self.run_cli("callsign", "ZZZZZ")
        self.assertEqual(code, EXIT_EMPTY)
        self.assertEqual(json.loads(out)["row_count"], 0)

    def test_ndjson_streams_one_object_per_row(self) -> None:
        code, out, _ = self.run_cli("--format", "ndjson", "search", "--state", "NY")
        self.assertEqual(code, EXIT_OK)
        lines = [json.loads(line) for line in out.strip().splitlines()]
        self.assertEqual(len(lines), 3)
        self.assertEqual(lines[0]["callsign"], "TEST1")

    def test_csv_writes_a_header_row(self) -> None:
        code, out, _ = self.run_cli("--format", "csv", "services")
        self.assertEqual(code, EXIT_OK)
        self.assertTrue(out.splitlines()[0].startswith("radio_service_code,"))

    def test_nested_record_refuses_flat_formats(self) -> None:
        for output_format in ("csv", "ndjson"):
            code, _, err = self.run_cli("--format", output_format, "license", "--callsign", "TEST1")
            self.assertEqual(code, EXIT_ERROR)
            self.assertIn("nested record", json.loads(err)["error"])

    def test_errors_are_json_on_stderr(self) -> None:
        code, out, err = self.run_cli("band", "500", "400")
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(out, "")
        payload = json.loads(err)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["command"], "band")

    def test_writes_are_refused(self) -> None:
        code, _, err = self.run_cli("sql", "DELETE FROM licenses")
        self.assertEqual(code, EXIT_ERROR)
        self.assertIn("SELECT", json.loads(err)["error"])

    def test_missing_database_is_reported_not_created(self) -> None:
        missing = Path(self.directory.name) / "absent.sqlite3"
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as raised:
                main(["--database", str(missing), "status"])
        self.assertEqual(raised.exception.code, EXIT_ERROR)
        self.assertFalse(missing.exists())


if __name__ == "__main__":
    unittest.main()
