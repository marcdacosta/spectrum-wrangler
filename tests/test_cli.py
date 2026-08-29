from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fixtures import build_fixture
from spectrum_wrangler import cli


class CliHarness(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "test.sqlite3"
        build_fixture(self.path)

    def run_cli(self, *argv: str, stdin: str = "") -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            with mock.patch("sys.stdin", io.StringIO(stdin)):
                try:
                    cli.main(["--database", str(self.path), *argv])
                    code = 0
                except SystemExit as exit_code:
                    code = int(exit_code.code or 0)
        return code, out.getvalue(), err.getvalue()

    def json_out(self, *argv: str, stdin: str = "") -> object:
        code, out, err = self.run_cli(*argv, stdin=stdin)
        self.assertEqual(code, 0, f"exited {code}: {err}")
        return json.loads(out)


class ParserTests(unittest.TestCase):
    def test_every_subcommand_is_wired_to_a_handler(self) -> None:
        parser = cli.parser()
        subparsers = next(
            action for action in parser._actions if hasattr(action, "choices") and action.choices
        )
        expected = {
            "init", "sources", "refresh", "callsign", "frequency", "nearby",
            "search", "status", "schema", "sql", "mcp",
        }
        self.assertEqual(set(subparsers.choices), expected)
        for name in expected:
            self.assertTrue(
                callable(subparsers.choices[name].get_default("func")),
                f"{name} has no handler",
            )

    def test_missing_subcommand_is_a_usage_error(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                cli.parser().parse_args([])
        self.assertEqual(raised.exception.code, 2)


class ReadCommandTests(CliHarness):
    def test_status_reports_loaded_provenance(self) -> None:
        payload = self.json_out("status")
        self.assertEqual(payload["sources"][0]["authority"], "FCC ULS")
        self.assertEqual(payload["normalized_counts"]["licenses"], 3)

    def test_callsign_is_case_insensitive(self) -> None:
        rows = self.json_out("callsign", "test1")
        self.assertEqual(rows[0]["callsign"], "TEST1")
        self.assertEqual(rows[0]["display_name"], "CITY OF TEST1")

    def test_callsign_with_no_match_prints_an_empty_list(self) -> None:
        self.assertEqual(self.json_out("callsign", "ZZZZZ"), [])

    def test_search_filters_combine(self) -> None:
        self.assertEqual(len(self.json_out("search", "--state", "NY")), 3)
        self.assertEqual(len(self.json_out("search", "--service", "PW")), 2)
        self.assertEqual(
            [row["callsign"] for row in self.json_out("search", "--service", "PW", "--status", "A")],
            ["TEST1"],
        )
        self.assertEqual(len(self.json_out("search", "--name", "CITY OF TEST2")), 1)

    def test_search_limit_is_bounded(self) -> None:
        self.assertEqual(len(self.json_out("search", "--limit", "1")), 1)
        code, _, err = self.run_cli("search", "--limit", "0")
        self.assertEqual(code, 2)
        self.assertIn("limit must be between", err)

    def test_frequency_tolerance_window(self) -> None:
        self.assertEqual(len(self.json_out("frequency", "462.5")), 1)
        self.assertEqual(self.json_out("frequency", "462.5")[0]["callsign"], "TEST1")
        self.assertEqual(self.json_out("frequency", "470")[0:], [])

    def test_frequency_rejects_negative_center(self) -> None:
        code, _, err = self.run_cli("frequency", "-5")
        self.assertEqual(code, 2)
        self.assertIn("non-negative", err)

    def test_nearby_returns_distance_and_respects_radius(self) -> None:
        rows = self.json_out("nearby", "40.7", "-74.0", "--radius-km", "1")
        self.assertEqual(rows[0]["callsign"], "TEST1")
        self.assertAlmostEqual(rows[0]["distance_km"], 0.0, places=3)
        self.assertEqual(self.json_out("nearby", "0", "0", "--radius-km", "1"), [])

    def test_nearby_rejects_impossible_coordinates(self) -> None:
        code, _, err = self.run_cli("nearby", "91", "0")
        self.assertEqual(code, 2)
        self.assertIn("out of range", err)

    def test_schema_lists_groups_then_describes_one_table(self) -> None:
        groups = self.json_out("schema")["groups"]
        self.assertIn("licenses", groups["normalized"])
        self.assertIn("raw_en", groups["raw"])
        columns = {column["name"] for column in self.json_out("schema", "licenses")["columns"]}
        self.assertIn("radio_service_code", columns)

    def test_schema_rejects_an_unknown_table(self) -> None:
        code, _, err = self.run_cli("schema", "no_such_table")
        self.assertEqual(code, 2)
        self.assertIn("Unknown table", err)


class SqlCommandTests(CliHarness):
    def test_select_returns_rows_and_metadata(self) -> None:
        payload = self.json_out("sql", "SELECT callsign FROM licenses ORDER BY callsign")
        self.assertEqual(payload["columns"], ["callsign"])
        self.assertEqual([row["callsign"] for row in payload["rows"]], ["TEST1", "TEST2", "TEST3"])
        self.assertEqual(payload["row_count"], 3)
        self.assertFalse(payload["truncated"])

    def test_query_is_read_from_stdin_when_omitted(self) -> None:
        payload = self.json_out("sql", stdin="SELECT count(*) AS n FROM licenses")
        self.assertEqual(payload["rows"][0]["n"], 3)

    def test_limit_marks_results_truncated(self) -> None:
        payload = self.json_out("sql", "SELECT callsign FROM licenses", "--limit", "2")
        self.assertEqual(payload["row_count"], 2)
        self.assertTrue(payload["truncated"])

    def test_write_statements_are_refused(self) -> None:
        for statement in (
            "DELETE FROM licenses",
            "UPDATE licenses SET callsign='X'",
            "DROP TABLE licenses",
            "INSERT INTO licenses(unique_system_id) VALUES(99)",
        ):
            code, _, err = self.run_cli("sql", statement)
            self.assertEqual(code, 2, statement)
            self.assertIn("SELECT", err)

    def test_a_comment_cannot_smuggle_a_write_past_the_prefix_check(self) -> None:
        code, _, err = self.run_cli("sql", "-- harmless\nDELETE FROM licenses")
        self.assertEqual(code, 2)
        self.assertIn("SELECT", err)

    def test_database_is_unchanged_after_refused_writes(self) -> None:
        self.run_cli("sql", "DELETE FROM licenses")
        self.assertEqual(self.json_out("status")["normalized_counts"]["licenses"], 3)

    def test_raw_contact_fields_are_denied_by_default(self) -> None:
        code, _, err = self.run_cli("sql", "SELECT phone FROM raw_en")
        self.assertEqual(code, 2)
        self.assertIn("prohibited", err.lower())

    def test_allow_sensitive_opts_a_local_operator_in(self) -> None:
        payload = self.json_out("sql", "SELECT phone FROM raw_en", "--allow-sensitive")
        self.assertEqual(payload["rows"][0]["phone"], "2125551212")

    def test_normalized_entity_name_is_never_gated(self) -> None:
        """The licensee's name is the point of the dataset, not contact data."""
        payload = self.json_out("sql", "SELECT display_name FROM entities LIMIT 1")
        self.assertEqual(payload["rows"][0]["display_name"], "CITY OF TEST1")


class WriteCommandTests(unittest.TestCase):
    def test_init_creates_an_indexed_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "new.sqlite3"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                with self.assertRaises(SystemExit) as raised:
                    cli.main(["--database", str(path), "init"])
            self.assertEqual(raised.exception.code, 0)
            self.assertTrue(json.loads(out.getvalue())["initialized"])
            self.assertTrue(path.exists())

    def test_sources_compares_the_live_directory_with_the_reviewed_set(self) -> None:
        out = io.StringIO()
        with mock.patch.object(
            cli, "list_official_archives", return_value=["l_amat.zip", "l_brandnew.zip"]
        ):
            with contextlib.redirect_stdout(out):
                with self.assertRaises(SystemExit):
                    cli.main(["sources"])
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["new_or_unreviewed"], ["l_brandnew.zip"])
        self.assertIn("l_paging.zip", payload["missing"])

    def test_refresh_refuses_an_archive_the_fcc_no_longer_lists(self) -> None:
        err = io.StringIO()
        with mock.patch.object(cli, "list_official_archives", return_value=["l_amat.zip"]):
            with contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as raised:
                    cli.main(["refresh", "--archive", "paging"])
        self.assertEqual(raised.exception.code, 2)
        self.assertIn("does not currently list", err.getvalue())

    def test_refresh_rejects_an_unknown_archive_alias(self) -> None:
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as raised:
                cli.main(["refresh", "--archive", "not-an-archive"])
        self.assertEqual(raised.exception.code, 2)

    def test_mcp_subcommand_serves_the_named_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.sqlite3"
            build_fixture(path)
            with mock.patch("spectrum_wrangler.mcp_server.serve") as serve:
                with self.assertRaises(SystemExit):
                    cli.main(["--database", str(path), "mcp", "--allow-sensitive"])
            serve.assert_called_once()
            self.assertEqual(serve.call_args.kwargs["allow_sensitive"], True)
            self.assertEqual(serve.call_args.args[0], path)


class OutputContractTests(CliHarness):
    def test_results_are_json_on_stdout_and_errors_are_plain_on_stderr(self) -> None:
        code, out, err = self.run_cli("callsign", "TEST1")
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        json.loads(out)

        code, out, err = self.run_cli("schema", "nope")
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertTrue(err.startswith("error: "))

    def test_json_is_stable_for_diffing(self) -> None:
        first = self.run_cli("search", "--state", "NY")[1]
        second = self.run_cli("search", "--state", "NY")[1]
        self.assertEqual(first, second)
        self.assertIn("\n  ", first, "output should be indented")

    def test_a_missing_database_fails_without_creating_one(self) -> None:
        missing = Path(self.directory.name) / "absent.sqlite3"
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                cli.main(["--database", str(missing), "status"])
        self.assertEqual(raised.exception.code, 2)
        self.assertFalse(missing.exists())


if __name__ == "__main__":
    unittest.main()
