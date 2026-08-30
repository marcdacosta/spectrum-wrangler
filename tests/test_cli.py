from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
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

    def envelope(self, *argv: str, stdin: str = "") -> dict:
        """Run a command and return the whole JSON envelope."""
        code, out, err = self.run_cli(*argv, stdin=stdin)
        self.assertIn(code, (0, 1), f"exited {code}: {err}")
        return json.loads(out)

    def json_out(self, *argv: str, stdin: str = "") -> object:
        """Run a command and return just its payload."""
        return self.envelope(*argv, stdin=stdin)["data"]


class ParserTests(unittest.TestCase):
    def test_every_subcommand_is_wired_to_a_handler(self) -> None:
        parser = cli.parser()
        subparsers = next(
            action for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        expected = {
            "init", "sources", "refresh", "capabilities", "callsign", "frequency",
            "nearby", "search", "organization", "status", "schema", "sql",
            "license", "text", "band", "geography", "services", "expirations",
        }
        self.assertEqual(set(subparsers.choices), expected)
        for name in expected:
            sub = subparsers.choices[name]
            handler = sub.get_default("func") or sub.get_default("operation")
            self.assertIsNotNone(handler, f"{name} has no handler")

    def test_every_operation_is_declared_once(self) -> None:
        """The parser, capabilities, and help all come from OPERATIONS."""
        declared = {operation.name for operation in cli.OPERATIONS}
        manifest = {entry["name"] for entry in cli.capabilities_manifest()["commands"]}
        self.assertEqual(declared, manifest)

    def test_every_parameter_documents_itself(self) -> None:
        for operation in cli.OPERATIONS:
            for parameter in operation.params:
                self.assertTrue(
                    parameter.help,
                    f"{operation.name}.{parameter.name} has no help text",
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
        self.assertEqual(rows[0]["display_name"], "CITY OF TEST")

    def test_callsign_with_no_match_prints_an_empty_list(self) -> None:
        self.assertEqual(self.json_out("callsign", "ZZZZZ"), [])
        self.assertEqual(self.run_cli("callsign", "ZZZZZ")[0], 1)

    def test_search_filters_combine(self) -> None:
        self.assertEqual(len(self.json_out("search", "--state", "NY")), 3)
        self.assertEqual(len(self.json_out("search", "--service", "PW")), 2)
        self.assertEqual(
            [row["callsign"] for row in self.json_out("search", "--service", "PW", "--status", "A")],
            ["TEST1"],
        )
        self.assertEqual(len(self.json_out("search", "--name", "SOMEONE ELSE")), 1)

    def test_search_limit_is_bounded(self) -> None:
        self.assertEqual(len(self.json_out("search", "--limit", "1")), 1)
        code, _, err = self.run_cli("search", "--limit", "0")
        self.assertEqual(code, 2)
        self.assertIn("limit must be between", err)

    def test_frequency_tolerance_window(self) -> None:
        self.assertEqual(len(self.json_out("frequency", "462.5")), 1)
        self.assertEqual(self.json_out("frequency", "462.5")[0]["callsign"], "TEST1")
        self.assertEqual(self.json_out("frequency", "470"), [])

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

    def test_published_contact_fields_are_queryable(self) -> None:
        """FCC record is public and the database is a local file; there is no gate."""
        payload = self.json_out("sql", "SELECT phone, email, frn FROM raw_en")
        self.assertEqual(payload["rows"][0]["phone"], "2125551212")
        self.assertEqual(payload["rows"][0]["frn"], "0001234567")

    def test_normalized_entity_name_is_queryable(self) -> None:
        payload = self.json_out("sql", "SELECT display_name FROM entities LIMIT 1")
        self.assertEqual(payload["rows"][0]["display_name"], "CITY OF TEST")


class WriteCommandTests(unittest.TestCase):
    def run_init(self, *argv: str) -> tuple[int, dict, mock.Mock]:
        """Run init with the download path mocked out; return code, payload, mock."""
        out = io.StringIO()
        with mock.patch.object(cli, "_load_archives", return_value=[]) as load:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    cli.main(list(argv))
        return int(raised.exception.code or 0), json.loads(out.getvalue()), load

    def test_init_creates_an_indexed_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "new.sqlite3"
            code, payload, _ = self.run_init("--database", str(path), "init")
            self.assertEqual(code, 0)
            self.assertTrue(payload["initialized"])
            self.assertTrue(path.exists())

    def test_init_loads_the_starter_archive_on_an_empty_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "new.sqlite3"
            code, payload, load = self.run_init("--database", str(path), "init")
            self.assertEqual(code, 0)
            self.assertEqual(load.call_args.args[3], ["l_paging.zip"])
            self.assertIn("refresh", payload["next"])

    def test_init_loads_a_requested_archive_instead_of_the_starter(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "new.sqlite3"
            _, _, load = self.run_init(
                "--database", str(path), "init", "--archive", "amateur")
            self.assertEqual(load.call_args.args[3], ["l_amat.zip"])

    def test_init_leaves_an_already_loaded_database_alone(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "loaded.sqlite3"
            build_fixture(path)
            code, payload, load = self.run_init("--database", str(path), "init")
            self.assertEqual(code, 0)
            load.assert_not_called()
            self.assertEqual(payload["active_sources"], 1)
            self.assertIn("refresh", payload["hint"])

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

class OutputContractTests(CliHarness):
    def test_results_are_json_on_stdout_and_errors_are_json_on_stderr(self) -> None:
        code, out, err = self.run_cli("callsign", "TEST1")
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertTrue(json.loads(out)["ok"])

        code, out, err = self.run_cli("schema", "nope")
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertFalse(json.loads(err)["ok"])

    def test_human_errors_are_plain_text(self) -> None:
        code, _, err = self.run_cli("--format", "table", "schema", "nope")
        self.assertEqual(code, 2)
        self.assertTrue(err.startswith("error: "))

    def test_table_format_renders_a_header(self) -> None:
        code, out, _ = self.run_cli("--format", "table", "search", "--state", "NY")
        self.assertEqual(code, 0)
        self.assertIn("CALLSIGN", out)
        self.assertIn("---", out)

    def test_table_format_says_so_when_nothing_matched(self) -> None:
        code, out, _ = self.run_cli("--format", "table", "callsign", "ZZZZZ")
        self.assertEqual(code, 1)
        self.assertIn("no matching records", out)

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

    def test_a_missing_database_error_says_how_to_point_at_one(self) -> None:
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit):
                cli.main(["--format", "json", "--database", "/absent/nowhere.sqlite3",
                          "status"])
        message = json.loads(err.getvalue())["error"]
        self.assertIn("init", message)
        self.assertIn("refresh", message)
        self.assertIn(cli.DB_ENV_VAR, message)


class DatabaseResolutionTests(unittest.TestCase):
    """--database wins, then $SPECTRUM_WRANGLER_DB, then ./data, then the user dir."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.previous_cwd = Path.cwd()
        self.addCleanup(os.chdir, self.previous_cwd)
        os.chdir(self.directory.name)

    def test_environment_variable_wins_over_a_checkout_database(self) -> None:
        (Path("data")).mkdir()
        Path(cli.REPOSITORY_DB).touch()
        with mock.patch.dict(os.environ, {cli.DB_ENV_VAR: "/elsewhere/db.sqlite3"}):
            self.assertEqual(cli.default_database(), Path("/elsewhere/db.sqlite3"))

    def test_a_checkout_database_is_used_when_present(self) -> None:
        (Path("data")).mkdir()
        Path(cli.REPOSITORY_DB).touch()
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(cli.DB_ENV_VAR, None)
            self.assertEqual(cli.default_database(), cli.REPOSITORY_DB)

    def test_the_per_user_directory_is_the_installed_default(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(cli.DB_ENV_VAR, None)
            resolved = cli.default_database()
        self.assertEqual(resolved, cli.data_home() / cli.DB_FILENAME)
        self.assertTrue(resolved.is_absolute())

    def test_the_environment_variable_reaches_a_real_query(self) -> None:
        database = Path(self.directory.name) / "resolved.sqlite3"
        build_fixture(database)
        out = io.StringIO()
        with mock.patch.dict(os.environ, {cli.DB_ENV_VAR: str(database)}):
            with contextlib.redirect_stdout(out):
                try:
                    cli.main(["--format", "json", "status"])
                except SystemExit as raised:
                    self.assertEqual(int(raised.code or 0), 0)
        self.assertTrue(json.loads(out.getvalue())["ok"])


class HelpTextTests(unittest.TestCase):
    def test_flag_defaults_appear_in_generated_help(self) -> None:
        parser = cli.parser()
        subparsers = next(
            action for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        nearby_help = subparsers.choices["nearby"].format_help()
        self.assertIn("default: 10.0", nearby_help)
        self.assertIn("default: 100", nearby_help)
        sql_help = subparsers.choices["sql"].format_help()
        self.assertIn("default: 5000", sql_help)

    def test_the_database_flag_documents_its_resolution(self) -> None:
        root_help = cli.parser().format_help()
        self.assertIn(cli.DB_ENV_VAR, root_help)


class GlobalFlagPlacementTests(CliHarness):
    def test_format_is_accepted_after_the_subcommand(self) -> None:
        code, out, _ = self.run_cli("callsign", "TEST1", "--format", "ndjson")
        self.assertEqual(code, 0)
        row = json.loads(out.splitlines()[0])
        self.assertEqual(row["callsign"], "TEST1")

    def test_database_is_accepted_after_the_subcommand(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                cli.main(["callsign", "TEST1", "--database", str(self.path),
                          "--format", "json"])
            except SystemExit as raised:
                self.assertEqual(int(raised.code or 0), 0)
        self.assertTrue(json.loads(out.getvalue())["ok"])

    def test_a_trailing_flag_wins_over_a_leading_one(self) -> None:
        args = cli.parser().parse_args(
            ["--database", "leading.sqlite3", "callsign", "X",
             "--database", "trailing.sqlite3", "--format", "csv"])
        self.assertEqual(args.database, Path("trailing.sqlite3"))
        self.assertEqual(args.output_format, "csv")

    def test_an_absent_trailing_flag_keeps_the_leading_value(self) -> None:
        args = cli.parser().parse_args(["--format", "ndjson", "callsign", "X"])
        self.assertEqual(args.output_format, "ndjson")
        self.assertIsNone(args.database)


if __name__ == "__main__":
    unittest.main()
