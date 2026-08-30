from __future__ import annotations

import io
import os
import sqlite3
import unittest
from unittest import mock

from spectrum_wrangler.progress import (
    Reporter,
    human_bytes,
    human_count,
    human_duration,
    human_rate,
)
from spectrum_wrangler.uls import _batch_insert


class FakeTty(io.StringIO):
    def isatty(self) -> bool:
        return True


class FormattingTests(unittest.TestCase):
    def test_bytes_scale_like_the_readme(self) -> None:
        self.assertEqual(human_bytes(6_530_758), "6.5 MB")
        self.assertEqual(human_bytes(1_250_000_000), "1.25 GB")
        self.assertEqual(human_bytes(512), "512 B")

    def test_durations_read_naturally(self) -> None:
        self.assertEqual(human_duration(47), "47s")
        self.assertEqual(human_duration(78), "1m 18s")
        self.assertEqual(human_duration(3852), "1h 04m")

    def test_rates_pick_a_sensible_unit(self) -> None:
        self.assertEqual(human_rate(198_000_000, 47, "B"), "4.2 MB/s")
        self.assertEqual(human_rate(135_000, 1, "rows"), "135k rows/s")
        self.assertEqual(human_rate(10, 0, "rows"), "")

    def test_counts_group_thousands(self) -> None:
        self.assertEqual(human_count(10_548_712), "10,548,712")


class PipedReporterTests(unittest.TestCase):
    """Piped output is plain lines: no escape codes, no carriage returns."""

    def test_steps_are_one_started_and_one_finished_line(self) -> None:
        stream = io.StringIO()
        reporter = Reporter(stream)
        reporter.stage(1, 14, "l_amat.zip")
        reporter.begin("downloading")
        reporter.update("198.0 MB")
        reporter.done("downloaded", "198.0 MB")
        text = stream.getvalue()
        self.assertIn("[ 1/14] l_amat.zip", text)
        self.assertIn("  downloading\n", text)
        self.assertIn("downloaded  198.0 MB", text)
        self.assertNotIn("\x1b", text)
        self.assertNotIn("\r", text)

    def test_updates_are_silent_when_piped(self) -> None:
        stream = io.StringIO()
        reporter = Reporter(stream)
        reporter.begin("importing")
        stream.truncate(0), stream.seek(0)
        reporter.update("1,000 rows")
        self.assertEqual(stream.getvalue(), "")


class LiveReporterTests(unittest.TestCase):
    def test_updates_redraw_in_place(self) -> None:
        stream = FakeTty()
        with mock.patch.dict(os.environ, {"NO_COLOR": "1"}):
            reporter = Reporter(stream)
            reporter.begin("downloading")
            reporter.update("42.0 MB")
        text = stream.getvalue()
        self.assertIn("\r", text)
        self.assertIn("downloading  42.0 MB", text)
        self.assertNotIn("\x1b", text, "NO_COLOR must disable escape codes")

    def test_color_is_used_on_a_terminal_unless_disabled(self) -> None:
        stream = FakeTty()
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NO_COLOR", None)
            reporter = Reporter(stream)
            reporter.say(reporter.bold("done"))
        self.assertIn("\x1b[1m", stream.getvalue())


class BatchProgressTests(unittest.TestCase):
    def test_on_batch_reports_cumulative_counts_including_the_final_flush(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE t(x)")
        ticks: list[int] = []
        count = _batch_insert(
            connection, "INSERT INTO t VALUES(?)", ((i,) for i in range(5)),
            batch_size=2, on_batch=ticks.append,
        )
        self.assertEqual(count, 5)
        self.assertEqual(ticks, [2, 4, 5])


if __name__ == "__main__":
    unittest.main()
