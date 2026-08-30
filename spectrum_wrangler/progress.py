"""Terminal reporting for the long-running commands.

Presentation lives here so the ingest code stays silent and testable. On an
interactive terminal, steps draw in place with a spinner and finish as checked,
timed lines; piped, every step is one plain line, so logs and agents see events
without escape codes or carriage returns. NO_COLOR is respected, and a stream
whose encoding cannot draw the glyphs gets ASCII ones.
"""

from __future__ import annotations

import os
import shutil
import sys
import time
from typing import TextIO

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def human_bytes(count: float) -> str:
    if count >= 1e9:
        return f"{count / 1e9:.2f} GB"
    if count >= 1e6:
        return f"{count / 1e6:.1f} MB"
    if count >= 1e3:
        return f"{count / 1e3:.0f} KB"
    return f"{count:.0f} B"


def human_duration(seconds: float) -> str:
    whole = int(seconds)
    if whole >= 3600:
        return f"{whole // 3600}h {whole % 3600 // 60:02d}m"
    if whole >= 60:
        return f"{whole // 60}m {whole % 60:02d}s"
    return f"{whole}s"


def human_count(count: float) -> str:
    return f"{count:,.0f}"


def human_rate(count: float, seconds: float, unit: str) -> str:
    if seconds <= 0:
        return ""
    rate = count / seconds
    if unit == "B":
        return f"{human_bytes(rate)}/s"
    if rate >= 1e6:
        return f"{rate / 1e6:.1f}M {unit}/s"
    if rate >= 1e3:
        return f"{rate / 1e3:.0f}k {unit}/s"
    return f"{rate:.0f} {unit}/s"


class Reporter:
    """Stage and step progress that is safe to use piped."""

    def __init__(self, stream: TextIO | None = None):
        self.stream = stream if stream is not None else sys.stderr
        self.live = bool(getattr(self.stream, "isatty", lambda: False)())
        self.color = self.live and "NO_COLOR" not in os.environ
        if self.color and os.name == "nt":
            os.system("")  # switches the legacy console into VT mode
        encoding = getattr(self.stream, "encoding", None) or "ascii"
        try:
            (SPINNER + "✓").encode(encoding)
            self.check, self.frames = "✓", SPINNER
        except (UnicodeEncodeError, LookupError):
            self.check, self.frames = "*", "|/-\\"
        self._drawn = 0
        self._frame = 0
        self._last_draw = 0.0
        self._verb = ""
        self._t0 = 0.0

    def _sgr(self, text: str, code: str) -> str:
        return f"\x1b[{code}m{text}\x1b[0m" if self.color else text

    def bold(self, text: str) -> str:
        return self._sgr(text, "1")

    def dim(self, text: str) -> str:
        return self._sgr(text, "2")

    def green(self, text: str) -> str:
        return self._sgr(text, "32")

    def _clear(self) -> None:
        if self._drawn:
            self.stream.write("\r" + " " * self._drawn + "\r")
            self._drawn = 0

    def say(self, text: str = "") -> None:
        self._clear()
        self.stream.write(text + "\n")
        self.stream.flush()

    def stage(self, index: int, total: int, name: str) -> None:
        width = len(str(total))
        self.say(self.bold(f"[{index:>{width}}/{total}] {name}"))

    def begin(self, verb: str) -> None:
        """Start a timed step; piped, this is the one 'started' line."""
        self._verb = verb
        self._t0 = time.monotonic()
        self._last_draw = 0.0
        if not self.live:
            self.say(f"  {verb}")

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._t0 if self._t0 else 0.0

    def update(self, detail: str) -> None:
        """Redraw the live step line; piped, updates are silent."""
        if not self.live:
            return
        now = time.monotonic()
        if now - self._last_draw < 0.1:
            return
        self._last_draw = now
        self._frame = (self._frame + 1) % len(self.frames)
        line = f"  {self.frames[self._frame]} {self._verb}  {detail}"
        columns = shutil.get_terminal_size().columns
        line = line[: max(columns - 1, 10)]
        padding = " " * max(self._drawn - len(line), 0)
        self.stream.write("\r" + line + padding + "\r" + line)
        self._drawn = len(line)
        self.stream.flush()

    def done(self, verb: str, detail: str = "") -> None:
        """Finish the step as a checked line, timed when it took a second or more."""
        elapsed = self.elapsed
        suffix = f" in {human_duration(elapsed)}" if elapsed >= 1 else ""
        body = f"  {self.green(self.check)} {verb}  {detail}".rstrip() + suffix
        self.say(body)
        self._verb, self._t0 = "", 0.0
