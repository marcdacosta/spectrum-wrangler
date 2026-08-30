"""The agent profile of the CLI.

Identical commands to `spectrum-wrangler`; only the output defaults differ. An
agent wants a machine-readable envelope whether or not it happens to be
attached to a terminal, so this entry point pins the format to JSON rather than
guessing from isatty. Keeping it a profile instead of a second program is
deliberate: two argparse trees is exactly how commands drift apart.
"""

from __future__ import annotations

from .cli import EXIT_EMPTY, EXIT_ERROR, EXIT_OK, main as _main

__all__ = ["EXIT_EMPTY", "EXIT_ERROR", "EXIT_OK", "main"]


def main(argv: list[str] | None = None) -> None:
    _main(argv, default_format="json")


if __name__ == "__main__":
    main()
