# Agent notes

Spectrum Wrangler is one read-only CLI over a local SQLite build of the FCC's
public radio licensing data (ULS). People and agents use the same commands.

## Using the tool

* Read [skills/spectrum-wrangler/SKILL.md](skills/spectrum-wrangler/SKILL.md)
  before querying — it carries the six dataset traps that produce confidently
  wrong answers (row-multiplying joins, `MM/DD/YYYY` text dates, the missing
  organization key, and the rest).
* `spectrum-wrangler capabilities` describes every command, argument, format,
  and exit code as JSON in one call.
* Run `spectrum-wrangler status` first and cite the FCC publication dates it
  reports. An empty result means "not in the loaded snapshots", never "no such
  authorization exists".
* Exit codes: `0` success, `1` understood but nothing matched, `2` bad request.
  Errors are JSON on stderr in machine formats.
* The database resolves from `--database`, then `$SPECTRUM_WRANGLER_DB`, then
  `./data/spectrum-wrangler.sqlite3` if already built, then the per-user data
  directory. If it is missing it must be built with `spectrum-wrangler refresh`
  (a 1.25 GB download; `--archive paging` is a small start).

## Working on the code

* Tests: `python3 -m unittest discover -s tests -v` — hermetic, no network,
  under a second. Also run `python3 -m compileall -q spectrum_wrangler tests`.
* Every query operation is declared once in `OPERATIONS` in
  `spectrum_wrangler/cli.py`; the argparse tree, help text, and `capabilities`
  manifest are generated from it. Never wire a subcommand by hand.
* There are no third-party runtime dependencies, deliberately. Read
  [docs/DEPENDENCIES.md](docs/DEPENDENCIES.md) before adding, updating, or even
  temporarily executing a package.
* `load.py` and `sample-fcc.csv` are legacy Python 2 artifacts kept for
  historical reference. Do not run, modernize, or delete them.
* [docs/QUERYING.md](docs/QUERYING.md) has the join semantics and worked
  investigations; [docs/RESEARCH-2026.md](docs/RESEARCH-2026.md) is an archived
  record of the 2026 source migration, not a live status page.
