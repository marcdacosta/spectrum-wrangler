# Dependency policy

Spectrum Wrangler 0.3 has no third-party runtime dependencies. Its build backend
is pinned to `setuptools==80.9.0`; PyPI registry metadata reports its release
files were uploaded on 2025-05-27, well outside the repository's 14-day minimum
age policy as checked on 2026-08-28.

Adding, updating, installing, or temporarily executing a package requires first
recording the exact version and official registry release timestamp. Existing
lockfile installs are acceptable only when they do not change resolved versions.

## Recorded release tooling

The release workflow builds distributions with `build==1.5.0`; PyPI registry
metadata reports its release files were uploaded on 2026-04-30, outside the
14-day minimum age as checked on 2026-08-30. At that check `build==1.5.1` was
yanked upstream and `build==1.6.0` was three days old, inside the window, so
neither was adopted.

GitHub Actions in CI are pinned by major version tag (`actions/checkout@v5`,
`actions/setup-python@v5`, `actions/upload-artifact@v4`,
`actions/download-artifact@v4`, `pypa/gh-action-pypi-publish@release/v1`).
They run only in CI and never ship in a release artifact.
