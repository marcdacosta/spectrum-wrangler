# Dependency policy

Spectrum Wrangler 0.3 has no third-party runtime dependencies. Its build backend
is pinned to `setuptools==80.9.0`; PyPI registry metadata reports its release
files were uploaded on 2025-05-27, well outside the repository's 14-day minimum
age policy as checked on 2026-08-28.

Adding, updating, installing, or temporarily executing a package requires first
recording the exact version and official registry release timestamp. Existing
lockfile installs are acceptable only when they do not change resolved versions.
