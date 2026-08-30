---
name: spectrum-wrangler
description: Query FCC radio spectrum licence data — who is licensed to transmit, where, on what frequency, and with how much power. Use when asked about radio licences, call signs, transmitter sites near a place, frequency assignments, spectrum holdings of an organization, antenna locations, or FCC ULS data. Also use for questions about what is broadcasting from a building, tower, or neighbourhood.
---

# Spectrum Wrangler

A local database of the FCC's public licensing data: every US radio transmitter
licence above certain power levels — who holds it, where it transmits from, on
what frequency, with what antenna.

Run `spectrum-wrangler <command>`, or `python3 -m spectrum_wrangler <command>`
from the repository. `--format json|ndjson|csv` for machine output; it already
defaults to JSON when piped.

## Always start here

```sh
spectrum-wrangler status
```

This reports which FCC snapshots are loaded and when the FCC published them.
**Every answer you give is a statement about those snapshots on that date, so
cite the date.** The data is rebuilt from weekly files, so it is up to a week
behind, and an empty result means *"not in the loaded snapshots"* — never *"no
such licence exists"*.

If the database is missing, say so rather than guessing: it must be built with
`spectrum-wrangler refresh`, which downloads 1.25 GB and takes about 15 minutes.

## Finding things

```sh
spectrum-wrangler capabilities          # every command and argument, as JSON
```

Investigation usually starts one of four ways, then follows what it finds.

**Near a place** — the radius is in kilometres, so use small values to isolate a
single structure:

```sh
spectrum-wrangler nearby 40.7128 -74.0060 --radius-km 5
spectrum-wrangler nearby 40.748444 -73.985694 --radius-km 0.1   # one building
```

A 100 m radius is the "who else is on this tower?" pivot. The Empire State
Building returns ~250 licensees this way. You need coordinates — the data has no
place-name lookup, though `locations` carries `county` and `state`.

**Digging into one licence** — this assembles the licensee, locations, antennas,
frequencies, and emissions in one call, so prefer it over six joins:

```sh
spectrum-wrangler license --callsign W1AW
```

**By type, frequency, or date:**

```sh
spectrum-wrangler frequency 931 --tolerance-khz 500
spectrum-wrangler band 462 468 --group-by licensee
spectrum-wrangler services                       # what the two-letter codes are
spectrum-wrangler expirations --start 2026-10-01 --end 2026-12-31 --state NY
```

**By organization** — read the caveats it returns, and see the warning below:

```sh
spectrum-wrangler organization --name "NEW YORK CITY POLICE"
spectrum-wrangler search --name "CITY OF NEW YORK" --state NY --status A
spectrum-wrangler text 'fire AND department'     # full-text
```

Anything else: `spectrum-wrangler sql "SELECT ..."` — one read-only
SELECT/WITH/EXPLAIN, capped at 1,000 rows. Run `spectrum-wrangler schema` and
`schema raw_fr` first to see what exists.

## Six things that will make you confidently wrong

**1. Counting an organization.** There is no reliable organization key. Names are
free text typed differently on every filing; FRN (the FCC Registration Number)
is exact but covers ~80% of licensees (contact and owner records never carry
one), and it is issued per filing office, so one body holds several. The NYPD's
licensee records carry 7 different FRNs. `organization` returns explicit
caveats — repeat them. Give counts as estimates with a stated method, never as
facts.

**2. Joining on `unique_system_id` alone.** A licence has many locations, each
with many frequencies. Joining `licenses`, `locations`, `frequencies`, and
`emissions` on the licence id alone turned one call sign into **1,843,200 rows**
describing 2 locations and 960 frequencies. Aggregate each relation separately,
or carry the full key (`location_number`, `antenna_number`, `frequency_number`).

**3. Dates are `MM/DD/YYYY` text.** Sorting them lexically sorts by month. The
built-in commands handle this; hand-written SQL must convert:
`substr(d,7,4)||substr(d,1,2)||substr(d,4,2)`.

**4. Forgetting `entity_type='L'`.** The `entities` table also holds contacts
(`CL`) and owners (`O`). Without the filter you count licences more than once.

**5. Assuming broadcast is here.** Full-power AM/FM/TV station licensing lives in
the FCC's separate LMS system and is **not** in this database. ULS carries
broadcast *auxiliary* only. If asked about a radio or TV station, say so.

**6. Treating `licenses` rows as active.** Only about half are. Filter
`license_status='A'`; `E` is expired, `C` cancelled, `T` terminated.

## What this data contains

All of it is FCC public record and every field is queryable — there is no
gating, because the database is a local file anyone can open directly. Writes
are refused by SQLite itself.

Be deliberate in bulk, though. Entity records carry licensee contact details,
and the amateur archive alone is ~1.7 million individual people with names and
mailing addresses. A single lookup is not the same act as republishing an
extract of private individuals.

## Exit codes

`0` success · `1` understood, nothing matched · `2` bad request or database
error. Errors are JSON on stderr in machine formats.

## Going deeper

`docs/QUERYING.md` in the repository has the full table-routing map, the join
diagram, and worked investigations.
