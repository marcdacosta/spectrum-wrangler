# Querying the data

This is the practical guide: which table answers which question, how the
records actually join, and the four things about FCC data that will produce a
confidently wrong answer if you do not know them.

Everything here runs against the database `spectrum-wrangler refresh` builds.
Examples use the `sql` command; the same queries work through the agent CLI and
the MCP `query_spectrum_sql` tool.

## Start with what you have

    python3 -m spectrum_wrangler status

This reports the issuing authority, each archive's FCC publication date, its
SHA-256, and row counts. Every conclusion you draw is a statement about *those
snapshots on that date*, so read it first and cite it. An empty result means
"not in the loaded snapshots", never "no such FCC authorization exists".

## Which table answers which question

| Question | Table | Notes |
|---|---|---|
| Who holds this call sign? | `licenses` + `entities` | Filter `entities.entity_type='L'` for the licensee |
| Is it still valid? | `licenses.license_status` | `A` active, `E` expired, `C` cancelled, `T` terminated |
| Where does it transmit from? | `locations` | WGS84 `latitude`/`longitude`, plus `county`/`state` |
| What's near this coordinate? | `location_rtree` → `locations` | Bounding box first, then a real distance filter |
| On what frequency? | `frequencies` | `frequency_assigned_mhz`, `power_output_w`, `erp_w` |
| With what modulation? | `emissions` | `emission_code` |
| On what antenna? | `antennas` | Height, gain, azimuth, beamwidth |
| Ham licence details? | `amateur` | Operator class, trustee, previous call sign |
| Free-text search | `license_fts` | FTS5 over call sign, licensee, service, state |
| Anything else | `raw_*` | Every published field, 89 tables, verbatim |

The structured CLI and MCP commands wrap the common shapes of these. Reach for
SQL when your question is not one of them.

## How records join

Everything hangs off `unique_system_id` (`unique_system_identifier` in the raw
tables). That is the durable key for a licence across all its related records:

    licenses ─┬─ entities      (licensee, contact, owner)
              ├─ locations     (+ location_number)
              ├─ antennas      (+ location_number, antenna_number)
              ├─ frequencies   (+ location_number, antenna_number, frequency_number)
              ├─ emissions     (+ location_number, antenna_number, frequency_number)
              └─ amateur

The indented numbers matter. A frequency belongs to a specific antenna at a
specific location, not to the licence in general. Joining on
`unique_system_id` alone throws that structure away — see the next section.

To skip the joins entirely for a single licence, use the assembled record:

    python3 -m spectrum_wrangler.agent license --callsign W1AW

## Four things that will bite you

### 1. Joining on the licence alone multiplies rows

Call sign `WQJX500` has 2 locations, 960 frequency assignments, and 960
emissions. Join all four tables on `unique_system_id` and you get:

    SELECT count(*) FROM licenses l
    JOIN locations   lo USING(unique_system_id)
    JOIN frequencies f  USING(unique_system_id)
    JOIN emissions   em USING(unique_system_id)
    WHERE l.callsign='WQJX500'
    -- 1843200

That is 2 × 960 × 960, and every aggregate over it is wrong. This is exactly
the row multiplication the old denormalized License View CSV suffered from.
Either carry the full key through the join, or aggregate each relation
separately:

    SELECT (SELECT count(*) FROM locations   WHERE unique_system_id=l.unique_system_id) AS locations,
           (SELECT count(*) FROM frequencies WHERE unique_system_id=l.unique_system_id) AS frequencies
    FROM licenses l WHERE l.callsign='WQJX500'
    -- 2, 960

### 2. Dates are MM/DD/YYYY text

The FCC publishes `03/12/2026`, and the importer preserves it. Sorting or
comparing that lexically sorts by month. Convert first:

    substr(expired_date,7,4) || substr(expired_date,1,2) || substr(expired_date,4,2)

The built-in commands already do this — `expirations` takes ordinary
`YYYY-MM-DD` input and orders correctly. Only hand-written SQL needs care.

### 3. Everything raw is text

Raw values are stored exactly as published so identifiers keep their leading
zeroes, and precision and date formats are not silently reinterpreted. Cast in
the query when you need arithmetic. The normalized tables are typed, which is
the main reason to prefer them.

### 4. The codes are not self-explanatory

`entity_type` is `L` for the licensee, `CL` for contact, `O` for owner. Filter
to `L` or you will count the same licence more than once — there are 5.2M `L`
rows against 1.3M `CL`. Status codes are `A`/`E`/`C`/`T`/`P`. Radio service
codes are two letters (`HA` amateur, `PW` public safety, `IG` industrial) and
the ULS bulk archives do not ship their descriptions, so `list_radio_services`
reports observed codes and counts rather than inventing definitions. Expect
occasional dirty values — there is exactly one lowercase `c` status in the
August 2026 snapshot.

## Worked example: who transmits over a neighbourhood

Active licences with transmitter sites in Lower Manhattan, and the frequency
range each one operates across:

    SELECT e.display_name, l.radio_service_code,
           count(DISTINCT f.frequency_assigned_mhz) AS distinct_freqs,
           round(min(f.frequency_assigned_mhz),4) AS lowest_mhz,
           round(max(f.frequency_assigned_mhz),4) AS highest_mhz
    FROM locations lo
    JOIN licenses l USING(unique_system_id)
    LEFT JOIN entities e
           ON e.unique_system_id=l.unique_system_id AND e.entity_type='L'
    JOIN frequencies f ON f.unique_system_id=l.unique_system_id
    WHERE lo.latitude  BETWEEN 40.700 AND 40.730
      AND lo.longitude BETWEEN -74.020 AND -73.990
      AND l.license_status='A'
    GROUP BY 1,2 ORDER BY distinct_freqs DESC LIMIT 5

    NEW YORK CITY POLICE DEPARTMENT   PW   145   155.37 – 2473.0 MHz
    Redwave Wireless                  CF    94   10715.0 – 23575.0
    NW TECHNOLOGIES, LLC              MG    92   10755.0 – 23575.0
    NEW YORK CITY POLICE DEPARTMENT   MW    59   5974.85 – 22375.0
    CITY OF NEW YORK                  YW    58   482.0437 – 487.6375

Note the `DISTINCT` and the `entity_type='L'` filter — both are load-bearing.
For a plain radius search without SQL, `nearby` uses the RTree index and
returns true great-circle distances.

## Worked example: what is expiring

    python3 -m spectrum_wrangler.agent --format csv \
      expirations --start 2026-10-01 --end 2026-12-31 --state NY --service PW

Renewal windows are a reliable way into a story: a licence lapsing is a public
fact with a date attached. `--status` defaults to `A`, so you are asking about
authorizations that are live now and will not be soon.

## Raw versus normalized

The normalized tables are typed, indexed, and cover the common questions. The
89 `raw_*` tables hold every field the FCC publishes, verbatim, with the
original ULS names — `raw_hd` header, `raw_en` entities, `raw_lo` locations,
`raw_an` antennas, `raw_fr` frequencies, `raw_em` emissions.

    python3 -m spectrum_wrangler schema            # what exists
    python3 -m spectrum_wrangler schema raw_fr     # exact columns

Use raw when you need a field normalization does not carry, or when you are
auditing against the source. `uls_raw_catalog` maps record codes to their
column lists, and undocumented trailing fields are preserved in
`extra_fields_json` rather than dropped.

Raw contact, address, ZIP, and FRN columns are denied to SQL unless the local
operator passes `--allow-sensitive`. Licensee names in `entities.display_name`
are never gated — knowing who holds a licence is the point of the dataset.
