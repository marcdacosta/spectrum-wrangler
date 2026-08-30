"""A small, fully-related database shared by the CLI and agent test suites.

Three licenses is the minimum that exercises what matters: one with every
relation attached, one in a different radio service, and one expired. Dates
are written in the FCC's MM/DD/YYYY form so ordering bugs surface here rather
than against 5 million live rows.
"""

from __future__ import annotations

from pathlib import Path

from spectrum_wrangler.db import connect, initialize


# TEST1 and TEST3 share one FRN under two different spellings, which is the
# situation that makes display_name matching undercount a real organization.
LICENSES = [
    # usi, callsign, status, service, grant, expired, last_action, name, frn
    (1, "TEST1", "A", "PW", "01/02/2020", "09/15/2026", "03/04/2026", "CITY OF TEST", "0001234567"),
    (2, "TEST2", "A", "HA", "05/06/2019", "01/10/2031", "12/31/2025", "SOMEONE ELSE", "0009999999"),
    (3, "TEST3", "E", "PW", "07/08/2015", "02/02/2020", "02/02/2020", "City of Test", "0001234567"),
]


def build_fixture(path: Path) -> None:
    with connect(path) as connection:
        initialize(connection)
        connection.execute(
            "INSERT INTO sources(source_key,authority,url,retrieved_at,sha256,byte_size) "
            "VALUES(?,?,?,?,?,?)",
            ("test", "FCC ULS", "https://example.test", "2026-01-01T00:00:00Z", "0" * 64, 0),
        )
        for usi, sign, status, service, grant, expired, action, name, frn in LICENSES:
            connection.execute(
                "INSERT INTO licenses(unique_system_id,source_id,source_archive,callsign,"
                "license_status,radio_service_code,grant_date,expired_date,last_action_date) "
                "VALUES(?,1,'test.zip',?,?,?,?,?,?)",
                (usi, sign, status, service, grant, expired, action),
            )
            connection.execute(
                "INSERT INTO entities(unique_system_id,callsign,entity_type,display_name,frn,state) "
                "VALUES(?,?,'L',?,?,?)",
                (usi, sign, name, frn, "NY"),
            )
            connection.execute(
                "INSERT INTO license_fts(callsign,display_name,radio_service_code,state,"
                "unique_system_id) VALUES(?,?,?,?,?)",
                (sign, name, service, "NY", usi),
            )
        connection.execute(
            "INSERT INTO locations(unique_system_id,callsign,location_number,county,state,"
            "latitude,longitude) VALUES(1,'TEST1',1,'KINGS','NY',40.7,-74.0)"
        )
        connection.execute(
            "INSERT INTO antennas(unique_system_id,callsign,location_number,antenna_number,"
            "azimuth_deg) VALUES(1,'TEST1',1,1,180.0)"
        )
        connection.execute(
            "INSERT INTO frequencies(unique_system_id,callsign,location_number,antenna_number,"
            "frequency_number,frequency_assigned_mhz,power_output_w) "
            "VALUES(1,'TEST1',1,1,1,462.5,50.0)"
        )
        connection.execute(
            "INSERT INTO emissions(unique_system_id,callsign,frequency_assigned_mhz,emission_code) "
            "VALUES(1,'TEST1',462.5,'11K0F3E')"
        )
        # A raw entity row so the sensitive-column policy can be tested against
        # real published field names rather than a synthetic table.
        connection.execute(
            "INSERT INTO raw_en(source_id,source_archive,source_row,unique_system_identifier,"
            "call_sign,entity_name,phone,email,street_address,zip_code,frn) "
            "VALUES(1,'test.zip',1,1,'TEST1','CITY OF TEST1','2125551212',"
            "'clerk@example.test','1 TEST PLAZA','10001','0001234567')"
        )
        # RTree entries are built by the importer, not by initialize().
        connection.execute(
            "INSERT INTO location_rtree(location_id,min_latitude,max_latitude,"
            "min_longitude,max_longitude) SELECT id,latitude,latitude,longitude,longitude "
            "FROM locations WHERE latitude IS NOT NULL"
        )
        connection.commit()
