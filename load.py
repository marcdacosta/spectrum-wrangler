#coding: utf-8
## nb this will re-download and load the current "FCC License View" database. Expect script to run for ~1hr
## data notes: ~31% have null values for location

import psycopg2
import requests
import zipfile
import os
import shutil

conn = psycopg2.connect("dbname=fcc2 user=m")
cur = conn.cursor()

workingdir = "/tmp/fcclicense/"

if not os.path.exists(workingdir):
    os.makedirs(workingdir)

#download fcc license data data from http://reboot.fcc.gov/license-view/

# print "Downloading from FCC server"
# licensezip_url = "http://data.fcc.gov/download/license-view/fcc-license-view-data-csv-format.zip"
# r = requests.get(licensezip_url, allow_redirects=True)  # to get content after redirection
# zip_url = r.url # 'https://media.readthedocs.org/pdf/django/latest/django.pdf'
# with open(workingdir + "fcc-license.zip", 'wb') as f:
#     f.write(r.content)

# print "Unzipping file"
# zip_ref = zipfile.ZipFile(workingdir +  "fcc-license.zip", 'r')
# zip_ref.extractall(workingdir)
# zip_ref.close()

# print "Checking if table exists"
# #remove old table if exists
# cur.execute("select * from information_schema.tables where table_name=%s", ('fcclicenses',))
# if (bool(cur.rowcount)):
# 	cur.execute("DROP TABLE fcclicenses;")
# 	conn.commit()

# print "Creating table and loading data"
# cur.execute("CREATE TABLE fcclicenses (license_id float,   source_system varchar,   callsign varchar,   facility_id float,   frn varchar,   lic_name varchar,   common_name varchar,   radio_service_code varchar, radio_service_desc varchar, rollup_category_code varchar, rollup_category_desc varchar, grant_date date, expired_date date, cancellation_date date,  last_action_date  date, lic_status_code  varchar, lic_status_desc  varchar, rollup_status_code  varchar, rollup_status_desc  varchar, entity_type_code  varchar, entity_type_desc  varchar, rollup_entity_code  varchar, rollup_entity_desc  varchar, lic_address  varchar, lic_city  varchar, lic_state  varchar, lic_zip_code  varchar, lic_attention_line  varchar, contact_company  varchar, contact_name  varchar, contact_title  varchar, contact_address1  varchar, contact_address2  varchar, contact_city  varchar, contact_state  varchar, contact_zip  varchar, contact_country  varchar, contact_phone  varchar, contact_fax  varchar, contact_email  varchar, market_code  varchar, market_desc  varchar, channel_block  varchar, loc_type_code  varchar, loc_type_desc  varchar, loc_city  varchar, loc_county_code  varchar, loc_county_name  varchar, loc_state  varchar, loc_radius_op  float, loc_seq_id  float, loc_lat_deg  float, loc_lat_min  float, loc_lat_sec  float, loc_lat_dir  varchar, loc_long_deg  float, loc_long_min  float, loc_long_sec  float, loc_long_dir  varchar, hgt_structure  float, asr_num  varchar, antenna_id  float, ant_seq_id  float, ant_make  varchar, ant_model  varchar, ant_type_code  varchar, ant_type_desc  varchar, azimuth  float, beamwidth  float, polarization_code varchar,   frequency_id  float, freq_seq_id  varchar, freq_class_station_code  varchar, freq_class_station_desc  varchar, power_erp  float, power_output  float, frequency_assigned  float, frequency_upper_band  float, unit_of_measure  varchar, tolerance  float, emission_id  float, emission_seq_id  float, emission_code  varchar, ground_elevation float);")
# conn.commit()

# cur.execute("COPY fcclicenses FROM %s CSV HEADER;", (workingdir + "fcc_lic_vw.csv",))
# conn.commit()

# print "Removing cached files"
# shutil.rmtree(workingdir)

def addDMS2DDextension():
	cur.execute("""\
CREATE OR REPLACE FUNCTION DMS2DD(strDegMinSec varchar)
    RETURNS numeric
    AS
    $$
    DECLARE
       i               numeric;
       intDmsLen       numeric;          -- Length of original string
       strCompassPoint Char(1);
       strNorm         varchar(16) = ''; -- Will contain normalized string
       strDegMinSecB   varchar(100);
       blnGotSeparator integer;          -- Keeps track of separator sequences
       arrDegMinSec    varchar[];        -- TYPE stringarray is table of varchar(2048) ;
       dDeg            numeric := 0;
       dMin            numeric := 0;
       dSec            numeric := 0;
       strChr          Char(1);
    BEGIN
       -- Remove leading and trailing spaces
       strDegMinSecB := REPLACE(strDegMinSec,' ','');
       -- assume no leading and trailing spaces?
       intDmsLen := Length(strDegMinSecB);

       blnGotSeparator := 0; -- Not in separator sequence right now

       -- Loop over string, replacing anything that is not a digit or a
       -- decimal separator with
       -- a single blank
       FOR i in 1..intDmsLen LOOP
          -- Get current character
          strChr := SubStr(strDegMinSecB, i, 1);
          -- either add character to normalized string or replace
          -- separator sequence with single blank         
          If strpos('0123456789,.', strChr) > 0 Then
             -- add character but replace comma with point
             If (strChr <> ',') Then
                strNorm := strNorm || strChr;
             Else
                strNorm := strNorm || '.';
             End If;
             blnGotSeparator := 0;
          ElsIf strpos('neswNESW',strChr) > 0 Then -- Extract Compass Point if present
            strCompassPoint := strChr;
          Else
             -- ensure only one separator is replaced with a blank -
             -- suppress the rest
             If blnGotSeparator = 0 Then
                strNorm := strNorm || ' ';
                blnGotSeparator := 0;
             End If;
          End If;
       End Loop;

       -- Split normalized string into array of max 3 components
       arrDegMinSec := string_to_array(strNorm, ' ');

       --convert specified components to double
       i := array_upper(arrDegMinSec,1);
       If i >= 1 Then
          dDeg := CAST(arrDegMinSec[1] AS numeric);
       End If;
       If i >= 2 Then
          dMin := CAST(arrDegMinSec[2] AS numeric);
       End If;
       If i >= 3 Then
          dSec := CAST(arrDegMinSec[3] AS numeric);
       End If;

       -- convert components to value
       return (CASE WHEN UPPER(strCompassPoint) IN ('S','W') 
                    THEN -1 
                    ELSE 1 
                END 
               *
               (dDeg + dMin / 60 + dSec / 3600));
    End 
$$
    LANGUAGE 'plpgsql' IMMUTABLE;
	""")
	conn.commit()

def geoindextable():
	cur.execute("alter table fcclicenses add column lat float")
	cur.execute("alter table fcclicenses add column long float")
	cur.execute("""\
		UPDATE fcclicenses
		SET lat = round(dms2dd(
		CONCAT  (LOC_LAT_DEG, '° ', LOC_LAT_MIN, ''' ', LOC_LAT_SEC, '" ', LOC_LAT_DIR )),9)
		WHERE LOC_LAT_DEG > 0 and LOC_LAT_MIN > 0 and LOC_LAT_SEC > 0 and LOC_LAT_DIR is not null
		""")
	cur.execute("""\
		UPDATE fcclicenses
		SET long = round(dms2dd(
		CONCAT  (LOC_LONG_DEG, '° ', LOC_LONG_MIN, ''' ', LOC_LONG_SEC, '" ', LOC_LONG_DIR )),9)
		WHERE LOC_LONG_DEG > 0 and LOC_LONG_MIN > 0 and LOC_LONG_SEC > 0 and LOC_LONG_DIR is not null
		""")
	#cur.execute("CREATE EXTENSION postgis")
	#cur.execute("alter table fcclicenses ADD COLUMN geom geometry(POINT,4326)")
	cur.execute("update fcclicenses SET geom = ST_SetSRID(ST_MakePoint(long,lat),4326)")
	cur.execute("create index idx_geom on fcclicenses using GIST(geom)")
	conn.commit()

#print "Adding DMS2DD PostGIS extension"
#addDMS2DDextension()
print "Geo-indexing table"
geoindextable()
print "Done"




cur.close()
conn.close()
