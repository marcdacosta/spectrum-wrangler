
## nb this will re-download and load the current "FCC License View" database. Expect script to run for ~1hr


import psycopg2
import urllib
import zipfile
import os
import shutil

conn = psycopg2.connect("dbname=fcc user=m")
cur = conn.cursor()

workingdir = "/tmp/fcclicense/"

if not os.path.exists(workingdir):
    os.makedirs(workingdir)

#download fcc license data data from http://reboot.fcc.gov/license-view/

licensezip_url = "http://data.fcc.gov/download/license-view/fcc-license-view-data-csv-format.zip"
licensezip = urllib.URLopener()
licensezip.retrieve(licensezip_url, workingdir + "fcc-license.zip")


zip_ref = zipfile.ZipFile(workingdir +  "fcc-license.zip", 'r')
zip_ref.extractall(workingdir)
zip_ref.close()


#remove old table if exists
cur.execute("select * from information_schema.tables where table_name=%s", ('fcclicenses',))
if (bool(cur.rowcount)):
	cur.execute("DROP TABLE fcclicenses;")
	conn.commit()


cur.execute("CREATE TABLE fcclicenses (license_id float,   source_system varchar,   callsign varchar,   facility_id float,   frn varchar,   lic_name varchar,   common_name varchar,   radio_service_code varchar, radio_service_desc varchar, rollup_category_code varchar, rollup_category_desc varchar, grant_date date, expired_date date, cancellation_date date,  last_action_date  date, lic_status_code  varchar, lic_status_desc  varchar, rollup_status_code  varchar, rollup_status_desc  varchar, entity_type_code  varchar, entity_type_desc  varchar, rollup_entity_code  varchar, rollup_entity_desc  varchar, lic_address  varchar, lic_city  varchar, lic_state  varchar, lic_zip_code  varchar, lic_attention_line  varchar, contact_company  varchar, contact_name  varchar, contact_title  varchar, contact_address1  varchar, contact_address2  varchar, contact_city  varchar, contact_state  varchar, contact_zip  varchar, contact_country  varchar, contact_phone  varchar, contact_fax  varchar, contact_email  varchar, market_code  varchar, market_desc  varchar, channel_block  varchar, loc_type_code  varchar, loc_type_desc  varchar, loc_city  varchar, loc_county_code  varchar, loc_county_name  varchar, loc_state  varchar, loc_radius_op  float, loc_seq_id  float, loc_lat_deg  float, loc_lat_min  float, loc_lat_sec  float, loc_lat_dir  varchar, loc_long_deg  float, loc_long_min  float, loc_long_sec  float, loc_long_dir  varchar, hgt_structure  float, asr_num  varchar, antenna_id  float, ant_seq_id  float, ant_make  varchar, ant_model  varchar, ant_type_code  varchar, ant_type_desc  varchar, azimuth  float, beamwidth  float, polarization_code varchar,   frequency_id  float, freq_seq_id  varchar, freq_class_station_code  varchar, freq_class_station_desc  varchar, power_erp  float, power_output  float, frequency_assigned  float, frequency_upper_band  float, unit_of_measure  varchar, tolerance  float, emission_id  float, emission_seq_id  float, emission_code  varchar, ground_elevation float);")
conn.commit()

cur.execute("COPY fcclicenses FROM '%s' CSV HEADER;", (workingdir + "fcc_lic_vw.csv",))
conn.commit()

shutil.rmtree(workingdir)

cur.close()
conn.close()
