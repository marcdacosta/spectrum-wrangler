![Radio Wave Image](https://s3.amazonaws.com/marcdacosta.com/img/radiowave.png)

# What is Spectrum Wrangler?
Spectrum Wrangler will download the most recent version of the FCC License View database, parse it into a PostGIS table, clean up the geo columns so they can be used to query on, and produce a spatial index on them.


# About the FCC License View database
In the United States the Federal Communication Commission (FCC) is resposible for regulating the use of the radio waves. The FCC issues licenses for radio transmitters that exceed certain power levels or which operate on certain frequencies. The [License View](http://reboot.fcc.gov/license-view/) database harmonizes the schema of the various parts of the [Universal Licensing System](http://wireless.fcc.gov/uls/index.htm?job=transaction&page=weekly) (ULS) but with less detail than the individual ULS databases provide. Approximately 200,000 new licenses are granted each year and the historical sample size of active/inactive licenses is approximately 18M.

# How to use
* Configure the `user` `dbname` `server` & `port` in the [`load.py`](load.py) script to work with your PostGIS setup
* Ensure that you have read/write privlidges to `workingdir` specified in [`load.py`](load.py)
* Run `python load.py` 


# Requirements
* Access to a [PostGIS database server](http://postgis.net/install/)
* Python libraries `psycopg2` `urllib` `zipfile`
* 50GB of free space


# Examples
Example raw data extract can be found in [`sample-fcc.csv`](sample-fcc.csv)

## Query to test whether PostGIS extension successfully configured 

```sql
SELECT round(dms2dd('43° 0''50.60"S'),9) as latitude,
       round(dms2dd('147°12''18.20"E'),9) as longitude;
```       
```
latitude    longitude
-43.014055556   147.205055556
```
## Ranged queries 
Query to search within 1000m of [40.7253319,-74.0076834](https://www.google.com/maps/search/40.7253319,-74.0076834?sa=X&ved=0ahUKEwjj06n16p_WAhWR8oMKHUMfA9oQ8gEIJzAA) and export results

```sql
COPY (
SELECT *
FROM fcclicenses
WHERE ST_DWithin(geom::geography,
                 ST_GeogFromText('POINT(-74.0076834 40.7253319)'),
                 1000, false)
) To '/tmp/trump-antennas.csv' With CSV HEADER DELIMITER ',';
```
