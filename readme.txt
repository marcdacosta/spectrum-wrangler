#example query


SELECT round(dms2dd('43° 0''50.60"S'),9) as latitude,
       round(dms2dd('147°12''18.20"E'),9) as longitude;
latitude    longitude
-43.014055556   147.205055556

COPY (
SELECT *
FROM fcclicenses
WHERE ST_DWithin(geom::geography,
                 ST_GeogFromText('POINT(-74.0076834 40.7253319)'),
                 1000, false)
) To '/tmp/trump-antennas.csv' With CSV HEADER DELIMITER ',';