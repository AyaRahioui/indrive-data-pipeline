DROP TABLE IF EXISTS staging.zones;
CREATE TABLE staging.zones AS
SELECT zone_id,
       TRIM(zone_name) AS zone_name,
       TRIM(city)      AS city
FROM raw.zones;