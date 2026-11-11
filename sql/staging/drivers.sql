DROP TABLE IF EXISTS staging.drivers;
CREATE TABLE staging.drivers AS
SELECT driver_id,
       TRIM(driver_name) AS driver_name,
       TRIM(city)        AS city,
       rating,
       joined_date
FROM raw.drivers;