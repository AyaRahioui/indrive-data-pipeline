DROP TABLE IF EXISTS staging.vehicles;
CREATE TABLE staging.vehicles AS
SELECT vehicle_id,
       driver_id,
       TRIM(make)         AS make,
       TRIM(model)        AS model,
       TRIM(vehicle_type) AS vehicle_type
FROM raw.vehicles;