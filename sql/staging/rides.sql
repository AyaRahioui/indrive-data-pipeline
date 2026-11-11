DROP TABLE IF EXISTS staging.rides;
CREATE TABLE staging.rides AS
SELECT DISTINCT ON (ride_id)               -- 1) supprime les doublons
       ride_id, driver_id, pickup_zone_id, dropoff_zone_id,
       requested_at, distance_km, fare_amount
FROM raw.rides
WHERE fare_amount > 0                      -- 2) retire les tarifs invalides
  AND driver_id IN (SELECT driver_id FROM raw.drivers)   -- 3) retire les chauffeurs inconnus
ORDER BY ride_id, ingested_at;