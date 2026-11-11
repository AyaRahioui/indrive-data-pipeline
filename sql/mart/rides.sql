DROP TABLE IF EXISTS mart.rides;
CREATE TABLE mart.rides AS
SELECT r.ride_id,
       r.requested_at,
       r.driver_id,
       d.driver_name,
       d.rating,
       z.zone_name AS pickup_zone,
       r.distance_km,
       r.fare_amount
FROM staging.rides r
JOIN staging.drivers d      ON d.driver_id = r.driver_id
LEFT JOIN staging.zones z   ON z.zone_id   = r.pickup_zone_id;