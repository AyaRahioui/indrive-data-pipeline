CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- Même ordre de colonnes que les CSV (nécessaire pour COPY)
CREATE TABLE raw.drivers (
  driver_id INT, driver_name TEXT, phone TEXT,
  city TEXT, rating NUMERIC(2,1), joined_date DATE
);

CREATE TABLE raw.vehicles (
  vehicle_id INT, driver_id INT, make TEXT,
  model TEXT, plate_number TEXT, vehicle_type TEXT
);

CREATE TABLE raw.zones (
  zone_id INT, zone_name TEXT, city TEXT
);

-- 1 ligne = 1 événement de course
CREATE TABLE raw.rides (
  ride_id TEXT,
  driver_id INT,
  pickup_zone_id INT,
  dropoff_zone_id INT,
  requested_at TIMESTAMP,
  distance_km NUMERIC,
  fare_amount NUMERIC,
  ingested_at TIMESTAMP DEFAULT now()
);