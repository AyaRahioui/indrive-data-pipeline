DROP TABLE IF EXISTS mart.driver_leaderboard;
CREATE TABLE mart.driver_leaderboard AS
SELECT m.driver_id,
       m.driver_name,
       v.vehicle_type,
       COUNT(*)                    AS nb_rides,
       ROUND(SUM(m.fare_amount),2) AS total_revenue
FROM mart.rides m
LEFT JOIN staging.vehicles v ON v.driver_id = m.driver_id
GROUP BY m.driver_id, m.driver_name, v.vehicle_type
ORDER BY total_revenue DESC;