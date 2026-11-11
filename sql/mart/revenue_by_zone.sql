DROP TABLE IF EXISTS mart.revenue_by_zone;
CREATE TABLE mart.revenue_by_zone AS
SELECT pickup_zone,
       COUNT(*)                  AS nb_rides,
       ROUND(SUM(fare_amount),2) AS total_revenue
FROM mart.rides
GROUP BY pickup_zone;