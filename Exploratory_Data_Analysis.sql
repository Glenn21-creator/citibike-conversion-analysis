-- ============================================================
-- PROJECT  : Citi Bike Trips — Exploratory Data Analysis (EDA)
-- DATASET  : portfolio-projects-494318.citibike_trips.citibike_copy
-- PURPOSE  : Understand trip duration distributions, seasonal and
--            daily behavioural patterns, and identify Customers
--            whose ride behaviour resembles Subscribers —
--            the primary conversion target.
-- AUTHOR   : Glenn Ayuk
-- UPDATED  : 2026-04-25
-- NOTE     : Run the Data Cleaning script before this file.
--            All queries operate on citibike_copy (the clean table).
-- ============================================================


-- ============================================================
-- SECTION 1 — TRIP DURATION DISTRIBUTION
-- ============================================================

-- 1A. Distribution by usertype: Subscriber
--     Provides the full percentile profile to understand the
--     central tendency and spread of Subscriber ride lengths.
SELECT DISTINCT
  AVG(tripduration_min)                      OVER() AS mean_duration,
  MIN(tripduration_min)                      OVER() AS min_duration,
  PERCENTILE_CONT(tripduration_min, 0.25)    OVER() AS percentile_25,
  PERCENTILE_CONT(tripduration_min, 0.50)    OVER() AS percentile_50,
  PERCENTILE_CONT(tripduration_min, 0.75)    OVER() AS percentile_75,
  PERCENTILE_CONT(tripduration_min, 0.99)    OVER() AS percentile_99,
  MAX(tripduration_min)                      OVER() AS max_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Subscriber';

-- 1B. Distribution by usertype: Customer (casual riders)
SELECT DISTINCT
  AVG(tripduration_min)                      OVER() AS mean_duration,
  MIN(tripduration_min)                      OVER() AS min_duration,
  PERCENTILE_CONT(tripduration_min, 0.25)    OVER() AS percentile_25,
  PERCENTILE_CONT(tripduration_min, 0.50)    OVER() AS percentile_50,
  PERCENTILE_CONT(tripduration_min, 0.75)    OVER() AS percentile_75,
  PERCENTILE_CONT(tripduration_min, 0.99)    OVER() AS percentile_99,
  MAX(tripduration_min)                      OVER() AS max_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Customer';

-- 1C. Modal trip duration — Subscriber
--     The mode reveals the single most common trip length,
--     complementing the percentile picture above.
SELECT
  APPROX_TOP_COUNT(tripduration_min, 1)[OFFSET(0)].value AS modal_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Subscriber';

-- 1D. Modal trip duration — Customer
SELECT
  APPROX_TOP_COUNT(tripduration_min, 1)[OFFSET(0)].value AS modal_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Customer';


-- ============================================================
-- SECTION 2 — STANDARD DEVIATION & BOUNDS ANALYSIS
-- ============================================================

-- 2A. Baseline std_dev on the raw (pre-cleaned) table for reference.
--     Kept here to document the pre-cleaning spread.
SELECT
  STDDEV(tripduration_min) AS std_dev
FROM `portfolio-projects-494318.citibike_trips.citibike`;

-- 2B. Std_dev for Subscribers and customers on the raw table.
SELECT
  STDDEV(tripduration_min) AS std_dev
FROM `portfolio-projects-494318.citibike_trips.citibike`
WHERE usertype = 'Subscriber';

SELECT
  STDDEV(tripduration_min) AS std_dev
FROM `portfolio-projects-494318.citibike_trips.citibike`
WHERE usertype = 'Customer';

-- 2C. 3-sigma bounds for Subscribers (clean table).
--     Values beyond ±3 standard deviations are statistical outliers.
--     The lower_bound being negative is normal — duration cannot be negative,
--     so in practice the effective lower bound is 0 (or the 2-min filter applied
--     during cleaning).
SELECT
  AVG(tripduration_min)                                         AS mean_duration,
  STDDEV(tripduration_min)                                      AS std_dev,
  AVG(tripduration_min) - (3 * STDDEV(tripduration_min))       AS lower_bound,
  AVG(tripduration_min) + (3 * STDDEV(tripduration_min))       AS upper_bound
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Subscriber';

-- 2D. 3-sigma bounds for Customers (clean table).
SELECT
  AVG(tripduration_min)                                         AS mean_duration,
  STDDEV(tripduration_min)                                      AS std_dev,
  AVG(tripduration_min) - (3 * STDDEV(tripduration_min))       AS lower_bound,
  AVG(tripduration_min) + (3 * STDDEV(tripduration_min))       AS upper_bound
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Customer';


-- ============================================================
-- SECTION 3 — SEASONAL RIDE VOLUME ANALYSIS
-- ============================================================

-- 3A. Seasonal ride counts by usertype with period-over-period drop %.
--     The LAG window function compares each season's count to the
--     previous (higher) count within the same usertype, revealing
--     how sharply ridership falls across seasons.
WITH seasonal_ride AS (
  SELECT
    usertype,
    season,
    COUNT(*) AS seasonal_ride_count
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
  GROUP BY usertype, season
)
SELECT
  *,
  LAG(seasonal_ride_count, 1, 0)
    OVER (PARTITION BY usertype ORDER BY seasonal_ride_count DESC) AS prev_ride_count,
  ABS(ROUND((
    (seasonal_ride_count - LAG(seasonal_ride_count, 1, 0)
      OVER (PARTITION BY usertype ORDER BY seasonal_ride_count DESC))
    / NULLIF(seasonal_ride_count, 0)
  ), 2)) * 100 AS perc_decrease
FROM seasonal_ride
ORDER BY usertype, seasonal_ride_count DESC;

-- 3B. Average trip duration by season and usertype.
--     Highlights whether certain seasons drive longer or shorter trips.
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY usertype ORDER BY avg_tripduration DESC) AS duration_rank
FROM (
  SELECT
    usertype,
    season,
    ROUND(AVG(tripduration_min), 2) AS avg_tripduration
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
  GROUP BY usertype, season
);


-- ============================================================
-- SECTION 4 — GRANULAR TIME-OF-DAY DISTRIBUTION
-- ============================================================

-- 4A. Duration profile for Customers on Weekday Mornings.
--     Used to isolate the overlap segment between casual and commuter behaviour.
SELECT DISTINCT
  AVG(tripduration_min)                      OVER() AS mean_duration,
  MIN(tripduration_min)                      OVER() AS min_duration,
  PERCENTILE_CONT(tripduration_min, 0.25)    OVER() AS percentile_25,
  PERCENTILE_CONT(tripduration_min, 0.50)    OVER() AS percentile_50,
  PERCENTILE_CONT(tripduration_min, 0.75)    OVER() AS percentile_75,
  PERCENTILE_CONT(tripduration_min, 0.99)    OVER() AS percentile_99,
  MAX(tripduration_min)                      OVER() AS max_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype     = 'Customer'
  AND time_of_day  = 'Morning'
  AND week_category = 'Weekday';

-- 4B. Modal duration for Customers (all conditions, for reference).
SELECT
  APPROX_TOP_COUNT(tripduration_min, 1)[OFFSET(0)].value AS modal_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Customer';

-- 4C. Duration profile for all Subscribers.
SELECT DISTINCT
  AVG(tripduration_min)                      OVER() AS mean_duration,
  MIN(tripduration_min)                      OVER() AS min_duration,
  PERCENTILE_CONT(tripduration_min, 0.25)    OVER() AS percentile_25,
  PERCENTILE_CONT(tripduration_min, 0.50)    OVER() AS percentile_50,
  PERCENTILE_CONT(tripduration_min, 0.75)    OVER() AS percentile_75,
  PERCENTILE_CONT(tripduration_min, 0.99)    OVER() AS percentile_99,
  MAX(tripduration_min)                      OVER() AS max_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Subscriber';

-- 4D. Modal duration for Subscribers in Winter specifically.
--     Helps quantify whether colder months shift the typical ride length.
SELECT
  APPROX_TOP_COUNT(tripduration_min, 1)[OFFSET(0)].value AS modal_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Subscriber'
  AND season   = 'Winter';


-- ============================================================
-- SECTION 5 — DAILY PATTERNS: SUBSCRIBER BEHAVIOUR
-- ============================================================

-- 5A. Most frequent start and stop times by day of week for Subscribers.
--     Reveals peak commute hours per weekday — key for understanding
--     habitual usage patterns that define the Subscriber segment.
SELECT
  day_of_week,
  APPROX_TOP_COUNT(EXTRACT(TIME FROM starttime), 1)[OFFSET(0)].value AS most_freq_starttime,
  APPROX_TOP_COUNT(EXTRACT(TIME FROM stoptime),  1)[OFFSET(0)].value AS most_freq_stoptime
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE usertype = 'Subscriber'
GROUP BY day_of_week;

-- 5B. Average trip duration by day of week and time of day for Subscribers.
--     Identifies which day/time combinations produce the longest or shortest trips.
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY day_of_week ORDER BY avg_tripduration DESC) AS row_num
FROM (
  SELECT
    day_of_week,
    time_of_day,
    AVG(tripduration_min) AS avg_tripduration
  FROM `portfolio-projects-494316.citibike_trips.citibike_copy`
  WHERE usertype = 'Subscriber'
  GROUP BY 1, 2
)
ORDER BY avg_tripduration DESC;


-- ============================================================
-- SECTION 6 — CUSTOMER RIDES SIMILAR TO SUBSCRIBER (CusRsimSub)
-- ============================================================
-- OBJECTIVE: Identify casual Customers' ride behaviour that
-- mirror Subscriber commute patterns — i.e., short weekday
-- morning trips (5–20 min). These are the highest-value
-- conversion targets for a subscription marketing campaign.
-- ============================================================

-- 6A. CLEAN: Create a Materialised View of Customer rides that
--     match the Subscriber commute profile.
--     Criteria: duration 5–20 min, Weekday, Morning.
CREATE OR REPLACE MATERIALIZED VIEW `portfolio-projects-494318.citibike_trips.mv_CusRsimSub` AS
SELECT *
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE tripduration_min BETWEEN 5 AND 20
  AND usertype       = 'Customer'
  AND week_category  = 'Weekday'
  AND time_of_day    = 'Morning';

-- 6B. What percentage of total Subscriber rides does CusRsimSub represent,
--     broken down by year?
--     NOTE: The denominator uses Subscriber count — this measures how large
--     the CusRsimSub group is relative to the existing Subscriber base.
--     Consider also including total Customer count as an alternative denominator.
SELECT
  EXTRACT(YEAR FROM starttime) AS year,
  ROUND(
    (COUNT(*) / NULLIF((
      SELECT COUNT(*)
      FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
      WHERE usertype = 'Subscriber'
    ), 0)) * 100, 2
  ) AS perc_CusRsimSub
FROM `portfolio-projects-494318.citibike_trips.mv_CusRsimSub`
GROUP BY 1
ORDER BY 1;

-- 6C. Demographic breakdown of CusRsimSub riders by age group and gender.
--     Identifies which demographic segments are most likely conversion candidates.
SELECT
  age_group,
  gender,
  ROUND(
    (COUNT(*) / NULLIF((
      SELECT COUNT(*)
      FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
      WHERE usertype = 'Subscriber'
    ), 0)) * 100, 3
  ) AS perc_CusRsimSub
FROM `portfolio-projects-494318.citibike_trips.mv_CusRsimSub`
GROUP BY 1, 2
ORDER BY 3 DESC;

-- 6D. Top start stations used by CusRsimSub riders.
--     Useful for targeting physical marketing or promotional incentives
--     at specific station locations.
SELECT
  start_station_id,
  start_station_name,
  COUNT(*) AS station_count
FROM `portfolio-projects-494318.citibike_trips.mv_CusRsimSub`
GROUP BY 1, 2
ORDER BY COUNT(*) DESC;

-- 6E. Top end stations used by CusRsimSub riders.
SELECT
  end_station_id,
  end_station_name,
  COUNT(*) AS station_count
FROM `portfolio-projects-494318.citibike_trips.mv_CusRsimSub`
GROUP BY 1, 2
ORDER BY COUNT(*) DESC;


-- ============================================================
-- SECTION 7 — HIGH-TRAFFIC STATION ANALYSIS (CusRsimSub)
-- ============================================================

-- 7A. Identify the busiest stations (start + end combined) for CusRsimSub
--     riders during Weekday Mornings.
--     FULL OUTER JOIN ensures stations that appear only as start or only
--     as end are still captured. time_of_day is included in the join key
--     to prevent row multiplication across time periods.
WITH start_station AS (
  SELECT
    time_of_day,
    start_station_name                                                     AS station_name,
    CONCAT(start_station_latitude, ',', start_station_longitude)           AS location,
    COUNT(*)                                                               AS start_station_count
  FROM `portfolio-projects-494318.citibike_trips.mv_CusRsimSub`
  GROUP BY 1, 2, 3
),
end_station AS (
  SELECT
    time_of_day,
    end_station_name                                                       AS station_name,
    COUNT(*)                                                               AS end_station_count
  FROM `portfolio-projects-494318.citibike_trips.mv_CusRsimSub`
  GROUP BY 1, 2
)
SELECT
  COALESCE(s.station_name,  e.station_name)  AS station_name,
  s.location,
  COALESCE(s.time_of_day,   e.time_of_day)   AS time_of_day,
  IFNULL(s.start_station_count, 0)           AS start_station_count,
  IFNULL(e.end_station_count,   0)           AS end_station_count,
  (IFNULL(s.start_station_count, 0) + IFNULL(e.end_station_count, 0)) AS total_station_traffic
FROM start_station s
FULL OUTER JOIN end_station e
  ON s.station_name = e.station_name
 AND s.time_of_day  = e.time_of_day
ORDER BY total_station_traffic DESC;
