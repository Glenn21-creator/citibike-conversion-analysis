-- ============================================================
-- PROJECT  : Citi Bike Trips — Data Cleaning
-- DATASET  : portfolio-projects-494318.citibike_trips.citibike
-- PURPOSE  : Prepare raw Citi Bike trip data for analysis by
--            removing nulls, duplicates, test rides, and outliers;
--            standardising station names and coordinates;
--            and engineering derived columns.
-- AUTHOR   : Glenn Ayuk
-- UPDATED  : 2026-04-25
-- ============================================================


-- ============================================================
-- SECTION 1 — NULL REMOVAL
-- ============================================================

-- 1A. AUDIT: Count rows where both starttime and stoptime are NULL
--     Run this BEFORE the DELETE to document the scope of the issue.
SELECT 
  COUNT(*) AS null_trip_count
FROM (
  SELECT *
  FROM `portfolio-projects-494318.citibike_trips.citibike`
  WHERE starttime IS NULL
    AND stoptime IS NULL
);

-- 1B. CLEAN: Delete rows where both timestamps are NULL.
--     These records represent trips with no temporal anchor
--     and cannot be used in any time-based analysis.
DELETE FROM `portfolio-projects-494318.citibike_trips.citibike`
WHERE starttime IS NULL
  AND stoptime IS NULL;

-- 1C. VERIFY: Confirm no NULL-timestamp rows remain.
--     Expected result: null_trip_count = 0
SELECT
  COUNT(*) AS null_trip_count
FROM `portfolio-projects-494318.citibike_trips.citibike`
WHERE starttime IS NULL
   OR stoptime IS NULL;


-- ============================================================
-- SECTION 2 — UNIT CONVERSION: tripduration seconds → minutes
-- ============================================================

-- 2A. CLEAN: Add a new column for trip duration in minutes.
ALTER TABLE `portfolio-projects-494318.citibike_trips.citibike`
ADD COLUMN tripduration_min FLOAT64;

-- 2B. CLEAN: Populate tripduration_min by dividing seconds by 60.
--     CAST to FLOAT64 first to avoid integer division truncation,
--     then CAST result to INT64 for whole-minute granularity.
UPDATE `portfolio-projects-494318.citibike_trips.citibike`
SET tripduration_min = CAST(CAST(tripduration AS FLOAT64) / 60 AS INT64)
WHERE TRUE;

-- 2C. CLEAN: Drop the original seconds column to avoid confusion.
ALTER TABLE `portfolio-projects-494318.citibike_trips.citibike`
DROP COLUMN tripduration;

-- 2D. VERIFY: Confirm the conversion is reasonable.
--     min_duration should be > 0; max_duration should be plausible (thousands of minutes).
--     null_count should be 0 if the UPDATE covered all rows.
SELECT
  MIN(tripduration_min)   AS min_duration,
  MAX(tripduration_min)   AS max_duration,
  AVG(tripduration_min)   AS avg_duration,
  COUNTIF(tripduration_min IS NULL) AS null_count
FROM `portfolio-projects-494318.citibike_trips.citibike`;


-- ============================================================
-- SECTION 3 — DUPLICATE REMOVAL
-- ============================================================

-- 3A. AUDIT: Identify duplicate rows using ROW_NUMBER().
--     A duplicate is a row with identical values across ALL
--     columns. row_num > 1 flags the copies.
WITH duplicate_check AS (
  SELECT
    ROW_NUMBER() OVER(
      PARTITION BY
        starttime, stoptime,
        start_station_id, start_station_name,
        CAST(start_station_latitude  AS STRING),
        CAST(start_station_longitude AS STRING),
        end_station_id, end_station_name,
        CAST(end_station_latitude    AS STRING),
        CAST(end_station_longitude   AS STRING),
        bikeid, usertype, birth_year, gender,
        customer_plan,
        CAST(tripduration_min AS STRING)
      ORDER BY starttime
    ) AS row_num,
    *
  FROM `portfolio-projects-494318.citibike_trips.citibike`
)
SELECT *
FROM duplicate_check
WHERE row_num > 1;

-- 3B. CLEAN: Materialise deduplicated data into a working copy table.
--     BigQuery does not support DELETE with window functions directly,
--     Hence created a copy and remove duplicates from there.
CREATE TABLE `portfolio-projects-494318.citibike_trips.citibike_copy` AS (
  SELECT
    ROW_NUMBER() OVER(
      PARTITION BY
        starttime, stoptime,
        start_station_id, start_station_name,
        CAST(start_station_latitude  AS STRING),
        CAST(start_station_longitude AS STRING),
        end_station_id, end_station_name,
        CAST(end_station_latitude    AS STRING),
        CAST(end_station_longitude   AS STRING),
        bikeid, usertype, birth_year, gender,
        customer_plan,
        CAST(tripduration_min AS STRING)
      ORDER BY starttime
    ) AS row_num,
    *
  FROM `portfolio-projects-494318.citibike_trips.citibike`
);

DELETE FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE row_num > 1;

-- 3C. VERIFY: Confirm all duplicates have been removed.
--     Expected result: zero rows returned.
SELECT *
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE row_num > 1;


-- ============================================================
-- SECTION 4 — TEST RIDE REMOVAL
-- ============================================================

-- 4A. AUDIT: Preview rides where the start station name contains 'TEST'.
--     These are internal quality-assurance rides and should not
--     be included in customer behaviour analysis.
SELECT *
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE REGEXP_CONTAINS(start_station_name, 'TEST');

-- 4B. CLEAN: Delete the identified test rides.
DELETE FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE REGEXP_CONTAINS(start_station_name, 'TEST');

-- 4C. VERIFY: Confirm no test rides remain.
--     Expected result: test_ride_count = 0
SELECT
  COUNT(*) AS test_ride_count
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE REGEXP_CONTAINS(start_station_name, 'TEST');


-- ============================================================
-- SECTION 5 — STATION NAME STANDARDISATION
-- ============================================================

-- 5A. AUDIT: Identify start stations with multiple name variants
--     for the same station ID (potential misspellings / inconsistencies).
--     The most-used name per ID is selected as the canonical version.
WITH unique_stations AS (
  SELECT
    start_station_id,
    start_station_name,
    COUNT(*) AS ride_count
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
  GROUP BY start_station_id, start_station_name
),
ranked_stations AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY start_station_id
      ORDER BY ride_count DESC
    ) AS rank_
  FROM unique_stations
),
correct_names AS (
  SELECT
    start_station_id,
    start_station_name AS correct_name
  FROM ranked_stations
  WHERE rank_ = 1
)
SELECT *
FROM correct_names
ORDER BY start_station_id;

-- 5B. CLEAN: Replace all non-canonical start_station_names with the
--     most-frequently occurring name for that station ID.
UPDATE `portfolio-projects-494318.citibike_trips.citibike_copy` t1
SET t1.start_station_name = t2.correct_name
FROM (
  SELECT
    start_station_id,
    start_station_name AS correct_name
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(
        PARTITION BY start_station_id
        ORDER BY ride_count DESC
      ) AS rank_
    FROM (
      SELECT
        start_station_id,
        start_station_name,
        COUNT(*) AS ride_count
      FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
      GROUP BY start_station_id, start_station_name
    )
  )
  WHERE rank_ = 1
) t2
WHERE t1.start_station_id = t2.start_station_id
  AND t1.start_station_name != t2.correct_name;

-- 5C. VERIFY: After standardisation, each station_id should map to
--     exactly one name. This query should return zero rows.
WITH unique_start_stations AS (
  SELECT
    start_station_id,
    start_station_name,
    COUNT(*) AS ride_count
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
  GROUP BY start_station_id, start_station_name
),
ranked_start_stations AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY start_station_id
      ORDER BY ride_count DESC
    ) AS rank_
  FROM unique_start_stations
)
SELECT *
FROM ranked_start_stations
WHERE rank_ > 1;

-- 5D. AUDIT: Identify end stations with multiple name variants
--     for the same end_station_id.
WITH unique_end_stations AS (
  SELECT
    end_station_id,
    end_station_name,
    COUNT(*) AS ride_count
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
  GROUP BY end_station_id, end_station_name
),
ranked_end_stations AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY end_station_id
      ORDER BY ride_count DESC
    ) AS rank_
  FROM unique_end_stations
)
SELECT *
FROM ranked_end_stations
WHERE rank_ > 1
ORDER BY end_station_id;

-- 5E. CLEAN: Use the already-standardised start_station_name as the
--     reference source to correct end_station_name where they share an ID.
--     This leverages the work done in 5B and avoids repeating the logic.
UPDATE `portfolio-projects-494318.citibike_trips.citibike_copy` t1
SET t1.end_station_name = t2.correct_station_name
FROM (
  SELECT DISTINCT
    start_station_id,
    start_station_name AS correct_station_name
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
) t2
WHERE t2.start_station_id = t1.end_station_id
  AND t2.correct_station_name != t1.end_station_name;

-- 5F. VERIFY: Each end_station_id should now map to exactly one name.
--     Expected result: zero rows returned.
WITH unique_end_stations AS (
  SELECT
    end_station_id,
    end_station_name,
    COUNT(*) AS ride_count
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
  GROUP BY end_station_id, end_station_name
),
ranked_end_stations AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY end_station_id
      ORDER BY ride_count DESC
    ) AS rank_
  FROM unique_end_stations
)
SELECT *
FROM ranked_end_stations
WHERE rank_ > 1;


-- ============================================================
-- SECTION 6 — COORDINATE STANDARDISATION & CONSOLIDATION
-- ============================================================

-- 6A. AUDIT: Preview coordinate variants per station to understand
--     how many stations have conflicting lat/lng values.
WITH unique_locations AS (
  SELECT
    start_station_id,
    start_station_name,
    CONCAT(start_station_latitude, ',', start_station_longitude) AS location,
    COUNT(*) AS station_count
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
  GROUP BY 1, 2, 3
),
ranked_locations AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY start_station_id, start_station_name
      ORDER BY station_count DESC
    ) AS rank_
  FROM unique_locations
)
SELECT *
FROM ranked_locations;

-- 6B. CLEAN: Combine latitude and longitude into a single location column
--     for easier deduplication, then drop the four raw coordinate columns.
CREATE OR REPLACE TABLE `portfolio-projects-494318.citibike_trips.citibike_copy` AS
SELECT
  *,
  CONCAT(start_station_latitude, ',', start_station_longitude) AS start_location,
  CONCAT(end_station_latitude,   ',', end_station_longitude)   AS end_location
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;

ALTER TABLE `portfolio-projects-494318.citibike_trips.citibike_copy`
  DROP COLUMN start_station_latitude,
  DROP COLUMN start_station_longitude,
  DROP COLUMN end_station_latitude,
  DROP COLUMN end_station_longitude;

-- 6C. CLEAN: Standardise start_location — replace less-frequent
--     coordinate variants with the most-common value per station ID.
UPDATE `portfolio-projects-494318.citibike_trips.citibike_copy` t1
SET t1.start_location = t2.correct_location
FROM (
  SELECT
    start_station_id,
    start_location AS correct_location
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(
        PARTITION BY start_station_id
        ORDER BY station_count DESC
      ) AS rank_
    FROM (
      SELECT
        start_station_id,
        start_location,
        COUNT(*) AS station_count
      FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
      GROUP BY start_station_id, start_location
    )
  )
  WHERE rank_ = 1
) t2
WHERE t1.start_station_id = t2.start_station_id
  AND t1.start_location   != t2.correct_location;

-- 6D. CLEAN: Use standardised start_location as reference to
--     correct end_location for matching station IDs.
UPDATE `portfolio-projects-494318.citibike_trips.citibike_copy` t1
SET t1.end_location = t2.correct_location
FROM (
  SELECT DISTINCT
    start_station_id,
    start_location AS correct_location
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
) t2
WHERE t2.start_station_id = t1.end_station_id
  AND t2.correct_location  != t1.end_location;

-- 6E. VERIFY: Each end_station_id should map to exactly one location.
--     Expected result: zero rows returned.
WITH unique_locations AS (
  SELECT
    end_station_id,
    end_location,
    COUNT(*) AS station_count
  FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
  GROUP BY 1, 2
),
ranked_locations AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY end_station_id, end_location
      ORDER BY station_count DESC
    ) AS rank_
  FROM unique_locations
)
SELECT *
FROM ranked_locations
WHERE rank_ > 1;

-- 6F. CLEAN: Re-split the cleaned location strings back into individual
--     latitude and longitude columns for downstream geo-analysis.
CREATE OR REPLACE TABLE `portfolio-projects-494318.citibike_trips.citibike_copy` AS
SELECT
  *,
  TRIM(SPLIT(start_location, ',')[SAFE_OFFSET(0)]) AS start_station_latitude,
  TRIM(SPLIT(start_location, ',')[SAFE_OFFSET(1)]) AS start_station_longitude,
  TRIM(SPLIT(end_location,   ',')[SAFE_OFFSET(0)]) AS end_station_latitude,
  TRIM(SPLIT(end_location,   ',')[SAFE_OFFSET(1)]) AS end_station_longitude
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;

-- 6G. VERIFY: Spot-check that coordinates were split correctly.
--     Review a sample; all lat/lng columns should contain numeric-looking values.
SELECT
  start_station_id,
  start_station_latitude,
  start_station_longitude,
  end_station_id,
  end_station_latitude,
  end_station_longitude
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
LIMIT 20;

-- 6H. CLEAN: Drop the intermediate combined location columns
--     now that coordinates are properly separated again.
ALTER TABLE `portfolio-projects-494318.citibike_trips.citibike_copy`
  DROP COLUMN location,
  DROP COLUMN start_location,
  DROP COLUMN end_location;


-- ============================================================
-- SECTION 7 — COLUMN CLEANUP
-- ============================================================

-- 7A. CLEAN: Drop customer_plan — not used in the analysis scope.
ALTER TABLE `portfolio-projects-494318.citibike_trips.citibike_copy`
DROP COLUMN customer_plan;

-- 7B. CLEAN: Drop the row_num helper column introduced in Section 3.
ALTER TABLE `portfolio-projects-494318.citibike_trips.citibike_copy`
DROP COLUMN row_num;

-- 7C. VERIFY: Confirm the table schema looks correct after cleanup.
SELECT *
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
LIMIT 10;


-- ============================================================
-- SECTION 8 — FEATURE ENGINEERING: DERIVED COLUMNS
-- ============================================================

-- 8A. CLEAN: Add derived categorical columns for temporal and
--     demographic segmentation.
ALTER TABLE `portfolio-projects-494318.citibike_trips.citibike_copy`
ADD COLUMN season        STRING,
ADD COLUMN day_of_week   STRING,
ADD COLUMN week_category STRING,
ADD COLUMN time_of_day   STRING,
ADD COLUMN age_group     STRING;

-- 8B. CLEAN: Populate all derived columns in a single UPDATE pass.
--     NOTE: age_group uses 2018 as the reference year — update if
--     the dataset spans multiple years or if the analysis year changes.
UPDATE `portfolio-projects-494318.citibike_trips.citibike_copy`
SET
  season = CASE
    WHEN EXTRACT(MONTH FROM starttime) IN (12, 1, 2)  THEN 'Winter'
    WHEN EXTRACT(MONTH FROM starttime) IN (3,  4, 5)  THEN 'Spring'
    WHEN EXTRACT(MONTH FROM starttime) IN (6,  7, 8)  THEN 'Summer'
    WHEN EXTRACT(MONTH FROM starttime) IN (9, 10, 11) THEN 'Autumn'
  END,

  day_of_week   = FORMAT_TIMESTAMP('%A', starttime),

  week_category = CASE
    WHEN EXTRACT(DAYOFWEEK FROM starttime) BETWEEN 2 AND 6 THEN 'Weekday'
    ELSE 'Weekend'
  END,

  time_of_day = CASE
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 5  AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 17 AND 20 THEN 'Evening'
    ELSE 'Night'
  END,

  age_group = CASE
    WHEN 2018 - birth_year BETWEEN 18 AND 25 THEN '18 - 25'
    WHEN 2018 - birth_year BETWEEN 26 AND 35 THEN '26 - 35'
    WHEN 2018 - birth_year BETWEEN 36 AND 45 THEN '36 - 45'
    WHEN 2018 - birth_year BETWEEN 46 AND 60 THEN '46 - 60'
    WHEN 2018 - birth_year > 60              THEN '60+'
    ELSE '-18'
  END
WHERE TRUE;

-- 8C. VERIFY: Confirm every row has a non-NULL value in all derived columns
--     and that no unexpected categories exist.
SELECT
  season,
  day_of_week,
  week_category,
  time_of_day,
  age_group,
  COUNT(*) AS row_count
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2;

-- Additional check: flag any NULLs in derived columns
SELECT
  COUNTIF(season        IS NULL) AS null_season,
  COUNTIF(day_of_week   IS NULL) AS null_day_of_week,
  COUNTIF(week_category IS NULL) AS null_week_category,
  COUNTIF(time_of_day   IS NULL) AS null_time_of_day,
  COUNTIF(age_group     IS NULL) AS null_age_group
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;


-- ============================================================
-- SECTION 9 — UNIQUE TRIP ID GENERATION
-- ============================================================

-- 9A. CLEAN: Assign a UUID as the primary key for each trip row.
--     This enables reliable row-level referencing in downstream analysis.
CREATE OR REPLACE TABLE `portfolio-projects-494318.citibike_trips.citibike_copy` AS
SELECT
  GENERATE_UUID() AS trip_id,
  *
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;

-- 9B. VERIFY: Confirm all trip_ids are unique (count should equal table row count).
SELECT
  COUNT(*)              AS total_rows,
  COUNT(DISTINCT trip_id) AS unique_trip_ids
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;


-- ============================================================
-- SECTION 10 — OUTLIER REMOVAL (tripduration_min)
-- ============================================================

-- 10A. AUDIT: Check the overall distribution to understand skewness
--      before removing outliers.
--      Findings: std_dev ≈ 226 min — extremely right-skewed distribution.
SELECT
  ROUND(STDDEV(tripduration_min)) AS std_dev
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;

-- 10B. AUDIT: Review the full percentile profile for context.
--      Key findings:
--        99th pct overall   ≈ 62 min; max = 325,167 min
--        99th pct Customer  ≈ 184 min; max = 325,167 min
--        99th pct Subscriber ≈ 44 min; max = 226,438 min
SELECT DISTINCT
  usertype,
  AVG(tripduration_min)                      OVER(PARTITION BY usertype) AS mean_duration,
  MIN(tripduration_min)                      OVER(PARTITION BY usertype) AS min_duration,
  PERCENTILE_CONT(tripduration_min, 0.25)    OVER(PARTITION BY usertype) AS percentile_25,
  PERCENTILE_CONT(tripduration_min, 0.50)    OVER(PARTITION BY usertype) AS percentile_50,
  PERCENTILE_CONT(tripduration_min, 0.75)    OVER(PARTITION BY usertype) AS percentile_75,
  PERCENTILE_CONT(tripduration_min, 0.99)    OVER(PARTITION BY usertype) AS percentile_99,
  MAX(tripduration_min)                      OVER(PARTITION BY usertype) AS max_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;

-- 10C. CLEAN: Apply three business-rule filters to remove outliers:
--      1. "Ghost Trips" — duration > 184 min (above 99th pct of Customers)
--      2. "System Errors" — duration < 2 min (likely failed/incomplete trips)
--      3. "False Starts" — < 5 min AND same start/end station
CREATE OR REPLACE TABLE `portfolio-projects-494318.citibike_trips.citibike_copy` AS
SELECT *
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`
WHERE
  tripduration_min <= 184        -- Rule 1: Remove ghost trips
  AND tripduration_min >= 2      -- Rule 2: Remove system errors
  AND NOT (                      -- Rule 3: Remove false starts
    start_station_id = end_station_id
    AND tripduration_min < 5
  );

-- 10D. VERIFY: Confirm the post-filter distribution is within expected bounds.
--      max_duration should now be ≤ 184; min_duration should be ≥ 2.
SELECT DISTINCT
  AVG(tripduration_min)                   OVER() AS mean_duration,
  MIN(tripduration_min)                   OVER() AS min_duration,
  PERCENTILE_CONT(tripduration_min, 0.25) OVER() AS percentile_25,
  PERCENTILE_CONT(tripduration_min, 0.50) OVER() AS percentile_50,
  PERCENTILE_CONT(tripduration_min, 0.75) OVER() AS percentile_75,
  PERCENTILE_CONT(tripduration_min, 0.99) OVER() AS percentile_99,
  MAX(tripduration_min)                   OVER() AS max_duration
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;

-- 10E. VERIFY: Additional boundary check — no rows should violate the filters.
--      Expected result: all counts = 0
SELECT
  COUNTIF(tripduration_min > 184)                                           AS ghost_trips_remaining,
  COUNTIF(tripduration_min < 2)                                             AS system_errors_remaining,
  COUNTIF(start_station_id = end_station_id AND tripduration_min < 5)       AS false_starts_remaining
FROM `portfolio-projects-494318.citibike_trips.citibike_copy`;
