/*
====================================================
STAGE   : ELT - Final Cleaning
SOURCE  : raw.airbnb_combined_raw (hasil casting)
TARGET  : staging.airbnb_cleaned_final

Cleaning dilakukan berdasarkan hasil profiling:
- missing value
- duplikasi
- outlier logis & statistik
- konsistensi kategori
====================================================
*/

BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.airbnb_cleaned_final;

-- =================================================
-- 1. HITUNG THRESHOLD OUTLIER STATISTIK (P01 & P99)
-- =================================================
WITH stats AS (
    SELECT
        percentile_cont(0.01) WITHIN GROUP (ORDER BY price) AS price_p01,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY price) AS price_p99,
        percentile_cont(0.01) WITHIN GROUP (ORDER BY reviews_per_month) AS rpm_p01,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY reviews_per_month) AS rpm_p99
    FROM raw.airbnb_combined_raw
),

-- =================================================
-- 2. CLEANING NILAI (LOGIS + STATISTIK)
-- =================================================
cleaned AS (
    SELECT
        id,
        name,

        host_id,
        COALESCE(host_name, 'Unknown') AS host_name,

        COALESCE(neighbourhood_group, 'Unknown') AS neighbourhood_group,
        neighbourhood,

        latitude,
        longitude,

        INITCAP(TRIM(room_type)) AS room_type,

        -- PRICE
        CASE
            WHEN price <= 0 THEN NULL
            WHEN price < stats.price_p01 THEN stats.price_p01
            WHEN price > stats.price_p99 THEN stats.price_p99
            ELSE price
        END AS price,

        -- MIN NIGHTS
        CASE
            WHEN minimum_nights <= 0 THEN NULL
            ELSE minimum_nights
        END AS minimum_nights,

        -- NUMBER OF REVIEWS
        CASE
            WHEN number_of_reviews < 0 THEN NULL
            ELSE number_of_reviews
        END AS number_of_reviews,

        -- LAST REVIEW
        CASE
            WHEN number_of_reviews = 0 THEN NULL
            ELSE last_review
        END AS last_review,

        -- REVIEWS PER MONTH
        CASE
            WHEN number_of_reviews = 0 THEN 0
            WHEN reviews_per_month < stats.rpm_p01 THEN stats.rpm_p01
            WHEN reviews_per_month > stats.rpm_p99 THEN stats.rpm_p99
            ELSE reviews_per_month
        END AS reviews_per_month,

        calculated_host_listings_count,

        CASE
            WHEN availability_365 > 365 THEN NULL
            ELSE availability_365
        END AS availability_365

    FROM raw.airbnb_combined_raw, stats
),

-- =================================================
-- 3. DEDUPLICATION
-- =================================================
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY
                last_review DESC NULLS LAST,
                number_of_reviews DESC NULLS LAST
        ) AS rn
    FROM cleaned
)

-- =================================================
-- 4. HASIL FINAL
-- =================================================
SELECT
    id,
    name,
    host_id,
    host_name,
    neighbourhood_group,
    neighbourhood,
    latitude,
    longitude,
    room_type,
    price,
    minimum_nights,
    number_of_reviews,
    last_review,
    reviews_per_month,
    calculated_host_listings_count,
    availability_365
INTO staging.airbnb_cleaned_final
FROM ranked
WHERE rn = 1;

COMMIT;
