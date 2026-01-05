/*
FILE    : 03_cleaning.sql
PURPOSE : Cleaning data hasil merge (ELT) -> buat table staging.airbnb_cleaned
AUTHOR  : Generated for project BigData Final (UAS)
NOTES   :
 - Input  : raw.airbnb_combined_raw
 - Output : staging.airbnb_cleaned
 - Prinsip ELT: tidak "mencipta" data baru; invalid values diset NULL.
 - Deduplikasi: keep record per id dengan last_review paling baru, kemudian number_of_reviews terbesar.
 - Jalankan di database bigdata_airbnb.
*/

BEGIN;

-- 1) Pastikan schema staging & warehouse ada
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- 2) Tabel metadata cleaning (buat jika belum ada)
CREATE TABLE IF NOT EXISTS warehouse.metadata_cleaning (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMP DEFAULT now(),
    source_table TEXT,
    target_table TEXT,
    total_input_rows BIGINT,
    total_output_rows BIGINT,
    note TEXT
);

-- 3) Buat tabel cleaned (drop jika sudah ada agar idempotent)
DROP TABLE IF EXISTS staging.airbnb_cleaned;

-- 4) Cleaning rules + deduplication:
--    - Normalisasi text (trim)
--    - Set nilai tidak valid menjadi NULL:
--        * price <= 0 -> NULL
--        * minimum_nights <= 0 -> NULL
--        * availability_365 not in [0..365] -> NULL
--        * latitude not in [-90,90] -> NULL
--        * longitude not in [-180,180] -> NULL
--        * reviews_per_month < 0 -> NULL
--        * number_of_reviews < 0 -> NULL
--        * last_review in future -> NULL
--    - Dedupe by id: keep row with newest last_review (NULLS LAST), tie-breaker number_of_reviews desc
--    - Semua kolom final diberi tipe yang sesuai

CREATE TABLE staging.airbnb_cleaned AS
WITH raw_prep AS (
    SELECT
        -- Maintain original id if possible, convert to bigint if already numeric or cast-able
        CASE
            WHEN id IS NULL OR trim(id::text) = '' THEN NULL
            WHEN id::text ~ '^[0-9]+$' THEN id::bigint
            ELSE NULL
        END AS id,

        NULLIF(trim(name::text), '')::text AS name,
        
        CASE
            WHEN host_id IS NULL OR trim(host_id::text) = '' THEN NULL
            WHEN host_id::text ~ '^[0-9]+$' THEN host_id::bigint
            ELSE NULL
        END AS host_id,

        NULLIF(trim(host_name::text), '')::text AS host_name,
        NULLIF(trim(neighbourhood_group::text), '')::text AS neighbourhood_group,
        NULLIF(trim(neighbourhood::text), '')::text AS neighbourhood,

        -- latitude/longitude numeric: keep valid range else NULL
        CASE
            WHEN latitude IS NULL THEN NULL
            WHEN latitude::text ~ '^-?[0-9]+(\.[0-9]+)?$' AND (latitude::numeric BETWEEN -90 AND 90) THEN latitude::numeric
            ELSE NULL
        END AS latitude,

        CASE
            WHEN longitude IS NULL THEN NULL
            WHEN longitude::text ~ '^-?[0-9]+(\.[0-9]+)?$' AND (longitude::numeric BETWEEN -180 AND 180) THEN longitude::numeric
            ELSE NULL
        END AS longitude,

        -- Normalize room_type text (trim + title case)
        NULLIF(initcap(trim(room_type::text)), '')::text AS room_type,

        -- price numeric, invalid or <=0 -> NULL
        CASE
            WHEN price IS NULL OR trim(price::text) = '' THEN NULL
            WHEN price::text ~ '^-?[0-9]+(\.[0-9]+)?$' AND (price::numeric > 0) THEN price::numeric
            ELSE NULL
        END AS price,

        -- integer fields
        CASE
            WHEN minimum_nights IS NULL OR trim(minimum_nights::text) = '' THEN NULL
            WHEN minimum_nights::text ~ '^[0-9]+$' AND (minimum_nights::int > 0) THEN minimum_nights::int
            ELSE NULL
        END AS minimum_nights,

        CASE
            WHEN number_of_reviews IS NULL OR trim(number_of_reviews::text) = '' THEN NULL
            WHEN number_of_reviews::text ~ '^[0-9]+$' AND (number_of_reviews::int >= 0) THEN number_of_reviews::int
            ELSE NULL
        END AS number_of_reviews,

        -- last_review: valid date format expected in table; if in future -> NULL
        CASE
            WHEN last_review IS NULL OR trim(last_review::text) = '' THEN NULL
            WHEN (last_review::date <= current_date) THEN last_review::date
            ELSE NULL
        END AS last_review,

        CASE
            WHEN reviews_per_month IS NULL OR trim(reviews_per_month::text) = '' THEN NULL
            WHEN reviews_per_month::text ~ '^[0-9]+(\.[0-9]+)?$' AND (reviews_per_month::numeric >= 0) THEN reviews_per_month::numeric
            ELSE NULL
        END AS reviews_per_month,

        CASE
            WHEN calculated_host_listings_count IS NULL OR trim(calculated_host_listings_count::text) = '' THEN NULL
            WHEN calculated_host_listings_count::text ~ '^[0-9]+$' THEN calculated_host_listings_count::int
            ELSE NULL
        END AS calculated_host_listings_count,

        CASE
            WHEN availability_365 IS NULL OR trim(availability_365::text) = '' THEN NULL
            WHEN availability_365::text ~ '^[0-9]+$' AND (availability_365::int BETWEEN 0 AND 365) THEN availability_365::int
            ELSE NULL
        END AS availability_365,

        -- keep a copy of original row for tie-breaker or audit if needed (optional)
        row_to_json(raw.*) AS raw_row_json,
        -- preserve original last_review_text for audit
        raw.last_review::text AS original_last_review_text

    FROM raw.airbnb_combined_raw raw
),

-- 5) Deduplicate: choose best row per id
ranked AS (
    SELECT
        rp.*,
        ROW_NUMBER() OVER (
            PARTITION BY rp.id
            ORDER BY
                -- newest last_review first (NULLS LAST),
                CASE WHEN rp.last_review IS NULL THEN '1900-01-01'::date ELSE rp.last_review END DESC,
                rp.number_of_reviews DESC NULLS LAST
        ) AS rn
    FROM raw_prep rp
)

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
    availability_365,
    raw_row_json,
    original_last_review_text
FROM ranked
WHERE rn = 1
;

-- 6) Summarize metadata: insert counts into warehouse.metadata_cleaning
INSERT INTO warehouse.metadata_cleaning(
    source_table,
    target_table,
    total_input_rows,
    total_output_rows,
    note
)
SELECT
    'raw.airbnb_combined_raw' AS source_table,
    'staging.airbnb_cleaned' AS target_table,
    (SELECT COUNT(*) FROM raw.airbnb_combined_raw) AS total_input_rows,
    (SELECT COUNT(*) FROM staging.airbnb_cleaned) AS total_output_rows,
    'Cleaning: logical nulling & deduplication. Invalid numeric/date -> NULL; duplicates deduped by last_review & number_of_reviews.'
;


COMMIT;

