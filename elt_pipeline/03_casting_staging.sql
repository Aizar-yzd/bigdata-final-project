/*
====================================================
FILE        : 02_casting_staging.sql
PIPELINE    : ELT
STAGE       : Casting Data Types
SOURCE      : raw.airbnb_combined_raw
TARGET      : staging.airbnb_casted

DESKRIPSI:
Melakukan casting tipe data dari TEXT ke NUMERIC/DATE
tanpa melakukan cleaning nilai data.
Nilai yang tidak valid akan menjadi NULL.
====================================================
*/

-- Pastikan schema staging ada
CREATE SCHEMA IF NOT EXISTS staging;

-- Buat tabel staging dengan hasil casting
CREATE TABLE staging.airbnb_casted AS
SELECT
    id,
    name,
    host_id,
    host_name,
    neighbourhood_group,
    neighbourhood,
    room_type,

    -- Numeric casting (AMAN)
    CASE
        WHEN latitude ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN latitude::NUMERIC
        ELSE NULL
    END AS latitude,

    CASE
        WHEN longitude ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN longitude::NUMERIC
        ELSE NULL
    END AS longitude,

    CASE
        WHEN price ~ '^[0-9]+(\.[0-9]+)?$'
        THEN price::NUMERIC
        ELSE NULL
    END AS price,

    CASE
        WHEN minimum_nights ~ '^[0-9]+$'
        THEN minimum_nights::INTEGER
        ELSE NULL
    END AS minimum_nights,

    CASE
        WHEN number_of_reviews ~ '^[0-9]+$'
        THEN number_of_reviews::INTEGER
        ELSE NULL
    END AS number_of_reviews,

    CASE
        WHEN reviews_per_month ~ '^[0-9]+(\.[0-9]+)?$'
        THEN reviews_per_month::NUMERIC
        ELSE NULL
    END AS reviews_per_month,

    CASE
        WHEN calculated_host_listings_count ~ '^[0-9]+$'
        THEN calculated_host_listings_count::INTEGER
        ELSE NULL
    END AS calculated_host_listings_count,

    CASE
        WHEN availability_365 ~ '^[0-9]+$'
        THEN availability_365::INTEGER
        ELSE NULL
    END AS availability_365,

    -- Date casting
    CASE
        WHEN last_review ~ '^\d{4}-\d{2}-\d{2}$'
        THEN last_review::DATE
        ELSE NULL
    END AS last_review

FROM raw.airbnb_combined_raw;
