BEGIN;

ALTER TABLE raw.airbnb_combined_raw

-- =====================
-- IDENTIFIER
-- =====================
ALTER COLUMN id TYPE BIGINT USING (
    CASE
        WHEN id IS NULL OR trim(id) = '' THEN NULL
        WHEN id ~ '^[0-9]+$' THEN id::BIGINT
        ELSE NULL
    END
),

ALTER COLUMN host_id TYPE BIGINT USING (
    CASE
        WHEN host_id IS NULL OR trim(host_id) = '' THEN NULL
        WHEN host_id ~ '^[0-9]+$' THEN host_id::BIGINT
        ELSE NULL
    END
),

-- =====================
-- STRING / CATEGORICAL
-- =====================
ALTER COLUMN name TYPE TEXT USING name::TEXT,
ALTER COLUMN host_name TYPE TEXT USING host_name::TEXT,
ALTER COLUMN neighbourhood_group TYPE TEXT USING neighbourhood_group::TEXT,
ALTER COLUMN neighbourhood TYPE TEXT USING neighbourhood::TEXT,
ALTER COLUMN room_type TYPE TEXT USING room_type::TEXT,

-- =====================
-- GEO
-- =====================
ALTER COLUMN latitude TYPE NUMERIC USING (
    CASE
        WHEN latitude IS NULL OR trim(latitude) = '' THEN NULL
        WHEN latitude ~ '^-?[0-9]+(\.[0-9]+)?$' THEN latitude::NUMERIC
        ELSE NULL
    END
),

ALTER COLUMN longitude TYPE NUMERIC USING (
    CASE
        WHEN longitude IS NULL OR trim(longitude) = '' THEN NULL
        WHEN longitude ~ '^-?[0-9]+(\.[0-9]+)?$' THEN longitude::NUMERIC
        ELSE NULL
    END
),

-- =====================
-- METRICS
-- =====================
ALTER COLUMN price TYPE NUMERIC USING (
    CASE
        WHEN price IS NULL OR trim(price) = '' THEN NULL
        WHEN price ~ '^[0-9]+(\.[0-9]+)?$' THEN price::NUMERIC
        ELSE NULL
    END
),

ALTER COLUMN minimum_nights TYPE INTEGER USING (
    CASE
        WHEN minimum_nights IS NULL OR trim(minimum_nights) = '' THEN NULL
        WHEN minimum_nights ~ '^[0-9]+$' THEN minimum_nights::INTEGER
        ELSE NULL
    END
),

ALTER COLUMN number_of_reviews TYPE INTEGER USING (
    CASE
        WHEN number_of_reviews IS NULL OR trim(number_of_reviews) = '' THEN NULL
        WHEN number_of_reviews ~ '^[0-9]+$' THEN number_of_reviews::INTEGER
        ELSE NULL
    END
),

ALTER COLUMN reviews_per_month TYPE NUMERIC USING (
    CASE
        WHEN reviews_per_month IS NULL OR trim(reviews_per_month) = '' THEN NULL
        WHEN reviews_per_month ~ '^[0-9]+(\.[0-9]+)?$' THEN reviews_per_month::NUMERIC
        ELSE NULL
    END
),

ALTER COLUMN calculated_host_listings_count TYPE INTEGER USING (
    CASE
        WHEN calculated_host_listings_count IS NULL OR trim(calculated_host_listings_count) = '' THEN NULL
        WHEN calculated_host_listings_count ~ '^[0-9]+$' THEN calculated_host_listings_count::INTEGER
        ELSE NULL
    END
),

ALTER COLUMN availability_365 TYPE INTEGER USING (
    CASE
        WHEN availability_365 IS NULL OR trim(availability_365) = '' THEN NULL
        WHEN availability_365 ~ '^[0-9]+$' THEN availability_365::INTEGER
        ELSE NULL
    END
),

-- =====================
-- DATE
-- =====================
ALTER COLUMN last_review TYPE DATE USING (
    CASE
        WHEN last_review IS NULL OR trim(last_review) = '' THEN NULL
        WHEN last_review ~ '^\d{4}-\d{2}-\d{2}$' THEN last_review::DATE
        ELSE NULL
    END
);

COMMIT;
