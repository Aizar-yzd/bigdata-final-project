/*
========================================
FEATURE ENGINEERING - LISTING LEVEL
TABLE : warehouse.fe_listing
========================================
*/

DROP TABLE IF EXISTS warehouse.fe_listing;

CREATE TABLE warehouse.fe_listing AS
SELECT
    id AS listing_id,

    host_id,
    neighbourhood_group,
    neighbourhood,
    room_type,

    price,
    minimum_nights,
    availability_365,
    number_of_reviews,
    reviews_per_month,

    -- FEATURE ENGINEERING
    CASE
        WHEN price IS NOT NULL AND price > 0
        THEN LN(price)
        ELSE NULL
    END AS price_log,

    CASE
        WHEN availability_365 IS NOT NULL
        THEN availability_365 / 365.0
        ELSE NULL
    END AS availability_ratio,

    CASE
        WHEN number_of_reviews > 0 AND reviews_per_month IS NOT NULL
        THEN reviews_per_month / number_of_reviews
        ELSE 0
    END AS review_intensity,

    CASE
        WHEN price >= 500 THEN 1
        ELSE 0
    END AS is_high_price_listing

FROM staging.airbnb_cleaned_final;
