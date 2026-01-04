CREATE TABLE raw.airbnb_combined_raw AS

-- DATASET 1: AB_US_2020
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
FROM raw.ab_us_2020_raw

UNION ALL

-- DATASET 2: Airbnb_Open_Data
SELECT
    id,
    "NAME" AS name,
    "host id" AS host_id,
    "host name" AS host_name,
    "neighbourhood group" AS neighbourhood_group,
    neighbourhood,
    lat AS latitude,
    long AS longitude,
    "room type" AS room_type,
    price,
    "minimum nights" AS minimum_nights,
    "number of reviews" AS number_of_reviews,
    "last review" AS last_review,
    "reviews per month" AS reviews_per_month,
    "calculated host listings count" AS calculated_host_listings_count,
    "availability 365" AS availability_365
FROM raw.airbnb_open_data_raw;
