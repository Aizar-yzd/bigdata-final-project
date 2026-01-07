/*
====================================================
FILE    : 05_load_to_warehouse.sql
STAGE   : ELT - Load to Data Warehouse
SOURCE  : staging.airbnb_cleaned_final
TARGET  : warehouse.fact_airbnb
====================================================
*/

BEGIN;

CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.fact_airbnb;

CREATE TABLE warehouse.fact_airbnb AS
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
FROM staging.airbnb_cleaned_final;

COMMIT;
