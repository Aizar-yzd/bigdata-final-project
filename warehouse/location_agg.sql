/*
========================================
FEATURE ENGINEERING - LOCATION AGGREGATION
TABLE : warehouse.fe_location_agg
========================================
*/

DROP TABLE IF EXISTS warehouse.fe_location_agg;

CREATE TABLE warehouse.fe_location_agg AS
SELECT
    neighbourhood_group,
    neighbourhood,

    COUNT(*) AS total_listings,
    AVG(price) AS avg_price,
    AVG(availability_365) AS avg_availability,
    SUM(number_of_reviews) AS total_reviews,

    -- FEATURE ENGINEERING
    CASE
        WHEN AVG(price) >= 300 THEN 'High Price Area'
        WHEN AVG(price) >= 150 THEN 'Medium Price Area'
        ELSE 'Low Price Area'
    END AS area_price_category

FROM staging.airbnb_cleaned_final
GROUP BY neighbourhood_group, neighbourhood;
