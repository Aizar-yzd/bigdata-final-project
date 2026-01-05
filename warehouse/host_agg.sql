/*
========================================
FEATURE ENGINEERING - HOST AGGREGATION
TABLE : warehouse.fe_host_agg
========================================
*/

DROP TABLE IF EXISTS warehouse.fe_host_agg;

CREATE TABLE warehouse.fe_host_agg AS
SELECT
    host_id,

    COUNT(*) AS total_listings,
    AVG(price) AS avg_price,
    SUM(number_of_reviews) AS total_reviews,
    AVG(availability_365) AS avg_availability,

    -- FEATURE ENGINEERING
    CASE
        WHEN COUNT(*) >= 10 THEN 'High Volume Host'
        WHEN COUNT(*) >= 3 THEN 'Medium Volume Host'
        ELSE 'Low Volume Host'
    END AS host_volume_category,

    CASE
        WHEN SUM(number_of_reviews) >= 100 THEN 1
        ELSE 0
    END AS is_popular_host

FROM staging.airbnb_cleaned_final
GROUP BY host_id;
