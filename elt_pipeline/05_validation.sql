-- cek jumlah baris
SELECT COUNT(*) FROM staging.airbnb_cleaned_final;

-- pastikan tidak ada outlier logis
SELECT COUNT(*) FROM staging.airbnb_cleaned_final WHERE price <= 0;
SELECT COUNT(*) FROM staging.airbnb_cleaned_final WHERE availability_365 > 365;

-- cek konsistensi review
SELECT COUNT(*)
FROM staging.airbnb_cleaned_final
WHERE number_of_reviews = 0 AND last_review IS NOT NULL;
