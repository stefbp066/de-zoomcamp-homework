-- QUESTION 1
select count(VendorID) from de_zoomcamp_datasets.yellow_2024;

-- QUESTION 2
-- external table query
select count(distinct VendorID) from de_zoomcamp_datasets.yellow_2024; -- process 0 B, due to processing an external table

-- internal table query
select count(distinct VendorID) from de_zoomcamp_datasets.yellow_2024_nonpart; -- process 155.12 MB

-- QUESTION 3
select PULocationID from de_zoomcamp_datasets.yellow_2024_nonpart; -- process 155.12 MB
select PULocationID, DOLocationID from de_zoomcamp_datasets.yellow_2024_nonpart; -- process 310.24 MB

-- QUESTION 4
select count(VendorID) from de_zoomcamp_datasets.yellow_2024 where fare_amount=0;

-- QUESTION 5: theory, but a partitioned table will be needed for QUESTION 6.

-- QUESTION 6
select count(distinct VendorID) from `de_zoomcamp_datasets.yellow_2024_nonpart` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'; -- process 310.24 MB
select count(distinct VendorID) from `de_zoomcamp_datasets.yellow_2024_part_datetime` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'; -- process 26.86 MB

-- QUESTION 7, QUESTION 8: theory.
-- QUESTION 9
select count(*) from `de_zoomcamp_datasets.yellow_2024_nonpart`;
