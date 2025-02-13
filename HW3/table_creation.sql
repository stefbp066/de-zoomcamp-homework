CREATE OR REPLACE EXTERNAL TABLE de_zoomcamp_datasets.yellow_2024
OPTIONS (
  format = "PARQUET",
  uris = ["gs://[GCP bucket name]/yellow_tripdata_2024-*.parquet"]
);

CREATE OR REPLACE TABLE [GCP project ID].de_zoomcamp_datasets.yellow_2024_nonpart AS
SELECT * FROM de_zoomcamp_datasets.yellow_2024;

-- for QUESTION 5
CREATE OR REPLACE TABLE [GCP project ID].de_zoomcamp_datasets.yellow_2024_part_datetime
PARTITION BY
  DATE(tpep_pickup_datetime) 
CLUSTER BY 
  VendorID
AS (SELECT * FROM [GCP project ID].de_zoomcamp_datasets.yellow_2024);
