## Module 3 Homework

<b><u>Important Note:</b></u> <p> For this homework we will be using the Yellow Taxi Trip Records for **January 2024 - June 2024 NOT the entire year of data** 
Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>

**personal notes: first tried to run the `load_yellow_taxi_data.py` file but could not make it work; i had to `pip install google-cloud google-cloud-vision google-cloud-storage`. unknown exactly why this is the case, but if it works then it works.
then tried to run `bq load --source_format=PARQUET --autodetect --noreplace [BQ dataset].[BQ table] gs://[GCP bucket name]/yellow_tripdata_2024-*.parquet` (https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet). the `--noreplace` is for appending. but this only works for native tables, so i will ignore that the external table instruction exists (it may be important to know why this instruction was made in the first place).**

## Question 1:
Question 1: What is count of records for the 2024 Yellow Taxi Data?
**note: in the bigquery part of the gcp console, click the bigquery dataset and then the bigquery table, then click `DETAILS`. check the `Storage info` subsection, check `Number of rows`.**
- 65,623
- 840,402
- 20,332,093 ✅
- 85,431,289

**as i do this, i realize that it's actually possible to do the sql query this way:**
`CREATE OR REPLACE EXTERNAL TABLE [BQ dataset name].[BQ table name within BQ dataset]
OPTIONS (
  format = "PARQUET",
  uris = ["gs://[GCS bucket name]/yellow_tripdata_2024-*.parquet"]
);`

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

**in this case, i got the numbers from just an internal table. unclear if the materialized table is the same as an internal table, or could be something else, since BQ docu has "materialized view" and not a "materialized table".**

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table ✅
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

## Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed. ✅
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

**it seems like BigQuery processes only the columns mentioned in the query, owing to its columnar nature. it doesnt seem that it scans the table twice (option 2 unlikely). option 3 implies that the size of the file being processed is the same, which is not the case. option 4 can be eliminated since from my learning, implicit join queries contain `... where a.id=b.id` and we are just doing normal select.**

## Question 4:
How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- 8,333 ✅

**query used on external table:**
`select count(VendorID) 
from de_zoomcamp_datasets.yellow_2024
where fare_amount=0;`

## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

**partitioning seems to work best by going over features such as timestamp data, while clustering works better by grouping high-cardinality features e.g. IDs. therefore,**
- Partition by tpep_dropoff_datetime and Cluster on VendorID ✅
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID


## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

**query for making the partitioned and clustered table:**
`CREATE OR REPLACE TABLE global-env-447720-j9.de_zoomcamp_datasets.yellow_2024_part_datetime
PARTITION BY
  DATE(tpep_pickup_datetime) 
CLUSTER BY 
  VendorID
AS (SELECT * FROM global-env-447720-j9.de_zoomcamp_datasets.yellow_2024);`

**query for the partitioned and clustered table:**
`select count(distinct VendorID) from `de_zoomcamp_datasets.yellow_2024_part_datetime` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';`
**the above query would be processing 26.86 MB so i assume the second choice is correct. on to the non-partitioned native table.**

**query for non-partitioned native table:**
``

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table


## Question 7: 
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- GCP Bucket
- Big Table

## Question 8:
It is best practice in Big Query to always cluster your data:
- True
- False


## (Bonus: Not worth points) Question 9:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?


## Submitting the solutions

Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw3
