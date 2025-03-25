# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

**following the guide in the videos, it's 3.3.2**

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)


## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

**code:**
`df = spark.read.option('header','true').parquet('yellow_tripdata_2024-10.parquet')`
`df.repartition(4)`
`df.write.parquet('subdir/')`

- 6MB
- 25MB
- 75MB **there was only 1 resulting parquet file. doing** `ls -lh dir/` **81 M so i will take this one.**
- 100MB 


## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

`from pyspark.sql import functions as F`

`df.select('tpep_pickup_datetime').filter(F.to_date(df.tpep_pickup_datetime) == '2024-10-15').count()`

- 85,567
- 105,567
- 125,567 **i got 128893 so this is the closest.**
- 145,567


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

`df = df.withColumn("time_diff_hours", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))/3600)`
`df.select(F.max("time_diff_hours")).show()`

- 122
- 142
- 162 **obtained solution of 162.617 hours, so 162 it is.**
- 182


## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
