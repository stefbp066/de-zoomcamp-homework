# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework. 

When your solution has SQL or shell commands and not code
(e.g. python files) file formad, include them directly in
the README file of your repository.


## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

Commands:
1. `docker run -it --entrypoint=bash python:3.12.8`
2. `pip show pip`
3. (For other packages installed in the Docker image eg Pandas: `[pkgname].__version__`)

- **24.3.1 ✅**
- 24.2.1
- 23.3.1
- 23.2.1


## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```
(5433:5432 has two ports, from the host machine port to the container port.\
Copy-paste the config into a .yml file, and run `docker compose up -d`)

**(From trying all options out after running the .yml file, the answer is localhost:5433 when using external PGAdmin.
However, accessing the container's own PGAdmin instance via http://localhost:8080/ and making a connection from there will yield the answer of postgres:5432.
Therefore, the answer is postgres:5432)**

- postgres:5433
- localhost:5432
- db:5433
- **postgres:5432 ✅**
- db:5432

(To remove this container, run in the CLI: `docker stop $(docker ps -a -q)` then `docker rm $(docker ps -a -q)`)

##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

**(THESE COMMANDS DO NOT WORK ON MAC OS WITHOUT HOMEBREW INSTALLATION OF WGET! NATIVE MAC OS APPROACH IS THE `curl` COMMAND, BUT SOMEHOW `curl -O https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz` DOES NOT WORK! IT IS BETTER TO DOWNLOAD MANUALLY.)**

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

**(Personally setup a Postgres container with command: `docker run -it -e POSTGRES_USER='postgres' -e POSTGRES_PASSWORD='postgres' -e POSTGRES_DB='ny_taxi' -v /[dir_for_container]:/var/lib/postgresql/data -p 5432:5432 postgres:17`) \
Advise to make own virtual environment for use of Pandas and SQLAlchemy**

## Question 3. Trip Segmentation Count

**Query used:** 
```
query = """
with trip_cte as 
(
    select
        trip_distance,
        CASE
            WHEN trip_distance < 1 THEN 'Up to 1 mile'
            WHEN trip_distance >= 1 AND trip_distance < 3 THEN 'In between 1 (exclusive) and 3 miles (inclusive)'
            WHEN trip_distance >= 3 AND trip_distance < 7 THEN 'In between 3 (exclusive) and 7 miles (inclusive)'
            WHEN trip_distance >= 7 AND trip_distance < 10 THEN 'In between 7 (exclusive) and 10 miles (inclusive)'
            ELSE 'Over 10 miles'
        END as trip_bin
    from green_tripdata_2019_10
    where lpep_pickup_datetime::timestamp >= '2019-10-01' and lpep_dropoff_datetime::timestamp < '2019-11-01'
)
select trip_bin, count(trip_bin) from trip_cte
group by trip_bin;
"""
pd.read_sql(query,con=engine)
```
**Answer is consistent with the 104,793; 201,407; 110,612; 27,831; 35,281 answer, even though the "up to 1 mile" answer is different. I obtained: 101,065; 201,407; 110,612; 27,831; 35,281.)**

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

Answers:

- 104,802;  197,670;  110,612;  27,831;  35,281
- 104,802;  198,924;  109,603;  27,678;  35,189
- **104,793;  201,407;  110,612;  27,831;  35,281 ✅**
- 104,793;  202,661;  109,603;  27,678;  35,189
- 104,838;  199,013;  109,645;  27,688;  35,202


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

**Query I used to answer:**
```
query = """
with longest_trip_day as 
(
    select date_trunc('day',lpep_pickup_datetime::timestamp)::date as pickup_day, max(trip_distance) as longest_trip
    from green_tripdata_2019_10
    group by date_trunc('day',lpep_pickup_datetime::timestamp)::date
)
select * from longest_trip_day
order by longest_trip desc
limit 1;
"""
pd.read_sql(query,con=engine)
```

- 2019-10-11 
- 2019-10-24
- 2019-10-26
- **2019-10-31 ✅ (trip distance found: 515.89, to verify)**


## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.

**Query I used:**
```
query = """
with pickup_location_total as 
(
    select sum(total_amount), "PULocationID"
    from green_tripdata_2019_10
    where date_trunc('day',lpep_pickup_datetime::timestamp)::date = '2019-10-18'
    group by "PULocationID"
    having sum(total_amount)>13000
)
select * from pickup_location_total;
"""
pd.read_sql(query,con=engine)
```
**Data does not have text corresponding to pickup location ID, so external searching is necessary: https://github.com/fivethirtyeight/uber-tlc-foil-response/blob/master/uber-trip-data/taxi-zone-lookup.csv**
- **East Harlem North, East Harlem South, Morningside Heights ✅ (code: 74, 75, 166. sum: 18686.68, 16797.26, 13029.79)**
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park


## Question 6. Largest tip

For the passengers picked up in Ocrober 2019 in the zone
name "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.
**Query used:**
```
# From Question 5, East Harlem North is ID 74.
query = """
with largest_tip_pickup as
(
    select max(tip_amount) as largest_tip, "DOLocationID"
    from green_tripdata_2019_10
    where "PULocationID"=74 and date_trunc('month',lpep_pickup_datetime::timestamp)::date = '2019-10-01'
    group by "DOLocationID"
)
select * from largest_tip_pickup
order by largest_tip desc
limit 1;
"""
pd.read_sql(query,con=engine)
# location ID 132 is JFK Airport.
```

- Yorkville West
- **JFK Airport ✅**
- East Harlem North
- East Harlem South


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-aprove, terraform destroy
- terraform init, terraform apply -auto-aprove, terraform destroy
- terraform import, terraform apply -y, terraform rm


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw1

```
docker run -it \
    -e POSTGRES_USER="postgres" \ 
    -e POSTGRES_PASSWORD="postres" \ 
    -e POSTGRES_DB="ny_taxi" \ 
    -v dtc_postgres_volume_local:/var/lib/postgresql/data \ 
    -p 5432:5432 \ 
    —network=pg-network \ 
    —name pg-database \ 
    postgres:17
```
