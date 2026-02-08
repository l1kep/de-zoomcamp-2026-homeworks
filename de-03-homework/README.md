# How to start
- Install dependencies with uv and write env path value to gcp account
```bash
uv sync
echo "CRED_PATH=''{path_to_your_cred_json''" > .env
```
- Run injestion script
```bash
uv run --env-file .env python load_yelllow_taxi_to_gcp.py
```

- Create Dataset in BigQuery, for example 'de_education'
- Preparing external and materialized table, 'yellow_taxy_trip_2024_ext' and 'yellow_taxy_trip_2024'
```sql
CREATE OR REPLACE EXTERNAL TABLE de_education.yellow_taxy_trip_2024_ext
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://test_backet_gsc/*.parquet']
);

CREATE OR REPLACE TABLE de_education.yellow_taxy_trip_2024 AS
SELECT * FROM de_education.yellow_taxy_trip_2024_ext;
```
# Questions

# Q1
```sql
select 
  count(1) 
from 
  de_education.yellow_taxy_trip_2024;
```

# Q2
```sql
select 
  count(distinct PULocationID) 
from 
  de_education.yellow_taxy_trip_2024;

select 
  count(distinct PULocationID) 
from 
  de_education.yellow_taxy_trip_2024_ext;
```
## Q3
```sql
select 
  PULocationID
from 
  de_education.yellow_taxy_trip_2024;

select 
    PULocationID
  , DOLocationID
from 
  de_education.yellow_taxy_trip_2024;
```

## Q4
```sql
select 
  count(1)
from 
  de_education.yellow_taxy_trip_2024
where 
  fare_amount = 0;
```

## Q5
```sql
CREATE OR REPLACE TABLE de_education.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de_education.yellow_taxy_trip_2024;
```

## Q6
```sql
select 
  count(distinct VendorID)
from 
  de_education.yellow_taxy_trip_2024
where 
  tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';

select 
  count(distinct VendorID)
from 
  de_education.yellow_tripdata_partitioned_clustered
where 
  tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';
```

## Q7
 
GCP_BUCKET

## Q8

NO

## Q9
```sql
select 
  *
from 
  de_education.yellow_taxy_trip_2024
```