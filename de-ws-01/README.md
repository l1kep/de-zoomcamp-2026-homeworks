# How to start
- Install dependencies with uv and run ETL to duckdb
```bash
uv sync
uv run python taxi_pipeline.py
```

# Questions

# Q1
```sql
select 
	  min(trip_dropoff_date_time)
	, max(trip_dropoff_date_time)
from taxi_pipeline.taxi_data.trips 
```

# Q2
```sql
select 
	distinct payment_type
from taxi_pipeline.taxi_data.trips 

select 
	count(if(payment_type = 'Credit', 1, null)) / count(1) * 100
from taxi_pipeline.taxi_data.trips 
```

## Q3
```sql
select 
	sum(tip_amt)
from taxi_pipeline.taxi_data.trips 
```