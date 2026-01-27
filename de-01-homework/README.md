# How to start
- Create infrastructure for homework
```bash
docker compose up
```
- Ingestion data to pg
```bash
uv sync
uv run python ingest_data.py --pg-port 5433 --pg-host localhost 
```
- Use pgadmin for execution sql-queries (http://localhost:8080/browser/)

# Questions

## Question 3. Counting short trips

```sql
select 
	count(1)
from
	public.dim_taxi_data
where 
		lpep_pickup_datetime >= '2025-11-01'
	and lpep_pickup_datetime < '2025-12-01'
	and trip_distance <= 1
```

## Question 4. Longest trip for each day

```sql
with get_longest_grip as (
	select 
		  lpep_pickup_datetime
		, row_number() over(order by trip_distance desc) as rn 
	from
		public.dim_taxi_data
	where 
		trip_distance < 100
)

select
	lpep_pickup_datetime
from 
	get_longest_grip
where
	rn = 1
```

## Question 5. Biggest pickup zone
### The answer can have several answers and the limit opetation may give false answer

```sql
select 
	  z."Zone"
	, sum(total_amount)
from
	public.dim_taxi_data as t
left join 
	public.dim_zones as z
		on z."LocationID" = t."PULocationID"
where 
		lpep_pickup_datetime >= '2025-11-18'
	and lpep_pickup_datetime < '2025-12-19'
group by 
	z."Zone"
order by 
	sum(total_amount) desc
limit 1
```

## Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? (1 point)

```sql
with largest_tip as (
	select 
		  doz."Zone"
		, row_number() over(order by tip_amount desc) as rn
	from
		public.dim_taxi_data as t
	left join 
		public.dim_zones as puz
			on puz."LocationID" = t."PULocationID"
	left join 
		public.dim_zones as doz
			on doz."LocationID" = t."DOLocationID"
	where 
			lpep_pickup_datetime >= '2025-11-01'
		and lpep_pickup_datetime < '2025-12-01'
		and puz."Zone" = 'East Harlem North'
)

select
	*
from 
	largest_tip
where 
	rn = 1
```