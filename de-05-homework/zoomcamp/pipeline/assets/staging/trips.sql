/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone where the meter was engaged"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone where the meter was disengaged"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Base fare in USD"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi service (yellow or green)"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type name"
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: "Number of passengers in the vehicle"
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Total amount charged to passengers"
    checks:
      - name: not_null


@bruin */

WITH deduplicated_trips AS (
  SELECT
    COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS pickup_datetime,
    COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS dropoff_datetime,
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
    fare_amount,
    taxi_type,
    payment_type,
    passenger_count,
    trip_distance,
    total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY 
        COALESCE(tpep_pickup_datetime, lpep_pickup_datetime),
        COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime),
        pu_location_id,
        do_location_id,
        fare_amount
      ORDER BY extracted_at DESC
    ) AS row_num
  FROM ingestion.trips
  WHERE COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) >= '{{ start_datetime }}'
    AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) < '{{ end_datetime }}'
    AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) IS NOT NULL
    AND COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) IS NOT NULL
    AND fare_amount IS NOT NULL
    AND payment_type IS NOT NULL
)

SELECT
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  fare_amount,
  taxi_type,
  pl.payment_type_name,
  passenger_count,
  trip_distance,
  total_amount
FROM deduplicated_trips
INNER JOIN ingestion.payment_lookup pl ON deduplicated_trips.payment_type = pl.payment_type_id
WHERE row_num = 1
