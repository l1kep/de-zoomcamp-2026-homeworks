/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: trip_date
    type: date
    description: "Date of the trip"
    primary_key: true
  - name: taxi_type
    type: string
    description: "Type of taxi service"
    primary_key: true
  - name: payment_type_name
    type: string
    description: "Payment type"
    primary_key: true
  - name: total_trips
    type: integer
    description: "Total number of trips"
    checks:
      - name: not_null
      - name: non_negative
  - name: total_passengers
    type: integer
    description: "Total passengers across all trips"
    checks:
      - name: non_negative
  - name: avg_passengers_per_trip
    type: float
    description: "Average passengers per trip"
    checks:
      - name: non_negative
  - name: total_distance
    type: float
    description: "Total trip distance in miles"
    checks:
      - name: non_negative
  - name: avg_distance_per_trip
    type: float
    description: "Average distance per trip in miles"
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: "Total fare amount in USD"
    checks:
      - name: not_null
  - name: avg_fare_per_trip
    type: float
    description: "Average fare per trip in USD"
    checks:
      - name: not_null
  - name: total_amount
    type: float
    description: "Total amount charged in USD"
    checks:
      - name: not_null
  - name: avg_total_amount_per_trip
    type: float
    description: "Average total amount per trip in USD"
    checks:
      - name: not_null

@bruin */

SELECT
  DATE_TRUNC('day', pickup_datetime::TIMESTAMP) AS trip_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS total_trips,
  SUM(passenger_count) AS total_passengers,
  AVG(passenger_count) AS avg_passengers_per_trip,
  SUM(trip_distance) AS total_distance,
  AVG(trip_distance) AS avg_distance_per_trip,
  SUM(fare_amount) AS total_fare_amount,
  AVG(fare_amount) AS avg_fare_per_trip,
  SUM(total_amount) AS total_amount,
  AVG(total_amount) AS avg_total_amount_per_trip
FROM staging.trips
WHERE pickup_datetime::TIMESTAMP >= '{{ start_datetime }}'
  AND pickup_datetime::TIMESTAMP < '{{ end_datetime }}'
GROUP BY
  DATE_TRUNC('day', pickup_datetime::TIMESTAMP),
  taxi_type,
  payment_type_name
ORDER BY
  trip_date DESC,
  taxi_type,
  payment_type_name