"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
    description: "A code indicating the TPEP provider that provided the record"
  - name: tpep_pickup_datetime
    type: timestamp
    description: "The date and time when the meter was engaged"
  - name: tpep_dropoff_datetime
    type: timestamp
    description: "The date and time when the meter was disengaged"
  - name: passenger_count
    type: integer
    description: "The number of passengers in the vehicle"
  - name: trip_distance
    type: float
    description: "The elapsed trip distance in miles"
  - name: RatecodeID
    type: integer
    description: "The final rate code in effect at the end of the trip"
  - name: store_and_fwd_flag
    type: string
    description: "This flag indicates whether the trip record was held in vehicle memory"
  - name: PULocationID
    type: integer
    description: "TLC Taxi Zone in which the taximeter was engaged"
  - name: DOLocationID
    type: integer
    description: "TLC Taxi Zone in which the taximeter was disengaged"
  - name: payment_type
    type: integer
    description: "A numeric code signifying how the passenger paid for the trip"
  - name: fare_amount
    type: float
    description: "The time-and-distance fare calculated by the meter"
  - name: extra
    type: float
    description: "Miscellaneous extras and surcharges"
  - name: mta_tax
    type: float
    description: "Tax that is automatically triggered based on the metered rate in use"
  - name: tip_amount
    type: float
    description: "Tip amount"
  - name: tolls_amount
    type: float
    description: "Total amount of all tolls paid in trip"
  - name: improvement_surcharge
    type: float
    description: "Improvement surcharge assessed trips"
  - name: total_amount
    type: float
    description: "Total amount charged to passengers"
  - name: congestion_surcharge
    type: float
    description: "Total amount collected in trip for congestion surcharge"
  - name: airport_fee
    type: float
    description: "Airport fee for trips that start at JFK or LaGuardia airports"
  - name: taxi_type
    type: string
    description: "Type of taxi service (yellow or green)"
  - name: extracted_at
    type: string
    description: "Timestamp when the data was extracted"

@bruin"""

import os
import pandas as pd
import requests
from datetime import datetime
from dateutil import rrule
import json


def materialize():
    """
    Ingest NYC taxi trip data from TLC public endpoint.
    
    Uses Bruin runtime context:
    - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
    - BRUIN_VARS JSON with taxi_types configuration
    """
    # Get date range from environment
    start_date = os.environ.get('BRUIN_START_DATE')
    end_date = os.environ.get('BRUIN_END_DATE')
    
    if not start_date or not end_date:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")
    
    # Get taxi_types from pipeline variables
    bruin_vars = os.environ.get('BRUIN_VARS', '{}')
    vars_dict = json.loads(bruin_vars)
    taxi_types = vars_dict.get('taxi_types', ['yellow', 'green'])
    
    # Base URL for NYC taxi data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Generate list of months to fetch
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Get all months in the range
    months = []
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_dt, until=end_dt):
        months.append(dt)
    
    all_dataframes = []
    
    # Fetch data for each taxi type and month
    for taxi_type in taxi_types:
        for month_dt in months:
            year = month_dt.year
            month = month_dt.month
            
            # Construct filename
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = base_url + filename
            
            print(f"Fetching: {url}")
            
            try:
                # Download and read parquet file
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # Read parquet data
                df = pd.read_parquet(url)
                
                # Convert timestamp columns to strings to avoid pyarrow timezone issues
                for col in df.columns:
                    if df[col].dtype.kind == 'M':  # M = datetime64
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Add taxi_type column
                df['taxi_type'] = taxi_type
                
                # Add extracted_at as string to avoid pyarrow timezone issues
                df['extracted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                all_dataframes.append(df)
                print(f"  Loaded {len(df)} rows")
                
            except Exception as e:
                print(f"  Error fetching {filename}: {e}")
                continue
    
    # Combine all dataframes
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        print(f"Total rows ingested: {len(final_df)}")
        return final_df
    else:
        print("No data was fetched")
        return pd.DataFrame()