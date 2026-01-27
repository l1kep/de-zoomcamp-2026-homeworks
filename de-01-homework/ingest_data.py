import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dataset_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'

dataset_zones_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

@click.command()
@click.option('--pg-user', default='postgres', help='PostgreSQL user')
@click.option('--pg-pass', default='postgres', help='PostgreSQL password')
@click.option('--pg-host', default='postgres', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db):
    print('HELLO')
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_zones = pd.read_csv(
        dataset_zones_url,
        dtype=dtype,
        #parse_dates=parse_dates,
        #iterator=True,
        #chunksize=10000
    )
    
    df_zones.to_sql(
        name = 'dim_zones',
        con=engine,
        if_exists='replace',
    )

    df = pd.read_parquet(
        dataset_url,
        #dtype=dtype,
        #parse_dates=parse_dates,
        #iterator=True,
        #chunksize=10000
    )

    df.to_sql(
        name = 'dim_taxi_data',
        con=engine,
        if_exists='replace',
    )


    '''
    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print("Table created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print("Inserted first chunk:", len(first_chunk))

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print("Inserted chunk:", len(df_chunk))
    '''
if __name__ == '__main__':
    run()

    #print(df)
