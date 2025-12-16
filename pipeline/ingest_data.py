#!/usr/bin/env python
# coding: utf-8
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz

dtype = {
    "VendorID" : "Int64",
    "passenger_count": "Int64",
    "trip_distance" : "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag" : "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "trip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates=[
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

                            
@click.command()
@click.option('--pg-user', default='root', help='Postgres user')
@click.option('--pg-pass', default='root', help='Postgres password')
@click.option('--pg-host', default='localhost', help='Postgres host')
@click.option('--pg-port', default=5432, type=int, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', help='Postgres database')
@click.option('--year', default=2021, type=int, help='Year of data')
@click.option('--month', default=1, type=int, help='Month of data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunk-size', default=100000, type=int, help='CSV chunk size')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunk_size):
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
        
    url = f"{prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz"

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
                url, 
                dtype=dtype,
                parse_dates = parse_dates,
                iterator = True,
                chunksize = chunk_size
    )

    first = True
    
    for df_chunk in tqdm(df_iter):
            if first:
                df_chunk.head(0).to_sql(
                    name = target_table, 
                    con=engine, 
                    if_exists='replace'
                )
                first = False
                
            df_chunk.to_sql(name = target_table,
                            con = engine,
                            if_exists = 'append')

if __name__ == '__main__':
    run()