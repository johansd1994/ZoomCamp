
import os
import pandas as pd
import numpy as np
import argparse
from time import time
from sqlalchemy import create_engine
from psycopg2 import connect

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = '/output.csv.gz'
    else:
        csv_name = '/output.csv'

    os.system(f'wget {url} -O {csv_name}')

    #connect to postgres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    #send data batches of 100000 rows 
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)



    #convert tpep_pickup and tpep_dropoff of text to datetime
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)   
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)



    #create sql schema 
    print(pd.io.sql.get_schema(df, name=table_name, con=engine))




    #create columns in postgres 
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    #Send the first data batch
    df.to_sql(name=table_name, con=engine, if_exists='append')


    #condition to send all batches and the time it takes to ship them
    while True:
        t_start = time()
    
        df = next(df_iter)
    
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
        df.to_sql(name='green_taxi_trips', con=engine, if_exists='append')
    
        t_end = time()
    
        print('inserted another chunk,  took %.3f second' % (t_end - t_start))


if __name__ == '__main__':

    #analyzes command line elements
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    #User
    #Password
    #host
    #port
    #database
    #table name
    #url of the csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the result to')
    parser.add_argument('--url', help='url of the CSV file')

    args = parser.parse_args()

    main(args)




