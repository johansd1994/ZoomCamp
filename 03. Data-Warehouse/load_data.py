import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    urls = [ 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet',
             'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet'
    ]
    #'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz'
    
    dfs = []

    for url in urls:

        df = pd.read_parquet(url)
        dfs.append(df)

    df = pd.concat(dfs)

    df_final = df.to_csv('concat.csv', index=False)

    taxi_dtypes={
                'VendorID' : pd.Int64Dtype(),
                'store_and_fwd_flag' : str, 
                'RatecodeID' : float,
                'PULocationID' : pd.Int64Dtype(),  
                'DOLocationID' : pd.Int64Dtype(),  
                'passenger_count' : float,
                'trip_distance' :  float,
                'fare_amount' : float,
                'extra' : float,
                'mta_tax' : float,
                'tip_amount' :float,
                'tolls_amount' :float,
                'ehail_fee' :float,
                'improvement_surcharge' : float,
                'total_amount' :float,
                'payment_type' : float,
                'trip_type' : float,
                'congestion_surcharge' : float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    df_final = pd.read_csv('concat.csv', sep=",", dtype=taxi_dtypes, parse_dates=parse_dates)
    return df_final

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
