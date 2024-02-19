from google.cloud import bigquery
from google.cloud.bigquery.client import Client

client = bigquery.Client()

table_id = 'encoded-ensign-412700.ny_taxi.green_taxi_data_2022'

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.sourceformat.parquet
)

uri = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))