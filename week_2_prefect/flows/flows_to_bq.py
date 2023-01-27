from prefect import task, flow
from pathlib import Path
import os
import pandas as pd
from prefect_gcp import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse, bigquery_load_cloud_storage, bigquery_create_table
from prefect_gcp import GcpCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from io import BytesIO

BUCKET_NAME = "dzc-trietle-data-lake"

'''
 # TODO:
 [x] Fetch raw data from GCS
 [x] Transform data
 [x] Upload cleaned data from GCS
 [x] Load to BQ
'''

def get_config(path: Path):
    external_config =  bigquery.ExternalConfig(source_format='PARQUET')
    external_config = dict(
        autodetect=True,
        source_uris=[
            f"gs://{BUCKET_NAME}/{path}"
        ]
    )
    return external_config

@task()
def fetch_from_gcs(color: str, dataset_file: str):
    ## Approach 1: Download parquet file to local
    # path = gcs_block.download_object_to_path(
    #     from_path=gcs_path,
    #     to_path=f"data/{color}/test_{dataset_file}.parquet"
    # )
    # return path

    # Approach 2: Read parquet file(bytes return type) and serialize and read to dataframe directly!
    gcs_path = f"data/raw/{color}/{dataset_file}.parquet"
    gcs_block = GcsBucket.load("decamp-gcs-bucket")
    
    df = pd.read_parquet(BytesIO(gcs_block.read_path(gcs_path)))
    return df

@task()
def transform(df):
    """
        Data wrangling and cast datetime type
    """
    print("N/A record: ", df.isna().sum())
    df.fillna(0, inplace=True)
    print("N/A record after filled: ", df.isna().sum())
    
    print(f"Pickup and dropoff datatype: {df['lpep_pickup_datetime'].dtype}, {df['lpep_pickup_datetime'].dtype}")
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    print(f"Pickup and dropoff datatype: {df['lpep_pickup_datetime'].dtype}, {df['lpep_pickup_datetime'].dtype}")
    return df


@task(log_prints=True)
def write_local(df, color: str, year:int, dataset_file: str) -> Path:
    """ Write dataset to local under parquet format type"""
    outdir = f"./data/cleaned/{color}"
    if not os.path.exists(outdir):  
        os.mkdir(outdir)  

    path = Path(f"{outdir}/{dataset_file}.parquet")

    df.to_parquet(path, compression="gzip")
    return path


@task(retries=3, log_prints=True)
def write_gcs(path: Path) -> None:
    '''
        Write cleaned data to gcs
    '''
    gcs_block = GcsBucket.load("decamp-gcs-bucket")
    gcs_path = gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    return gcs_path




@task(log_prints=True, retries=3)
def create_new_table_from_gcs(bq_client: bigquery.Client, table_id: str, gcs_path: Path):
    job_config = bigquery.LoadJobConfig(
        source_format="PARQUET",
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            field="lpep_pickup_datetime"
        ),
        clustering_fields=["PULocationID", "DOLocationID"]
    )
    uri =  f"gs://{BUCKET_NAME}/{gcs_path}"

    load_job = bq_client.load_table_from_uri(
        source_uris=uri, 
        destination=table_id, 
        job_config=job_config
    ) 
    return load_job.result()


@task(log_prints=True, retries=3)
def load_to_bq(gcs_path: Path):
    project="dzc-trietle"
    dataset = "trips_data_all"
    table = "yellow_trips"
    table_id = f"{project}.{dataset}.{table}"

    gcp_cred = GcpCredentials.load("decamp-gcp-cred")
    bq_client = gcp_cred.get_bigquery_client()
    
    try:
        bq_client.get_table(table_id)
        print("Table {} already exists.".format(table))

    except NotFound:
        print("Table {} is not found, creating new table.".format(table))
        create_new_table_from_gcs(bq_client, table_id, gcs_path) #task


@flow()
def flows_to_bq():
    color = "yellow"
    year = 2019
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    
    df = fetch_from_gcs(color, dataset_file) #task
    df = transform(df) #task
    path = write_local(df, color, year, dataset_file) #task
    gcs_path = write_gcs(path) #task
    table_name = load_to_bq(gcs_path) #task
    print(table_name)

if __name__ == "__main__":
    flows_to_bq()