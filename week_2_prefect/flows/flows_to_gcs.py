from prefect import task, flow
from prefect.tasks import task_input_hash
from prefect_gcp import GcsBucket

import pandas as pd
from datetime import timedelta
from pathlib import Path
import os


@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str) -> pd.DataFrame:
    """ Fetch dataset under DataFrame format type"""
    return pd.read_csv(url)


@task(log_prints=True)
def write_local(df, color: str, year:int, dataset_file: str) -> Path:
    """ Write dataset to local under parquet format type"""
    outdir = f"./data/raw/{color}"
    if not os.path.exists(outdir):  
        os.mkdir(outdir)  

    path = Path(f"{outdir}/{dataset_file}.parquet")

    df.to_parquet(path, compression="gzip")
    return path


@task(retries=3, log_prints=True)
def write_gcs(path: Path) -> None:
    """Write data from local to GCS"""
    gcs_block = GcsBucket.load("decamp-gcs-bucket")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    

@flow(name="Ingest taxi data to GCS")
def flows_to_gcs(month: int, year: int, color: str):
    """One single flow from data source(github) to GCS"""
    #sample variables
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(url) #task
    path = write_local(df, color, year, dataset_file) #task
    write_gcs(path) #task

@flow(name="Parent flow to gcs")
def parent_flow_to_gcs(
    months:list[int] = [1,2], 
    year:int=2019, 
    color:str="green"
):
    """Parent flow for parametrizing"""
    for month in months:
        flows_to_gcs(month, year, color)

if __name__ == "__main__":
    parent_flow_to_gcs()



    