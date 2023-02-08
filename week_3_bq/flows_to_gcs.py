from prefect import task, flow
from prefect.tasks import task_input_hash
from prefect_gcp import GcsBucket
from typing import Union

import pandas as pd
from datetime import timedelta
from pathlib import Path
import os



@task(log_prints=True)
def download(url:str, dataset_file: str) -> Path:
    """ Write dataset to local under parquet format type"""
    print(f"Downloading {dataset_file}")

    outdir = f"./data/raw/fhv"
    if not os.path.exists(outdir):  
        os.mkdir(outdir)  

    path = Path(f"{outdir}/{dataset_file}")
    os.system(f"wget -O {path} {url}")

    print(f"Downloaded {dataset_file}")
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
def flows_to_gcs(month: int, year: int):
    """One single flow from data source(github) to GCS"""
    #sample variables
    dataset_file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}"
    # url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz"
    
    path = download(url, dataset_file) #task
    # write_gcs(path) #task

    # path = write_local(df, year, dataset_file) #task

@flow(name="Parent flow to gcs")
def parent_flow_to_gcs(
    months:Union[int, list[int]] = 4, 
    year:int=2019, 
):
    """Parent flow for parametrizing"""
    if isinstance(months, int):
        flows_to_gcs(months, year)

    elif isinstance(months, list):
        for month in months:
            flows_to_gcs(month, year)

    else:
        raise TypeError("param must be of type int or list")

if __name__ == "__main__":
    months = list(range(1,13))
    year = 2019
    parent_flow_to_gcs(
        months = months,
        year = year
    )



    