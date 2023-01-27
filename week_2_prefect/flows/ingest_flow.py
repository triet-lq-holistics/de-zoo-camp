import os
import argparse
import pandas as pd
from datetime import timedelta

from sqlalchemy import create_engine
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.tasks import task_input_hash





def input():
    # print('get input from')
    parser = argparse.ArgumentParser(description='Ingest parquet data to postgres')

    # parser.add_argument('--user',  help='username for postgres')
    # parser.add_argument('--password',  help='password for postgres')
    # parser.add_argument('--host',  help='host for postgres')
    # parser.add_argument('--port', type=int, help='port for postgres')
    # parser.add_argument('--dbname',  help='db name for postgres')
    parser.add_argument('--tname',  help='table name for postgres')
    parser.add_argument('--url',  help='url to the converted file for postgres')
    return parser.parse_args() 


@task(log_prints=True, retries = 3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(url):
    print("Extracting from %s..." % url)

    file_type = url.split(".")[-1]

    if file_type == 'parquet':
        file_name = "output.parquet"
        os.system(f"wget {url} -O {file_name}")
        df = pd.read_parquet(f'{file_name}')

    elif file_type == 'csv':
        file_name = "output.csv"
        os.system(f"wget {url} -O {file_name}")
        df = pd.read_csv(f'{file_name}')
    
    elif file_type == "gz":
        file_name = "output.csv.gz"
        os.system(f"wget {url} -O {file_name}")
        df = pd.read_csv(
            f'{file_name}',
            compression='gzip',
            header=0,
            sep=',',
            quotechar='"'
            )
    else:
        raise Exception("Invalid input file type")
    
    return df

@task(log_prints=True, retries = 1)
def transform(df):
    print(df.columns)
    print("Transforming data")

    print(df.columns)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    return df

@task(log_prints=True, retries = 3)
def load(df, tb_name):
    # user = params.user
    # password = params.password
    # host = params.host
    # port = params.port
    # pg_engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    # pg_engine.connect()
    database_block =  SqlAlchemyConnector.load("postgres-decamp-test-connector")
    with database_block.get_connection(begin=False) as pg_engine:
        df.to_sql(name=f"{tb_name}", con=pg_engine, if_exists='replace', chunksize=10000)

@flow(name="Ingest taxi data to postgres")
def main_flow():
    params = input()
    df = extract(params.url)
    df = transform(df)
    load(df, params.tname)


if __name__ == "__main__":
    main_flow()