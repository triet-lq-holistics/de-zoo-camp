import os
import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    dbname = params.dbname
    tname = params.tname
    url = params.url

    file_type = url.split(".")[-1]
    print("This is the", file_type)
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

   
    
    pg_engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    pg_engine.connect()

    df.to_sql(name=f"{tname}", con=pg_engine, if_exists='replace', chunksize=10000)


if __name__ == "__main__":
    # user
    # password
    # host
    # port
    # databasename
    # tablename
    # url of the parquet
    parser = argparse.ArgumentParser(description='Ingest parquet data to postgres')

    parser.add_argument('--user',  help='username for postgres')
    parser.add_argument('--password',  help='password for postgres')
    parser.add_argument('--host',  help='host for postgres')
    parser.add_argument('--port', type=int, help='port for postgres')
    parser.add_argument('--dbname',  help='db name for postgres')
    parser.add_argument('--tname',  help='table name for postgres')
    parser.add_argument('--url',  help='url to the converted file for postgres')
    # parser.add_argument('--filename',  help='converted filename for postgres')
    args = parser.parse_args() 
    main(args)