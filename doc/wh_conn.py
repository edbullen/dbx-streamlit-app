# Example - Python script that queries a SQL Warehouse
import os
from typing import Dict, Optional

import pandas as pd
from dotenv import load_dotenv

from databricks import sql
from databricks.sdk.core import Config


# for local development - picks up variables from .env file
load_dotenv() 

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
warehouse_http_path = os.getenv("DATABRICKS_HTTP_PATH")


# Databricks SDK credentials - authenticate using OAuth token
def credential_provider():
    config = Config(
        host=f"https://{server_hostname}"
    )
    return config.authenticate


# Run query using databricks.sql, connection details in connection_kwargs, return Pandas DF
def _run_query(table_name: str, limit: int, connection_kwargs: Dict[str, str]) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name} LIMIT {int(limit)}"
    with sql.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = [r.asDict() for r in cursor.fetchall()]
            return pd.DataFrame(rows)


# Get data as Pandas DF by passing in a Databricks SQL Warehouse connection to _run_query
def get_data(table_name: str, limit: int = 200) -> pd.DataFrame:
    connection_kwargs = dict(
        server_hostname=server_hostname,
        http_path=warehouse_http_path,
        credentials_provider=credential_provider,
    )
    return _run_query(table_name, limit, connection_kwargs)


# MAIN 
if __name__ == '__main__':
    
    data = get_data(table_name='samples.nyctaxi.trips', limit=10)

    for index, row in data.iterrows():
        print(row[0], row[1], row[3])
        

