import os
import pandas as pd

from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal

import streamlit as st

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
print(server_hostname)

def credential_provider():
    config = Config(
        host=f"https://{server_hostname}",
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"))

    return oauth_service_principal(config)


@st.cache_data
def get_data(table_name):
    st.text(server_hostname)
    st.text(os.getenv("DATABRICKS_HTTP_PATH"))
    st.text(table_name)

    # get a connection to a warehouse
    with sql.connect(server_hostname=server_hostname,
                     http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                     credentials_provider=credential_provider) as connection:

        query = f"SELECT * FROM {table_name} LIMIT 10"

        # use the connection to run a query
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

            # collect result rows into a list of dictionaries, then convert to a Pandas dataframe
            rows = [r.asDict() for r in result]
            df = pd.DataFrame(rows)

    #data = {
    #    'Name': ['Alice', 'Bob'],
    #    'Age': [30, 25]
    #}
    #df = pd.DataFrame(data)

    return df


if __name__ == '__main__':
    # Create a text element and let the reader know the data is loading.
    data_load_state = st.text('Loading data...')

    data = get_data(table_name='samples.nyctaxi.trips')
    st.dataframe(data)

    # Notify the reader that the data was successfully loaded.
    data_load_state.text('Loading data...done!')

