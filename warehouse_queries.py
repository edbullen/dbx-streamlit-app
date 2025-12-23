from databricks.sql.client import Connection

import pandas as pd


def warehouse_fares_query(connection: Connection, table_name: str) -> pd.DataFrame:
    """warehouse query returns data-frame of avg fare amounts and trip counts per zip"""
    query = f"""SELECT pickup_zip, AVG(fare_amount) as avg_fare, COUNT(*) as count
                FROM {table_name}
                GROUP BY pickup_zip
            """
    with connection.cursor() as cursor:
            cursor.execute(query)
            rows = [r.asDict() for r in cursor.fetchall()]
            return pd.DataFrame(rows)



def warehouse_dests_query(connection: Connection, table_name: str) -> pd.DataFrame:
    """warehouse query returns data-frame of trip counts +fares per zip-to-zip dests"""
    query = f"""SELECT pickup_zip, dropoff_zip, AVG(fare_amount) as avg_fare, COUNT(*) as count
                FROM {table_name}
                GROUP BY pickup_zip, dropoff_zip
                ORDER BY count DESC LIMIT 200
            """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = [r.asDict() for r in cursor.fetchall()]
        return pd.DataFrame(rows)
