from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from utils.general import adhoc


def delete_table_by_partition(table_id: str, from_date: str, to_date: str):
    client = bigquery.Client()
    for date in pd.date_range(from_date, to_date):
        print(f"Delete table {table_id} on {date.strftime('%Y-%m-%d')}")
        client.delete_table(f'{table_id}${adhoc.date_str_no_dash(date)}')