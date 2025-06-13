import io
import re

import pandas as pd

import requests

# from utils import APPSFLYER_TOKEN_V2

APPSFLYER_TOKEN_V2 = "eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.dATpMjDx8u5bCI7HGl_1ih_Rbrk-BaStgyiUgFcyukH79g0Nda9J6g.M7kM0PWJ6Wu3xbLi.WOa_Zw5C7b3GD3g4URGqBh4KpSuW_ruZZv0-G6yOetkG5MPoVlOdK-6oG44L6JcC6fD3INl6PQakN6bNxvSnMntCVscjgSEj1HbtgZ8LJ0NAw11dvHvXr7VFnp0A6QaHlUZh0NuY3u7nXEZsi73LpK27Z7MuYOdQagJjXqKUNdrf3RfKpNhk6qdcOPV4iydAe-a_i3eOqqrUHlZbK5501A2O4kuKBm5NbhGtE0HMJgeMUkZCn-3q3tHu8etFd5t12WHwo3TVQDleIQpZ_XVmw00kB-ctcT8Z70px1MFCkO3_uGbUOA-Kb4gdtMR1VAKuf-aZ1C2BnpLjfb0kCTqNjzY.oJr6uq7W8c8Ej7Rk-xpu0w"


def get_apps():
    url = "https://hq1.appsflyer.com/api/mng/apps"
    headers = {"Authorization": f"Bearer {APPSFLYER_TOKEN_V2}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    apps = res.json().get('data')
    return [{'id': a['id'], 'name': a['attributes']['name'], 'platform': a['attributes']['platform']} for a in apps]

def get_raw_data(app_id, report_type, from_date, to_date, uri):
    url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{report_type}/v5"
    headers = {"Authorization": f"Bearer {APPSFLYER_TOKEN_V2}"}
    params = {
        'from': from_date,
        'to': to_date,
        'maximum_rows': 1000000
    }
    res = requests.get(url, headers=headers, params=params)
    if res.status_code == 400:
        print(res.text)
    res.raise_for_status()
    df = pd.read_csv(io.StringIO(res.text))

    # Rename column names
    cols = {}
    for col in list(df):
        cols.update({col: re.sub("[^0-9a-zA-Z]+", "_", col)})
    df.rename(columns=cols, inplace=True)
    
    df.to_csv(uri, index=False)
    
# TESTING
# print(get_apps())
get_raw_data(
    app_id='id6743389913',
    report_type='installs',
    from_date='2025-06-01',
    to_date='2025-06-07',
    uri='raw_data.csv'
)