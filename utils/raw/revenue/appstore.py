import gzip
import io
import math
from datetime import datetime, timedelta
from pathlib import Path

import jwt
import pandas as pd
import pandasql as ps
import requests

# from utils import APPSTORE_KEY_FILE, adhoc
# APPSTORE_KEY_FILE = 'Appstore API\AuthKey_62R93YJK3F.p8'
APPSTORE_KEY_FILE = 'Appstore API\AuthKey_U48654D4J9.p8'


# Documents: https://developer.apple.com/documentation/appstoreconnectapi/download_sales_and_trends_reports

def get_token():
    header = {
        "alg": "ES256",
        # "kid": "62R93YJK3F",
        "kid": "U48654D4J9",
        "typ": "JWT"
    }
    payload = {
        # "iss": '72be7889-4601-45c0-b01e-865a33a0bf6d',
        "iss": 'dd7ef26c-d3cf-4a75-9113-565858f1b366',
        # 'sub': 'user',
        # "iat": math.floor((datetime.now()).timestamp()),
        "exp": math.floor((datetime.now() + timedelta(minutes=20)).timestamp()),
        "aud": "appstoreconnect-v1"
        # "scope": [
        #     "GET /v1/salesReports"
        # ]
    }
    key = Path(APPSTORE_KEY_FILE).read_text()
    token = jwt.encode(headers=header, algorithm="ES256", payload=payload, key=key)
    return token


def get_sales_reports(from_date: str, to_date: str, uri: str):
    start_date = datetime.strptime(from_date, '%Y-%m-%d')
    end_date = datetime.strptime(to_date, '%Y-%m-%d')

    frames = []
    for date in pd.date_range(start_date, end_date):
        headers = {
            'Authorization': f'Bearer {get_token()}'
        }
        params = {
            'filter[frequency]': 'DAILY',
            'filter[reportDate]': date.strftime('%Y-%m-%d'),
            # 'filter[reportSubType]': 'SUMMARY',
            'filter[reportSubType]': 'DETAILED',
            # 'filter[reportType]': 'SALES',
            'filter[reportType]': 'SUBSCRIBER',
            'filter[version]': '1_3',
            # 'filter[vendorNumber]': '92677080'
            'filter[vendorNumber]': '88180631'
        
        }
        url = 'https://api.appstoreconnect.apple.com/v1/salesReports'
        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()

        frames.append(pd.read_csv(gzip.open(io.BytesIO(res.content)), delimiter='\t'))
    df = pd.concat(frames)
    
    cols = {}
    for col in df.columns:
        cols[col] = col.replace(' ', '_').replace('-', '_')
    df = df.rename(columns=cols)
    
    # df['Begin_Date'] = pd.to_datetime(df['Begin_Date'], format='%m/%d/%Y')
    # df['End_Date'] = pd.to_datetime(df['End_Date'], format='%m/%d/%Y')
    df['Event_Date'] = pd.to_datetime(df['Event_Date'], format='%Y-%m-%d')
    df['Purchase_Date'] = pd.to_datetime(df['Purchase_Date'], format='%Y-%m-%d')
    
    # df = df[[
    #     'Parent_Identifier',
    #     'SKU',
    #     'Begin_Date',
    #     'Title',
    #     'Country_Code',
    #     'Developer_Proceeds',
    #     'Units',
    #     'Customer_Currency',
    # ]]
    df.to_csv(uri, index=False)
    return uri


# weekly report: available on Mondays
# monthly report: available five days after the end of the month
def get_revenue_aggregated(date: str):
    headers = {
        'Authorization': f'Bearer {get_token()}'
    }

    # get last sunday
    d = datetime.strptime(date, '%Y-%m-%d').toordinal()
    l = d - 6
    sunday = datetime.fromordinal(l - (l % 7) + 7)
    print(sunday)

    params = {
        'filter[frequency]': 'WEEKLY',
        'filter[reportDate]': sunday.strftime('%Y-%m-%d'),  # the date of the Sunday ending the desired week
        'filter[reportSubType]': 'SUMMARY',
        'filter[reportType]': 'SALES',
        'filter[vendorNumber]': '86574592'
    }
    url = 'https://api.appstoreconnect.apple.com/v1/salesReports'
    res = requests.get(url, headers=headers, params=params)
    if res.status_code == 400:
        print(res.content)
    res.raise_for_status()

    df = pd.read_csv(gzip.open(io.BytesIO(res.content)), delimiter='\t')

    df = df.rename(columns={
        'Developer Proceeds': 'Developer_Proceeds',
    })
    df = df[[
        'Units',
        'Developer_Proceeds',
    ]]
    print(f'len df={len(df.index)}')
    query = "SELECT SUM(Developer_Proceeds * Units) as revenue FROM df"
    return ps.sqldf(query)


get_sales_reports(from_date='2025-06-01', to_date='2025-06-07', uri='./subscriber_report_2.csv')
# print(get_token())

