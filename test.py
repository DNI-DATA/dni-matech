from datetime import datetime, timedelta
import pandas as pd
import os
import ast

# from utils import GCP_PROJECT_ID, GCP_STORAGE_BUCKET
# from utils.general.adhoc import generate_bucket_uri, get_config
# from utils.general.bigquery import delete_table_by_partition
# from utils.raw import google_analytics


# from_date = "2025-06-01"
# to_date = "2025-06-05"

# for date in pd.date_range(from_date, to_date):
#         print(f"Delete table on {date.strftime('%Y-%m-%d')}")

uri ="['raw/google_analytics/4b814bc1-c125-4117-9b3a-2872827c0769.csv']"
# convert to list
uri = ast.literal_eval(uri)

print(uri)
print(type(uri))


date:DATE,
defaultChannelGroup:STRING,
source:STRING,
medium:STRING,
campaignId:STRING,
campaignName:STRING,
keyEvents:FLOAT,
advertiserAdCost:FLOAT,
advertiserAdClicks:INTEGER,
advertiserAdCostPerClick:FLOAT,
advertiserAdImpressions:INTEGER,
purchaseRevenue:FLOAT,
totalAdRevenue:FLOAT,
totalRevenue:FLOAT,
returnOnAdSpend:FLOAT,
property_id:STRING,
streams:STRING,