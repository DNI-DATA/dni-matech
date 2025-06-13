from google.analytics.admin import AnalyticsAdminServiceClient

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest

import pandas as pd


# To use this script, you need to add the service account email to your GA4 property with the "Editor" role.
# And enable the Google Analytics Data API in the service account's main Google Cloud project.

ads_dimensions = [
    "date",
    "defaultChannelGroup",
    "source",
    "medium",
    "campaignId",
    "campaignName",
    # "platform", # ad costs can't be broken down by streamId and platform
    # "streamId"
]

ads_metrics = [
    "keyEvents",
    "advertiserAdCost",
    "advertiserAdClicks",
    "advertiserAdCostPerClick",
    "advertiserAdImpressions",
    "purchaseRevenue",
    "totalAdRevenue",
    "totalRevenue",
    "returnOnAdSpend",
]

google_ads_dimensions = [
    "date",
    "googleAdsAccountName",
    "googleAdsCustomerId",
    "googleAdsAdGroupId",
    "googleAdsAdGroupName",
    "googleAdsAdNetworkType",
    "googleAdsCampaignId",
    "googleAdsCampaignName",
    # "googleAdsKeyword", # googleAdsKeyword and "googleAdsQuery" are not set
    # "googleAdsQuery",
]


# get all the property_ids link with service account
def list_property_ids():
    list_properties = []
    client = AnalyticsAdminServiceClient()
    results = client.list_account_summaries()
    for account in results.__getattr__("account_summaries"):
        for property in account.property_summaries:
            list_properties.append(property.property.split("properties/").pop())
    # list_properties = ['431146607']  # for testing
    return list_properties

# get package_name to join với dim_product
def get_package_name(property_id: str):
    client = AnalyticsAdminServiceClient()
    results = client.list_data_streams(parent=f"properties/{property_id}")
    columns = ["streamId", "package_name", "platform"]
    data = []
    for stream in results.__getattr__("data_streams"):
        stream_id = stream.name.split("dataStreams/").pop()
        if len(stream.android_app_stream_data.package_name) > 0:
            package_name = stream.android_app_stream_data.package_name
            platform = "Android"
        elif len(stream.ios_app_stream_data.bundle_id) > 0:
            package_name = stream.ios_app_stream_data.bundle_id
            platform = "iOS"
        else:
            continue
        data.append([stream_id] + [package_name] + [platform])
    info_df = pd.DataFrame(data=data, columns=columns)
    info_df['property_id'] = property_id
    return info_df

# get stream list for reports that can't breakdown by streamId and platform
def get_streams(property_id: str):
    client = AnalyticsAdminServiceClient()
    results = client.list_data_streams(parent=f"properties/{property_id}")
    columns = ['streams']
    streams = {'list': []}
    for stream in results.__getattr__("data_streams"):
        stream_id = stream.name.split("dataStreams/").pop()
        if len(stream.android_app_stream_data.package_name) > 0:
            package_name = stream.android_app_stream_data.package_name
            platform = "Android"
        elif len(stream.ios_app_stream_data.bundle_id) > 0:
            package_name = stream.ios_app_stream_data.bundle_id
            platform = "iOS"
        else:
            continue
        streams['list'].append({'stream':stream_id, 'package_name':package_name, 'platform':platform})
    data= [f"{streams}"]
    info_df = pd.DataFrame(data=data, columns=columns)
    info_df['property_id'] = property_id    
    return info_df

# get reports from GA4
def get_reports(property_id: str, start_date: str, end_date: str, uri, list_dimensions=ads_dimensions, list_metrics=ads_metrics):
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=dim) for dim in list_dimensions],
        metrics=[Metric(name=metric) for metric in list_metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    response = client.run_report(request)
    columns = [header.name for header in response.dimension_headers] + [header.name for header in
                                                                        response.metric_headers]
    data = []
    for row in response.rows:
        data.append([dim.value for dim in row.dimension_values] + [metric.value for metric in row.metric_values])
    df = pd.DataFrame(data=data, columns=columns)
    df['property_id'] = property_id
    if 'streamId' in list_dimensions and 'platform' in list_dimensions:
        info_df = get_package_name(property_id)
        df = df.merge(info_df, on=["streamId", "platform"], how='left')
    else:
        info_df = get_streams(property_id)
        df = df.merge(info_df, on=["property_id"], how='left')
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    df.to_csv(uri, index=False)
    return uri


# import os
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'secrets/gcp-service-account.json'
# # list_property_ids = list_property_ids()
# list_property_ids = ['431146607']
# for property_id in list_property_ids:
#     get_reports(property_id, "2025-06-01", "2025-06-07", "test.csv")

# # print(get_streams('461274246'))
# print(list_property_ids())