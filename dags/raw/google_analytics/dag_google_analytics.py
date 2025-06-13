from airflow import DAG
from airflow.decorators import dag, task

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

from plugins.operators.bigquery import BigQueryDeleteTableByPartitionOperator

from datetime import datetime, timedelta
import os

from utils import GCP_PROJECT_ID, GCP_STORAGE_BUCKET
from utils.general.adhoc import generate_bucket_uri, get_config
from utils.reports import google_analytics


FROM_DATE = "{{ macros.ds_add(ds, -2) }}"
TO_DATE = "{{ ds }}"

CONFIG = get_config(os.path.join(os.path.dirname(__file__), "config.yml"))
BUCKET = CONFIG["bucket"]
TABLE_ID = CONFIG["table_id"].format(gcp_project_id=GCP_PROJECT_ID)

default_args={
    "owner": "dni",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

@dag(
    dag_id = "raw_google_analytics",
    default_args = default_args,
    schedule = "0 1 * * *", # daily at 1:00 AM UTC ~ 8:00 AM UTC+7
    start_date = datetime(2025, 6, 1),
    catchup = True,
    max_active_runs = 1,
    tags = ["raw", "google_analytics"],
    dagrun_timeout=timedelta(hours=4),
    render_template_as_native_obj=True
)
def raw_google_analytics():
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")
    
    @task
    def get_reports(from_date, to_date, **kwargs):
        list_properties = google_analytics.list_property_ids()
        print(f"Found {len(list_properties)} properties linked to the service account.")
        uris = []
        for property_id in list_properties:
            print(property_id)
            uri = generate_bucket_uri(BUCKET, ext='csv')
            google_analytics.get_reports(property_id, from_date, to_date, uri)
            uri_path = uri.replace(f'gs://{GCP_STORAGE_BUCKET}/', '')
            uris.append(uri_path)
        return uris
    get_reports_task = get_reports(FROM_DATE, TO_DATE)
    
    uris = "{{ ti.xcom_pull(task_ids='get_reports', key='return_value') }}"
    
    delete_old_bigquery_data = BigQueryDeleteTableByPartitionOperator(
        task_id = "delete_old_bigquery_data",
        table_id = TABLE_ID,
        from_date = FROM_DATE,
        to_date = TO_DATE
    )

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id = "load_csv_to_bigquery",
        bucket = GCP_STORAGE_BUCKET,
        source_objects = uris,
        source_format = "CSV",
        destination_project_dataset_table = TABLE_ID,
        write_disposition = "WRITE_APPEND",
        skip_leading_rows = 1,
        autodetect = None,
    )
    
    clear_uris = GCSDeleteObjectsOperator(
        task_id = "clear_uris",
        bucket_name = GCP_STORAGE_BUCKET,
        objects = uris,
    )
    
    start >> get_reports_task >> delete_old_bigquery_data >> load_csv_to_bigquery >> clear_uris >> finish

raw_google_analytics()