from airflow.sdk import BaseOperator

from utils.general.bigquery import delete_table_by_partition
from utils import GCP_PROJECT_ID


class BigQueryDeleteTableByPartitionOperator(BaseOperator):
    """
    Custom operator to delete a BigQuery table by partition.
    """
    
    template_fields = ('from_date', 'to_date')
    
    def __init__(
        self,
        table_id: str,
        from_date: str,
        to_date: str,
        # gcp_project_id = GCP_PROJECT_ID,
        # gcp_conn_id = 'google_cloud_default',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.table_id = table_id
        self.from_date = from_date
        self.to_date = to_date
        # self.gcp_project_id = gcp_project_id

    def execute(self, context):
        delete_table_by_partition(
            table_id = self.table_id, 
            from_date = self.from_date, 
            to_date = self.to_date
        )