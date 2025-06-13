from pathlib import Path
import yaml
from jinja2 import Template
import uuid
from utils import GCP_STORAGE_BUCKET
from datetime import datetime, timedelta



def get_config(path:str):
    """
    Load the configuration from the YAML file.
    """
    path = Path(path)
    return yaml.safe_load(path.read_text(encoding='utf-8'))

def query_render(params, query_path=None, str_template=None):
    """
    Render a SQL query using Jinja2 template engine.
    :param params: Dictionary of parameters to be used in the template.
    :param query_path: Path to the SQL file to be rendered.
    :param str_template: String template to be rendered.
    """
    query_content = str_template
    if not query_content:
        query_path = Path(query_path)
        query_content = query_path.read_text()
    if not query_content:
        raise Exception("query_path or str_template must be set!!!")

    template = Template(query_content)
    return template.render(**params)

def generate_bucket_uri(bucket, ext='csv.gz'):
    return f'gs://{GCP_STORAGE_BUCKET}/{bucket}/{uuid.uuid4()}.{ext}'

def date_str_no_dash(_date: datetime):
    return _date.strftime("%Y%m%d")