import os.path as osp
from glob import glob
from pprint import pprint
from datetime import datetime, timedelta

import yaml

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.sdk import DAG, task

with open("dags/extract/params.yaml", "r") as f:
    _params = yaml.load(f, Loader=yaml.FullLoader)

DATA_DIR = _params["data_dir"]
GCP_PROJECT_ID = _params["gcp_project_id"]
GCS_BUCKET_NAME = _params["gcs_bucket_name"]
BQ_DATASET_NAME = _params["bq_dataset_name"]
GCP_CONN_ID = "google_cloud_default"


@task
def get_parquet_blob_names() -> list[str]:
    """List the parquet files produced by the extract task (their names become the GCS blob/BQ table names).""" #blob = binary large object
    return sorted(osp.basename(f) for f in glob(osp.join(DATA_DIR, "*.parquet")) if osp.basename(f) != "jobs_subdomains_extended.parquet")


@task
def build_upload_kwargs(blob_names: list[str]) -> list[dict]:
    """Build the keyword arguments for the LocalFilesystemToGCSOperator based on the list of blob names."""
    return [{"src": osp.join(DATA_DIR, name), "dst": name} for name in blob_names]


@task
def build_bq_load_kwargs(blob_names: list[str]) -> list[dict]:
    """Build the keyword arguments for the GCSToBigQueryOperator based on the list of blob names."""
    return [
        {
            "source_objects": [name],
            "destination_project_dataset_table": f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{osp.splitext(name)[0]}",
        }
        for name in blob_names
    ]


with DAG(
    "load_transform",

    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),

    },
    description="DAG for France Travail data load into bigquery and transformation with dbt",
    # schedule=timedelta(days=1),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # Get the list of parquet blob names to be processed.
    blob_names = get_parquet_blob_names()

    # Upload the parquet files to Google Cloud Storage.
    upload_to_gcs = LocalFilesystemToGCSOperator.partial(
        task_id="upload_to_gcs",
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    ).expand_kwargs(build_upload_kwargs(blob_names)) #expand the list of dictionaries into keyword arguments for the operator. 
    # It's a way to dynamically pass multiple sets of keyword arguments to the operator.

    # Create the corresponding BigQuery tables from the uploaded parquet files.
    create_bq_tables = GCSToBigQueryOperator.partial(
        task_id="create_bq_tables",
        bucket=GCS_BUCKET_NAME,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    ).expand_kwargs(build_bq_load_kwargs(blob_names))
    
    # Transform the data using dbt.
    transform = BashOperator(
        task_id="transform",
        bash_command="cd /opt/airflow/FT_data_transform ; dbt build --profile airflow",
    )

    blob_names >> upload_to_gcs >> create_bq_tables >> transform