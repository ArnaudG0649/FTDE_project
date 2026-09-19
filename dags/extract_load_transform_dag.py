import os.path as osp
from glob import glob
from pprint import pprint
from datetime import datetime, timedelta

import yaml

from extract.extract import extract

# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# The DAG object; we'll need this to instantiate a DAG
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
    """List the parquet files produced by the extract task (their names become the GCS blob/BQ table names)."""
    return sorted(osp.basename(f) for f in glob(osp.join(DATA_DIR, "*.parquet")))


@task
def build_upload_kwargs(blob_names: list[str]) -> list[dict]:
    return [{"src": osp.join(DATA_DIR, name), "dst": name} for name in blob_names]


@task
def build_bq_load_kwargs(blob_names: list[str]) -> list[dict]:
    return [
        {
            "source_objects": [name],
            "destination_project_dataset_table": f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{osp.splitext(name)[0]}",
        }
        for name in blob_names
    ]


with DAG(
    "extract_load_transform",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="DAG for France Travail data extraction, load into bigquery and transformation with dbt",
    # schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract", 
        python_callable=extract
    )

    blob_names = get_parquet_blob_names()

    upload_to_gcs = LocalFilesystemToGCSOperator.partial(
        task_id="upload_to_gcs",
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    ).expand_kwargs(build_upload_kwargs(blob_names)) #expand the list of dictionaries into keyword arguments for the operator. 
    # It's a way to dynamically pass multiple sets of keyword arguments to the operator.

    create_bq_tables = GCSToBigQueryOperator.partial(
        task_id="create_bq_tables",
        bucket=GCS_BUCKET_NAME,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    ).expand_kwargs(build_bq_load_kwargs(blob_names))
    
    transform = BashOperator(
        task_id="transform",
        bash_command="cd /opt/airflow/FT_data_transform ; dbt build --profile airflow",
    )

    extract_task >> blob_names >> upload_to_gcs >> create_bq_tables >> transform

    # extract_task >> blob_names >> upload_to_gcs >> create_bq_tables
    
    # t1
    # # t1, t2 and t3 are examples of tasks created by instantiating operators
    # t1 = BashOperator(
    #     task_id="print_date",
    #     bash_command="date",
    # )

    # t2 = BashOperator(
    #     task_id="sleep",
    #     depends_on_past=False,
    #     bash_command="sleep 2",
    #     retries=3,
    # )
    # t1.doc_md = textwrap.dedent(
    #     """\
    # #### Task Documentation
    # You can document your task using the attributes `doc_md` (markdown),
    # `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    # ![img](https://imgs.xkcd.com/comics/fixing_problems.png)
    # **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    # """
    # )

    # dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    # dag.doc_md = """
    # This is a documentation placed anywhere
    # """  # otherwise, type it like this
    # templated_command = textwrap.dedent(
    #     """
    # {% for i in range(5) %}
    #     echo "{{ ds }}"
    #     echo "{{ macros.ds_add(ds, 7)}}"
    # {% endfor %}
    # """
    # )

    # t3 = BashOperator(
    #     task_id="templated",
    #     depends_on_past=False,
    #     bash_command=templated_command,
    # )
    
    
    # t4 = PythonOperator(
    #     task_id=f"current_datetime", python_callable=print_current_datetime
    # )
    
    # t5 = PythonOperator(
    # task_id="create_and_save_dataframe",
    # python_callable=callable_create_and_save_dataframe,
    # # requirements=["pandas==3.0.5","pyarrow==25.0.1"],
    # # system_site_packages=False,
    # )   