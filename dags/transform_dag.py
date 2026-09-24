from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow.sdk import DAG, task

with DAG(
    "transform",

    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),

    },
    description="DAG for France Travail data transformation with dbt",
    # schedule=timedelta(days=1),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["example"],
) as dag:

    transform = BashOperator(
        task_id="transform",
        bash_command="cd /opt/airflow/FT_data_transform ; dbt build --profile airflow",
    )

    transform
