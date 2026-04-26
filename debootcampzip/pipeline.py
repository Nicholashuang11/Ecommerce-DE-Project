import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DATA_PATH    = "/opt/airflow/dataset"
STAGING_PATH = "/opt/airflow/staging"
SRC_PATH = "/opt/airflow/src"

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="olist_etl_pipeline",
    description="olist eccomerece pipeline for analytics",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "etl", "data-engineering"],
) as dag:

    transform_dims = BashOperator(
        task_id="transform_dimensions",
        bash_command=(
            f"python {SRC_PATH}/transform_dim.py "
            f"--data-path {DATA_PATH} --output-path {STAGING_PATH}"
        ),
    )

  
    transform_fact = BashOperator(
        task_id="transform_fact",
        bash_command=(
            f"python {SRC_PATH}/transform_fact.py "
            f"--data-path {DATA_PATH} --output-path {STAGING_PATH}"
        ),
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=(
            f"python {SRC_PATH}/loadscript.py "
            f"--staging-path {STAGING_PATH}"
        ),
    )

  
    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            f"python {SRC_PATH}/quality_checks.py"
        ),
    )
    refresh_views = BashOperator(
        task_id="refresh_analytics_views",
        bash_command=(
            f"python {SRC_PATH}/refresh_views.py"
        ),
    )

    (
        transform_dims
        >> transform_fact
        >> load_warehouse
        >> 
        quality_checks
        >> 
        refresh_views
    )