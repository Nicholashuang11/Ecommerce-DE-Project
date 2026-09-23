from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
RAW_PATH = "/opt/spark-data/raw_layer"
BRONZE_PATH   = "/opt/spark-data/bronze_layer"
SILVER_PATH = "/opt/spark-data/silver_layer"
GOLD_PATH   = "/opt/spark-data/gold_layer"
SRC_PATH    = "/opt/airflow/src"

default_args = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="olist_etl_pipeline",
    description="olist ecommerce pipeline for analytics",
    default_args=default_args,
    schedule="0 1 * * *",
    start_date=datetime(2026,9,19),
    catchup=False,
    tags=["olist", "etl", "data-engineering"],
) as dag:

    transform_bronze = SparkSubmitOperator(
        task_id="transform_bronze",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_stage.py",
        application_args=[
            "--data-path", RAW_PATH,
            "--output-path", BRONZE_PATH,
            "--ingest-date", "{{ ds }}",
        ]
    )

    transform_silver = SparkSubmitOperator(
        task_id="transform_silver",
        conn_id="spark_default",
        application="/opt/spark-jobs/silver_stage.py",
        application_args=[
            "--data-path", BRONZE_PATH,
            "--output-path", SILVER_PATH,
            "--ingest-date", "{{ ds }}",
        ],
    )

    transform_gold = SparkSubmitOperator(
        task_id="transform_gold",
        conn_id="spark_default",
        application="/opt/spark-jobs/gold_stage.py",
        application_args=[
            "--data-path", SILVER_PATH,
            "--output-path", GOLD_PATH,
            "--ingest-date", "{{ ds }}"
        ],
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=f"python {SRC_PATH}/loadscript.py --staging-path {GOLD_PATH}",
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=f"python {SRC_PATH}/quality_checks.py",
    )

    refresh_views = BashOperator(
        task_id="refresh_analytics_views",
        bash_command=f"python {SRC_PATH}/refresh_views.py",
    )

    transform_bronze >> transform_silver >> transform_gold >> load_warehouse >> quality_checks >> refresh_views