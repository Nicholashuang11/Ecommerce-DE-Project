from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
RAW_PATH = "/opt/spark-data/raw_layer"
BRONZE_PATH = "/opt/spark-data/bronze_layer"
SILVER_PATH = "/opt/spark-data/silver_layer"
GOLD_PATH = "/opt/spark-data/gold_layer"
SRC_PATH = "/opt/airflow/src"

SPARK_SUBMIT = "docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077"

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="olist_etl_pipeline",
    description="olist ecommerce pipeline for analytics",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2026,9,19),
    catchup=False,
    tags=["olist", "etl", "data-engineering"],
) as dag:
    transform_bronze = BashOperator(
            task_id="transform_bronze",
            bash_command=(
                f"{SPARK_SUBMIT} /opt/spark-jobs/bronze_stage.py "
                f"--data-path {RAW_PATH_PATH} "
                f"--output-path {BRONZE_PATH} "
                f"--ingest-date {{{{ ds }}}}"
            ),
        )
    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/silver_stage.py "
            f"--data-path {BRONZE_PATH} "
            f"--output-path {SILVER_PATH} "
            f"--ingest-date {{{{ ds }}}}"
        ),
    )

    transform_gold = BashOperator(
        task_id="transform_gold",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/transform_gold.py "
            f"--data-path {SILVER_PATH} "
            f"--output-path {GOLD_PATH} "
            f"--ingest-date {{{{ ds }}}}"
        ),
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=(
            f"python {SRC_PATH}/loadscript.py "
            f"--staging-path {GOLD_PATH}"
        ),
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=f"python {SRC_PATH}/quality_checks.py",
    )

    refresh_views = BashOperator(
        task_id="refresh_analytics_views",
        bash_command=f"python {SRC_PATH}/refresh_views.py",
    )

    transform_bronze>> transform_silver >> transform_gold >> load_warehouse >> quality_checks >> refresh_views