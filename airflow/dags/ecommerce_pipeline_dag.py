from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "janak",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "databricks", "medallion"],
) as dag:

    run_pipeline_job = DatabricksRunNowOperator(
        task_id="run_bronze_silver_gold_pipeline",
        databricks_conn_id="databricks_default",
        job_id=1033793591890426,
    )