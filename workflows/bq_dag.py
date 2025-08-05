import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os

PROJECT_ID = "active-district-466711-i0"
LOCATION = "US"

def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

ARGS = {
    "owner": "Nikhil Sharma",
    "start_date": days_ago(1),  # Set a valid start date
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    description="DAG to run the bigquery jobs",
    default_args=ARGS,
    tags=["gcs", "bq", "etl", "marvel"]
) as dag:

    bronze_sql = read_sql_file("/home/airflow/gcs/data/BQ/bronze.sql")
    silver_sql = read_sql_file("/home/airflow/gcs/data/BQ/silver.sql")
    gold_sql = read_sql_file("/home/airflow/gcs/data/BQ/gold.sql")

    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": bronze_sql,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": silver_sql,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": gold_sql,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    bronze_tables >> silver_tables >> gold_tables