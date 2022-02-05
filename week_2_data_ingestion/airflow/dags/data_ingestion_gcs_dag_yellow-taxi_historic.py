import logging
import os
from datetime import datetime

import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.utils.dates import days_ago
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

# dataset_file = "yellow_tripdata_2021-01.csv"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"  # Should be time of current run --> jinja templates

# source file
dataset_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/"  # base url of files, usefull for yellow taxi and fh
file_prefix = "yellow_tripdata_"  # + YYYY-MM.CSV
dataset_file = file_prefix + {{"ds_format(ds, %Y-%m)"}} + ".csv"
file_url = dataset_url + dataset_file

# change to parquet vars
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace(".csv", ".parquet")  # Should be ingested via XCOM


def format_to_parquet(**context):
    ti = context["ti"]
    src_file = ti.xcom_pull(task_ids=download_dataset_task)
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))
    ti.xcom_push(key="pq_file", value=src_file.replace(".csv", ".parquet"))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, **context):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    # get File name via XCOM
    ti = context["ti"]
    local_file = ti.xcom_pull(key="pq_file")

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
# Must run at month over with data of the previous month. So on 01-Feb we push data from 01-31 Jan
# DAGs `catch up` if there is any period
with DAG(
    dag_id="data_ingestion_gcs_yellow-taxi_monthly-dag",
    start_date=datetime(
        2019, 1, 1
    ),  # Start date, first run would happen at 2019-2-1 @6am with data of 2019-1-1
    schedule_interval="0 6 1 * *",  # Accepts cronjob syntax (from crontab.guru)
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dtc-de"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS {file_url} > {path_to_local_home}/{dataset_file}",
        xcom_push=True,
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        # op_kwargs={
        #     "src_file": f"{path_to_local_home}/{TaskInstance.xcom_pull(task_ids='download_dataset_task'}",
        # },
        provide_context=True,
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
        provide_context=True,
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{{ ti.xcom_pull(key='pq_file') }}"],
            },
        },
    )

    (
        download_dataset_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
    )
