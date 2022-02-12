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
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")


# source file
DATASET_URL = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
file_prefix = "fhv_tripdata_"
dataset_file = file_prefix + "{{execution_date.strftime('%Y-%m')}}" + ".csv"
file_url = DATASET_URL + dataset_file

# local storage
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace(".csv", ".parquet")
src_pq_file = path_to_local_home + parquet_file


def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


def upload_to_gcs(bucket, object_name, local_file):
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

    # get File name via XCOM  for training purposes (can be done with global `file_url` variable)
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
# Must run at month over with data of the previous month. So on 01-Feb we push data from 01-31 Jan
# DAGs `catch up` if there is any period
with DAG(
    dag_id="fhv_monthly_data",
    start_date=datetime(
        2019, 1, 1
    ),  # Start date, first run would happen at 2019-2-1 @6am with data of 2019-1-1
    # end_date=datetime(2021, 7, 1),
    schedule_interval="0 6 2 * *",  # Accepts cronjob syntax (from crontab.guru)
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=["dtc-de"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset",
        # do_xcom_push=True,
        bash_command=f"curl -sSLf {file_url} > {path_to_local_home}/{dataset_file}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
        # provide_context=True,
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
        # provide_context=True,
    )

    cleanup_files = BashOperator(
        task_id="cleanup_container",
        bash_command=f"rm {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}",
        trigger_rule="all_done",
    )

    (
        download_dataset_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> cleanup_files
    )