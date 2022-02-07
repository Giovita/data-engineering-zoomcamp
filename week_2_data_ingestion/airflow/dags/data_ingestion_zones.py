import logging
import os

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


# source file
DATASET_URL = "https://s3.amazonaws.com/nyc-tlc/misc/"
filename = "taxi_zone_lookup.csv"
file_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

# local
path_to_local = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = filename.replace(".csv", ".parquet")


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

with DAG(
    dag_id="zones_to_gcp",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["zones_ingestion"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset",
        bash_command=f"curl -sSLf {file_url} > {path_to_local}/{filename}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={"src_file": f"{path_to_local}/{filename}"},
    )

    local_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local}/{parquet_file}",
        },
    )

    cleanup_file = BashOperator(
        task_id="ckeanup_container",
        bash_command=f"rm {path_to_local}/{filename} {path_to_local}/{parquet_file}",
        trigger_rule="all_done",
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs >> cleanup_file
