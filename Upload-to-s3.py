from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime, timedelta
import os

S3_BUCKET_NAME = "credit-inst"
S3_PREFIX = "stage/"  
LOCAL_FOLDER = "/Case Study - Material"  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
    "email": ['airflow@example.com'],
    "email_on_failure": False
}

with DAG(
    "upload_csv_to_s3",
    default_args=default_args,
    schedule_interval="0 2 * * *", 
    catchup=False,
) as dag:

    for filename in os.listdir(LOCAL_FOLDER):
        if filename.endswith(".csv"):
            upload_task = LocalFilesystemToS3Operator(
                task_id=f"upload_{filename}",
                filename=os.path.join(LOCAL_FOLDER, filename),
                dest_key=f"{S3_PREFIX}{filename}",
                bucket_name=S3_BUCKET_NAME,
                aws_conn_id="aws_default",
                replace=True,
            )
