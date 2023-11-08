from prefect import flow, get_run_logger, tags
from prefect import task
from prefect_aws import MinIOCredentials
from prefect_aws.s3 import S3Bucket
import pandas as pd
import numpy as np
@task
def fetch_data():

    minio_credentials = MinIOCredentials(
       minio_root_user = "minio",
       minio_root_password = "minio123"
    )

    s3_bucket = S3Bucket(
        bucket_name="test1",  # must exist
        minio_credentials=minio_credentials,
        endpoint_url="http://10.30.8.228:9001"
    )

    s3_bucket.download_object_to_path("data.csv", "data.csv")
    data = pd.read_csv("data.csv")
    return data

@flow
def hello(name: str = "Marvin"):
    data = fetch_data()
    logger = get_run_logger()
    logger.info(f"Hello, {name}!")
    logger.info(f"minIO data: {data.head(10)}!")

if __name__ == "__main__":
    with tags("local"):
        hello()
