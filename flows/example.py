from prefect import flow, get_run_logger, tags
from prefect import task
from prefect_aws import MinIOCredentials
from prefect_aws.s3 import S3Bucket

from datetime import timedelta
import requests

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

import pandas as pd
import numpy as np
import mlflow

import os


@task
def fetch_data():

    minio_credentials = MinIOCredentials(
       minio_root_user = "minio",
       minio_root_password = "minio123"
    )
    s3_client = minio_credentials.get_boto3_session().client(
       service_name="s3",
       endpoint_url="http://10.30.8.228:9000"
    )

#    s3_client.download_file('test1', 'data.csv', 'data.csv')
    s3_client.download_file(Bucket="test1", Key="data.csv", Filename="data.csv")
    data = pd.read_csv("data.csv")
    return data


def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2
@task
def train_model(data, mlflow_experiment_id, alpha=0.5, l1_ratio=0.5, logger):
    mlflow.set_tracking_uri("http://10.30.8.228:5000")
    train, test = train_test_split(data)

    # The predicted column is "quality" which is a scalar from [3, 9]
    train_x = train.drop(["quality"], axis=1)
    test_x = test.drop(["quality"], axis=1)
    train_y = train[["quality"]]
    test_y = test[["quality"]]

    with mlflow.start_run(experiment_id=mlflow_experiment_id):
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)
        predicted_qualities = lr.predict(test_x)
        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

        logger.info("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
        logger.info("  RMSE: %s" % rmse)
        logger.info("  MAE: %s" % mae)
        logger.info("  R2: %s" % r2)

        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        mlflow.sklearn.log_model(lr, "model")

@flow
def hello(name: str = "Train"):
    data = fetch_data()

    logger = get_run_logger()
    logger.info(f"Let's, {name}!")
    logger.info(f"minIO data: {data.head(10)}!")

    train_model(data=data, mlflow_experiment_id=1, alpha=0.3, l1_ratio=0.3, logger)
if __name__ == "__main__":
    with tags("local"):
        hello()
