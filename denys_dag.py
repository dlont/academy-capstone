from datetime import datetime

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator


DAG_TAGS = ['denys_dag']

BATCH_JOB_NAME = 'denys_lontkovskyi-app_af_job1'
BATCH_JOB_QUEUE_NAME = 'academy-capstone-summer-2022-job-queue'
BATCH_JOB_DEFINITIOIN_NAME = 'denys_lontkovskyi-app'

JOB_OVERRIDES = {}


with DAG(
    dag_id='denys_dag_id',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2022, 9, 1),
    tags=DAG_TAGS,
    catchup=False,
) as dag:


    submit_batch_job = AwsBatchOperator(
        task_id='submit_batch_job',
        job_name=BATCH_JOB_NAME,
        job_queue=BATCH_JOB_QUEUE_NAME,
        job_definition=BATCH_JOB_DEFINITIOIN_NAME,
        overrides=JOB_OVERRIDES,
        # Set this flag to False, so we can test the sensor below
        # wait_for_completion=False,
    )
