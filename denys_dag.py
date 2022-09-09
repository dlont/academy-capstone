from airflow import DAG
     
from airflow.providers.amazon.aws.operators.batch import BatchOperator

DAG_TAGS = ['denys_dag']

BATCH_JOB_NAME = 'denys_lontkovskyi-app_af_job1'
BATCH_JOB_QUEUE_NAME = 'academy-capstone-summer-2022-job-queue'
BATCH_JOB_DEFINITIOIN_NAME = 'denys_lontkovskyi-app'

JOB_OVERRIDES = {}
# JOB_FLOW_OVERRIDES = {
#     'Name': 'my-demo-cluster',
#     'ReleaseLabel': 'emr-5.30.1',
#     'Applications': [
#         {
#             'Name': 'Spark'
#         },
#     ],    
#     'Instances': {
#         'InstanceGroups': [
#             {
#                 'Name': "Master nodes",
#                 'Market': 'ON_DEMAND',
#                 'InstanceRole': 'MASTER',
#                 'InstanceType': 'm5.xlarge',
#                 'InstanceCount': 1,
#             },
#             {
#                 'Name': "Slave nodes",
#                 'Market': 'ON_DEMAND',
#                 'InstanceRole': 'CORE',
#                 'InstanceType': 'm5.xlarge',
#                 'InstanceCount': 2,
#             }
#         ],
#         'KeepJobFlowAliveWhenNoSteps': False,
#         'TerminationProtected': False,
#         'Ec2KeyName': 'mykeypair',
#     },
#     'VisibleToAllUsers': True,
#     'JobFlowRole': 'EMR_EC2_DefaultRole',
#     'ServiceRole': 'EMR_DefaultRole'
# }

with DAG(
    dag_id='denys_dag_id',
    schedule_interval='@none',
    start_date=datetime(2022, 9, 1),
    tags=DAG_TAGS,
    catchup=False,
) as dag:


    submit_batch_job = BatchOperator(
        task_id='submit_batch_job',
        job_name=BATCH_JOB_NAME,
        job_queue=BATCH_JOB_QUEUE_NAME,
        job_definition=BATCH_JOB_DEFINITIOIN_NAME,
        overrides=JOB_OVERRIDES,
        # Set this flag to False, so we can test the sensor below
        wait_for_completion=False,
    )
