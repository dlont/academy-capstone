import os

from boto3 import Session

boto_session = Session()
credentials = boto_session.get_credentials()
# Credentials are refreshable, so accessing your access key / secret key
# separately can lead to a race condition. Use this to get an actual matched
# set.
current_credentials = credentials.get_frozen_credentials()
print(current_credentials)


from pyspark.sql import SparkSession

builder = SparkSession.builder
builder.config("fs.s3n.awsAccessKeyId", current_credentials.access_key)
builder.config("fs.s3n.awsSecretAccessKey", current_credentials.secret_key)
builder.config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2')
builder.config('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')

spark = SparkSession.builder.getOrCreate()

# Example filename for tests
EXAMPLE_FILE = 's3://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json'
url = str(EXAMPLE_FILE)




df = spark.read.json(EXAMPLE_FILE)
df.printSchema()

