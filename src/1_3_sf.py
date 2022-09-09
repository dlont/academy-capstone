import os
import json



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
builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3')
builder.config('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')

spark = SparkSession.builder.getOrCreate()

# Example filename for tests
EXAMPLE_FILE = 's3://dataminded-academy-capstone-resources/raw/open_aq/*.json'
#EXAMPLE_FILE = 'data_part_1.json'
url = str(EXAMPLE_FILE)

# df = spark.read.json(EXAMPLE_FILE)
# df.printSchema()
# df.show()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, TimestampType

sensor_schema = StructType(fields=[
    StructField('city', StringType(), False),
    StructField(
        'coordinates', #ArrayType(
            StructType([
                StructField('latitude', DoubleType(), False),
                StructField('longitude', DoubleType(), False)
            ])
        # )
    ),
    StructField('country', StringType(), False),
    StructField(
        'date', #ArrayType(
            StructType([
                StructField('local', TimestampType(), False),
                StructField('utc', TimestampType(), False)
            ])
        # )
    ),
    StructField('entity', StringType(), False),
    StructField('isAnalysis', BooleanType(), False),
    StructField('isMobile', BooleanType(), False),
    StructField('location', StringType(), False),
    StructField('locationId', IntegerType(), False),
    StructField('parameter', StringType(), False),
    StructField('sensorType', StringType(), False),
    StructField('unit', StringType(), False),
    StructField('value', DoubleType(), False),

])

schema_df = spark.read.json(EXAMPLE_FILE, schema=sensor_schema)
flat_df = schema_df.select('city','country','coordinates.latitude','coordinates.longitude','date.local','date.utc','entity','isAnalysis','isMobile','location','locationId','parameter','sensorType','unit','value')

import botocore 
import botocore.session 
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

client = botocore.session.get_session().create_client('secretsmanager', region_name='eu-west-1')
cache_config = SecretCacheConfig()
cache = SecretCache( config = cache_config, client = client)

secret = cache.get_secret_string('snowflake/capstone/login')
secret = json.loads(secret)
print(secret)

SNOWFLAKE_SOURCE_NAME= "net.snowflake.spark.snowflake"

sfOptions = {
"sfURL"       : secret['URL'],
"sfAccount"   : "yw41113",
"sfUser"      : secret['USER_NAME'],
"sfPassword"  : secret['PASSWORD'],
"sfDatabase"  : secret['DATABASE'],
"sfSchema"    : "DENYS_LONTKOVSKYI",
"sfWarehouse" : secret['WAREHOUSE'],
"sfRole"      : secret['ROLE'],
}
flat_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "denys_aq").mode('append').options(header=True).save()
