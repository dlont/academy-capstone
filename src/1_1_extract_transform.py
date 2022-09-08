from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("1_1_extract_transform")

sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

# Example filename for tests

from boto3 import Session

session = Session()
credentials = session.get_credentials()
# Credentials are refreshable, so accessing your access key / secret key
# separately can lead to a race condition. Use this to get an actual matched
# set.
current_credentials = credentials.get_frozen_credentials()
print(current_credentials)



EXAMPLE_FILE = 's3://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json'
url = str(EXAMPLE_FILE)

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", current_credentials.access_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", current_credentials.secret_key)

df = sqlContext.read.json(EXAMPLE_FILE)
df.printSchema()

