# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0ab336d6fcf7-bucket"
# Mount name for the bucket
MOUNT_NAME = f"/mnt/{AWS_S3_BUCKET}"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

# COMMAND ----------

# Mount the drive
try:
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
except:
# If error try unmounting it first
    dbutils.fs.unmount(MOUNT_NAME)
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls(MOUNT_NAME))

# COMMAND ----------

# Disable format checks during the reading of Delta tables
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# File location and type - Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_pin = f"{MOUNT_NAME}/topics/0ab336d6fcf7.pin/partition=0/*.json"
file_location_geo  = f"{MOUNT_NAME}/topics/0ab336d6fcf7.geo/partition=0/*.json"
file_location_user = f"{MOUNT_NAME}/topics/0ab336d6fcf7.user/partition=0/*.json"

# Read in JSONs from mounted S3 bucket and ask Spark to infer the schema
df_pin = spark.read.format("json").option("inferSchema", "true").load(file_location_pin)
df_geo = spark.read.format("json").option("inferSchema", "true").load(file_location_geo)
df_user = spark.read.format("json").option("inferSchema", "true").load(file_location_user)

# Display Spark dataframe to check its content
display(df_pin)
display(df_geo)
display(df_user)
