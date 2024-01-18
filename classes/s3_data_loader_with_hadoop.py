# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import urllib

class S3DataLoader:
    def __init__(self, delta_table_path, aws_s3_bucket):
        self.spark = SparkSession.builder.appName("S3DataLoader").getOrCreate()
        self.delta_table_path = delta_table_path
        self.aws_s3_bucket = aws_s3_bucket
        self.mount_name = f"/mnt/{aws_s3_bucket}"

    def load_aws_keys(self):
        access_key, secret_key = (
            self.spark.read.format("delta")
            .load(self.delta_table_path)
            .select("Access key ID", "Secret access key")
            .first()
        )
        encoded_secret_key = urllib.parse.quote(string=secret_key, safe="")
        return access_key, encoded_secret_key

    def mount_s3_bucket(self, source_url, mount_name):
        try:
            access_key, encoded_secret_key = self.load_aws_keys()

            s3_conf = f"fs.azure.s3n.{access_key}"
            self.spark.conf.set(f"{s3_conf}.access.key", access_key)
            self.spark.conf.set(f"{s3_conf}.secret.key", encoded_secret_key)
            self.spark.conf.set(f"{s3_conf}.endpoint", f"s3n://{self.aws_s3_bucket}")

            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            hadoop_conf.set(f"{s3_conf}.access.key", access_key)
            hadoop_conf.set(f"{s3_conf}.secret.key", encoded_secret_key)
            hadoop_conf.set(f"{s3_conf}.endpoint", f"s3n://{self.aws_s3_bucket}")

            self.spark.read.text(source_url).count()
            
        except Exception as e:
            self.spark._jvm.scala.Predef.println(f"Error mounting {mount_name}: {e}")
            self.spark.conf.unset(f"{s3_conf}.access.key")
            self.spark.conf.unset(f"{s3_conf}.secret.key")
            self.spark.conf.unset(f"{s3_conf}.endpoint")

    def display_mounted_contents(self):
        display(dbutils.fs.ls(self.mount_name))

    def read_json_files(self, file_location):
        return (
            self.spark
            .read
            .option("inferSchema", "true")
            .json(file_location)
        )
