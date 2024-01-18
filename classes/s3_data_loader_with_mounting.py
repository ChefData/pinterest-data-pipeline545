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
        encoded_secret_key = urllib.parse.quote(secret_key, safe="")
        return access_key, encoded_secret_key

    def is_mounted(self):
        return any(mount.mountPoint == self.mount_name for mount in dbutils.fs.mounts())

    def mount_s3_bucket(self, source_url):
        if not self.is_mounted():
            dbutils.fs.mount(source_url, self.mount_name)
            print(f"Mounted {source_url} to {self.mount_name}")
        else:
            print(f"Directory {self.mount_name} is already mounted.")
            # dbutils.fs.unmount(self.mount_name)

    def display_mounted_contents(self):
        display(dbutils.fs.ls(self.mount_name))

    def read_json_files(self, file_location):
        return (
            self.spark
            .read
            .option("inferSchema", "true")
            .json(file_location)
        )
