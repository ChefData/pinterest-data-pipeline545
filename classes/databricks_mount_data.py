# Databricks notebook source
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from typing import Tuple, List, Dict
import urllib


class S3DataLoader:
    """
    Class for loading data from AWS S3 into PySpark DataFrames.

    Attributes:
    - spark (pyspark.sql.SparkSession): Spark session for handling PySpark operations.
    - delta_table_path (str): The path to the Delta table containing AWS keys.
    - iam_username (str): IAM username for constructing S3 bucket name.
    - aws_s3_bucket (str): The constructed AWS S3 bucket name based on IAM username.
    - mount_name (str): The mount point for the S3 bucket in the Spark environment.
    - topics (list): List of topics to load.
    """

    def __init__(self, delta_table_path: str, iam_username: str, topics: List[str]):
        """
        Initialize Spark session and set instance variables

        Parameters:
        - delta_table_path (str): The path to the Delta table containing AWS keys.
        - iam_username (str): IAM username for constructing S3 bucket name.
        - topics (list): List of topics to load.
        """
        self.spark: SparkSession = SparkSession.builder.appName("S3DataLoader").getOrCreate()
        self.delta_table_path: str = delta_table_path
        self.iam_username: str = iam_username
        self.aws_s3_bucket: str = f"user-{self.iam_username}-bucket"
        self.mount_name: str = f"/mnt/{self.aws_s3_bucket}"
        self.topics: List[str] = topics

    def __load_aws_keys(self) -> Tuple[str, str, str]:
        """
        Load AWS keys from Delta table.

        Returns:
        - tuple: A tuple containing source URL, access key, and encoded secret key.

        Raises:
        - Exception: If there is an issue loading AWS keys.
        """
        try:
            # Read access keys from Delta table
            keys_df = (self.spark.read.format("delta").load(self.delta_table_path).select("Access key ID", "Secret access key"))
            access_key, secret_key = keys_df.first()
            # URL encode the secret key
            encoded_secret_key: str = urllib.parse.quote(secret_key, safe="")
            # Construct the source URL for S3 access
            source_url: str = f"s3n://{access_key}:{encoded_secret_key}@{self.aws_s3_bucket}"
            return source_url, access_key, encoded_secret_key
        except Exception as error:
            raise Exception(f"Error loading AWS keys: {str(error)}")

    def __is_mounted(self) -> bool:
        """
        Check if the S3 bucket is already mounted.

        Returns:
        - bool: True if the bucket is mounted, False otherwise.

        Raises:
        - Exception: If there is an issue checking the mount status.
        """
        try:
            return any(mount.mountPoint == self.mount_name for mount in dbutils.fs.mounts())
        except Exception as error:
            raise Exception(f"Error checking mount status: {str(error)}")

    def mount_s3_bucket(self) -> None:
        """
        Mount the S3 bucket if not already mounted.

        Raises:
        - Exception: If there is an issue mounting the S3 bucket.
        """
        try:
            source_url, access_key, encoded_secret_key = self.__load_aws_keys()
            if not self.__is_mounted():
                dbutils.fs.mount(source_url, self.mount_name)
                print(f"Mounted Source URL to {self.mount_name}")
            else:
                print(f"Directory {self.mount_name} is already mounted.")
        except Exception as error:
            raise Exception(f"Error mounting S3 bucket: {str(error)}")

    def unmount_s3_bucket(self) -> None:
        """
        Unmount the S3 bucket.

        Raises:
        - Exception: If there is an issue unmounting the S3 bucket.
        """
        try:
            dbutils.fs.unmount(self.mount_name)
        except Exception as error:
            raise Exception(f"Error unmounting S3 bucket: {str(error)}")

    def display_s3_bucket(self) -> None:
        """
        Display the contents of the S3 bucket.

        Raises:
        - Exception: If there is an issue displaying S3 bucket contents.
        """
        try:
            display(dbutils.fs.ls(self.mount_name))
            for topic in self.topics:
                display(dbutils.fs.ls(f"{self.mount_name}/topics/topics/{self.iam_username}.{topic}/partition=0"))
        except Exception as error:
            raise Exception(f"Error displaying S3 bucket contents: {str(error)}")

    def __read_json_files(self, mounted: bool = True) -> Dict[str, SparkDataFrame]:
        """
        Read JSON files from the S3 bucket into PySpark DataFrames.

        Parameters:
        - mounted (bool): Flag to indicate if the S3 bucket is already mounted.

        Returns:
        - dict: A dictionary with topics as keys and PySpark DataFrames as values.

        Raises:
        - Exception: If there is an issue reading JSON files.
        """
        try:
            source_url, access_key, encoded_secret_key = self.__load_aws_keys()
            dataframes: Dict[str, SparkDataFrame] = {}
            for topic in self.topics:
                file_location: str  = (
                    f"{self.mount_name}/topics/topics/{self.iam_username}.{topic}/partition=0/*.json"
                    if mounted else
                    f"s3n://{access_key}:{encoded_secret_key}@{self.aws_s3_bucket}/topics/{self.iam_username}.{topic}/partition=0/*.json"
                )
                dataframes[topic] = (self.spark.read.option("inferSchema", "true").json(file_location))
            return dataframes
        except Exception as error:
            raise Exception(f"Error reading JSON files: {str(error)}")
    
    def create_dataframes(self, mounted: bool = True) -> None:
        """
        Create global DataFrames from the loaded JSON files.

        Parameters:
        - mounted (bool): Flag to indicate if the S3 bucket is already mounted.

        Raises:
        - Exception: If there is an issue creating global DataFrames.
        """
        try:
            dataframes: Dict[str, SparkDataFrame]  = self.__read_json_files(mounted)
            for topic, df in dataframes.items():
                globals()[f"df_{topic}"] = df
                print(f"Created dataFrame df_{topic} from {self.aws_s3_bucket}/topics/{self.iam_username}.{topic}")
        except Exception as error:
            raise Exception(f"Error creating global DataFrames: {str(error)}")


