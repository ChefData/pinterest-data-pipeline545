# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType, ArrayType, DateType
from typing import Tuple, List, Dict
import urllib.parse


class S3DataLoader:
    """
    Class for loading data from AWS S3 into PySpark DataFrames.
    """
    def __init__(self, credentials_path: str, iam_username: str, topics: List[str]):
        """
        Initialise Spark session and set instance variables

        Parameters:
        - credentials_path (str): The path to the credentials table containing AWS keys.
        - iam_username (str): IAM username for constructing S3 bucket name.
        - topics (List[str]): List of topics to load.

        Attributes:
        - spark (SparkSession): Spark session for handling PySpark operations.
        - credentials_path (str): The path to the credentials table containing AWS keys.
        - iam_username (str): IAM username for constructing S3 bucket name.
        - aws_s3_bucket (str): The constructed AWS S3 bucket name based on IAM username.
        - mount_name (str): The mount point for the S3 bucket in the Spark environment.
        - topics (List[str]): List of topics to load.
        - source_url (str): Source URL for S3 access.
        - access_key (str): AWS access key.
        - secret_key (str): AWS secret key.
        - encoded_secret_key (str): URL-encoded AWS secret key.

        Raises:
        - FileNotFoundError: If there is an issue loading AWS keys.
        - Exception: If there is an issue initialising the S3DataLoader.
        """
        self.spark: SparkSession = SparkSession.builder.appName("S3DataLoader").getOrCreate()
        self.credentials_path: str = credentials_path
        self.iam_username: str = iam_username
        self.aws_s3_bucket: str = f"user-{self.iam_username}-bucket"
        self.mount_name: str = f"/mnt/{self.aws_s3_bucket}"
        self.topics: List[str] = topics
        self.source_url, self.access_key, self.secret_key, self.encoded_secret_key = self.__load_aws_keys()

    def __load_aws_keys(self) -> Tuple[str, str, str, str]:
        """
        Load AWS keys from Delta table.

        Returns:
        - tuple: A tuple containing source URL, access key, and encoded secret key.

        Raises:
        - Exception: If there is an issue loading AWS keys.
        """
        try:
            # Read access keys from Delta table
            keys_df = self.spark.read.format("delta").load(self.credentials_path).select("Access key ID", "Secret access key")
            access_key, secret_key = keys_df.first() if keys_df.count() > 0 else (None, None)
            # URL encode the secret key
            encoded_secret_key: str = urllib.parse.quote(secret_key, safe="")
            # Construct the source URL for S3 access
            source_url: str = f"s3n://{access_key}:{encoded_secret_key}@{self.aws_s3_bucket}"
            return source_url, access_key, secret_key, encoded_secret_key
        except FileNotFoundError as file_not_found_error:
            raise FileNotFoundError(f"Error loading AWS keys: {file_not_found_error}")
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
            if not self.__is_mounted():
                dbutils.fs.mount(self.source_url, self.mount_name)
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
            display(dbutils.fs.ls(f"{self.mount_name}/topics"))
            for topic in self.topics:
                display(dbutils.fs.ls(f"{self.mount_name}/topics/{self.iam_username}.{topic}/partition=0"))
        except Exception as error:
            raise Exception(f"Error displaying S3 bucket contents: {str(error)}")

    def __read_json_files(self, mounted: bool = True) -> Dict[str, DataFrame]:
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
            dataframes: Dict[str, DataFrame] = {}
            for topic in self.topics:
                file_location: str  = (
                    f"{self.mount_name}/topics/{self.iam_username}.{topic}/partition=0/*.json"
                    if mounted else
                    f"s3n://{self.access_key}:{self.encoded_secret_key}@{self.aws_s3_bucket}/topics/{self.iam_username}.{topic}/partition=0/*.json"
                )
                dataframes[topic] = self.spark.read.option("inferSchema", "true").json(file_location)
            return dataframes
        except Exception as error:
            raise Exception(f"Error reading JSON files: {str(error)}")
    
    def create_dataframes(self, mounted: bool = True) -> None:
        """
        Create global DataFrames from JSON files in the S3 bucket.

        Parameters:
        - mounted (bool): Flag to indicate if the S3 bucket is already mounted. Defaults to True.

        Raises:
        - FileNotFoundError: If a file specified in the path is not found.
        - Exception: If there is an issue creating or optimising global DataFrames.
        """
        try:
            dataframes: Dict[str, DataFrame]  = self.__read_json_files(mounted)
            for topic, df in dataframes.items():
                # Create a global temporary view of the Delta table
                globals()[f"df_{topic}"] = df
                df.createOrReplaceGlobalTempView(f"df_{topic}")
                print(f"Created DataFrame df_{topic} from {self.aws_s3_bucket}/topics/{self.iam_username}.{topic}")
        except FileNotFoundError as file_not_found_error:
            raise FileNotFoundError(f"File not found: {file_not_found_error}")
        except Exception as error:
            raise Exception(f"Error creating global DataFrames: {str(error)}")
    
    def __read_stream_files(self) -> Dict[str, DataFrame]:
        """
        Read streaming data from AWS Kinesis and return a dictionary of PySpark DataFrames.

        Returns:
        - Dict[str, DataFrame]: A dictionary with topics as keys and PySpark DataFrames as values.

        Raises:
        - Exception: If there is an error reading the streaming data.
        """
        try:
            dataframes: Dict[str, DataFrame] = {}
            for topic in self.topics:
                stream_name = f"streaming-{self.iam_username}-{topic}"
                df: DataFrame = (
                    self.spark.readStream.format('kinesis')
                    .option('streamName', stream_name)
                    .option('initialPosition', 'earliest')
                    .option('region', 'us-east-1')
                    .option('awsAccessKey', self.access_key)
                    .option('awsSecretKey', self.secret_key)
                    .load()
                )
                dataframes[topic] = df
            return dataframes
        except Exception as error:
            raise Exception(f"Error reading JSON files: {str(error)}")

    def create_stream_dataframes(self) -> None:
        """
        Process streaming data and create global temporary views of Delta tables.

        Raises:
        - FileNotFoundError: If a file specified in the path is not found.
        - Exception: If there is an error creating global DataFrames.
        """
        try:
            dataframes: Dict[str, DataFrame] = self.__read_stream_files()
            for topic, df in dataframes.items():
                # Cast 'data' column to string
                df = df.withColumn("data", col("data").cast(StringType()))
                
                # Explode the 'data' array to separate rows
                df = df.select(explode(from_json("data", ArrayType(StringType()))).alias("json_data"))

                # Define the schema for the JSON data
                schema_mapping: Dict[str, StructType]  = {
                    'pin': self.__get_pin_schema(),
                    'geo': self.__get_geo_schema(),
                    'user': self.__get_user_schema()
                }

                # Apply the schema to the exploded 'json_data' column
                df = df.select(from_json("json_data", schema_mapping[topic]).alias("data")).select("data.*")

                # Create a global temporary view of the Delta table
                table_name = f"df_{topic}"
                globals()[table_name]: DataFrame = df
                df.createOrReplaceGlobalTempView(table_name)
                print(f"Created DataFrame {table_name}")
        except FileNotFoundError as file_not_found_error:
            raise FileNotFoundError(f"File not found: {file_not_found_error}")
        except Exception as error:
            raise Exception(f"Error creating global DataFrames: {str(error)}")

    @staticmethod
    def __get_pin_schema() -> StructType:
        """
        Define the schema for 'pin' data.

        Returns:
        - StructType: The schema for the 'pin' data.

        Raises:
        - Exception: If there is an error defining the schema.
        """
        try:
            return StructType([
                StructField("index", IntegerType(), True),
                StructField("unique_id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("poster_name", StringType(), True),
                StructField("follower_count", StringType(), True),
                StructField("tag_list", StringType(), True),
                StructField("is_image_or_video", StringType(), True),
                StructField("image_src", StringType(), True),
                StructField("downloaded", IntegerType(), True),
                StructField("save_location", StringType(), True),
                StructField("category", StringType(), True)
            ])
        except Exception as error:
            raise Exception(f"Error defining 'pin' schema: {str(error)}")


    @staticmethod
    def __get_geo_schema() -> StructType:
        """
        Define the schema for 'geo' data.

        Returns:
        - StructType: The schema for the 'geo' data.

        Raises:
        - Exception: If there is an error defining the schema.
        """
        try:
            return StructType([
                StructField("ind", IntegerType()),
                StructField("timestamp", TimestampType()),
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType()),
                StructField("country", StringType())
            ])
        except Exception as error:
            raise Exception(f"Error defining 'geo' schema: {str(error)}")

    @staticmethod
    def __get_user_schema() -> StructType:
        """
        Define the schema for 'user' data.

        Returns:
        - StructType: The schema for the 'user' data.

        Raises:
        - Exception: If there is an error defining the schema.
        """
        try:
            return StructType([
                StructField("ind", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("date_joined", DateType(), True)
            ])
        except Exception as error:
            raise Exception(f"Error defining 'user' schema: {str(error)}")

    def write_stream(self, df: DataFrame) -> None:
        """
        Write streaming DataFrame to Delta table.

        Parameters:
        - df (DataFrame): The PySpark DataFrame to be written.

        Raises:
        - Exception: If there is an error writing the streaming DataFrame.
        """
        try:
            for topic in self.topics:
                table_name: str = f"{self.iam_username}_{topic}_table"
                df.writeStream \
                    .format("delta") \
                    .outputMode("append") \
                    .option("checkpointLocation", f"/tmp/kinesis/{table_name}_checkpoints/") \
                    .option("mergeSchema", "true") \
                    .table(table_name)
        except Exception as error:
            raise Exception(f"Error writing streaming DataFrame: {str(error)}")


    def clear_delta_tables(self) -> None:
        """
        Clear Delta table checkpoints.

        Raises:
        - Exception: If there is an error clearing Delta table checkpoints.
        """
        try:
            for topic in self.topics:
                table_name: str = f"{self.iam_username}_{topic}_table"
                dbutils.fs.rm(f"/tmp/kinesis/{table_name}_checkpoints/", True)
        except Exception as error:
            raise Exception(f"Error clearing Delta table checkpoints: {str(error)}")


