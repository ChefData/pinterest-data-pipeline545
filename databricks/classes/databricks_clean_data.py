# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col, array, concat, lit


class DataCleaning:
    """Class for cleaning data in PySpark DataFrames."""

    @staticmethod
    def clean_pin_data(df: DataFrame) -> DataFrame:
        """
        Clean pin data in the DataFrame.

        Parameters:
        - df (pyspark.sql.DataFrame): Input DataFrame.

        Returns:
        - pyspark.sql.DataFrame: Cleaned DataFrame.

        """
        # Drop duplicates in the DataFrame
        df = df.dropDuplicates()

        # Replace specific values in each column with None
        replace_dict: dict = {
            "description": {
                "Untitled": None, 
                "No description available Story format": None,
                "No description available": None, 
                "Art.": None
            },
            "follower_count": {"User Info Error": None},
            "image_src": {'Image src error.': None},
            "poster_name": {'User Info Error': None},
            "title": {'No Title Data Available': None},
            "tag_list": {'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}
        }
        for col_name, replace_values in replace_dict.items():
            df = df.replace(replace_values, subset=[col_name])

        # Perform regular expression replacements for specific columns
        regexp_replace_dict: dict = {
            "follower_count": {"k": "000", "M": "000000"},
            "save_location": {"Local save in": ""}
        }
        for col_name, replace_values in regexp_replace_dict.items():
            # Loop through the nested dictionary for multiple replacements
            for find, replace in replace_values.items():
                df = df.withColumn(col_name, regexp_replace(df[col_name], find, replace))

        # Ensure numeric columns have the correct data type
        numeric_columns: list = ["follower_count", "downloaded", "index"]
        for col_name in numeric_columns:
            df = df.withColumn(col_name, col(col_name).cast("int"))

        # Rename the 'index' column to 'ind'
        df = df.withColumnRenamed("index", "ind")

        # Reorder the DataFrame columns
        column_order: list = ["ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category"]
        df = df.select(column_order)

        return df

    @staticmethod    
    def clean_geo_data(df: DataFrame) -> DataFrame:
        """
        Clean geo data in the DataFrame.

        Parameters:
        - df (pyspark.sql.DataFrame): Input DataFrame.

        Returns:
        - pyspark.sql.DataFrame: Cleaned DataFrame.

        """
        # Drop duplicates in the DataFrame
        df = df.dropDuplicates()

        # Create a new 'coordinates' column as an array of 'latitude' and 'longitude', then drop 'latitude' and 'longitude'
        df = df.withColumn("coordinates", array("latitude", "longitude")).drop("latitude", "longitude")

        # Convert the 'timestamp' column from string to timestamp data type
        df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

        # Reorder the DataFrame columns
        column_order: list = ["ind", "country", "coordinates", "timestamp"]
        df = df.select(column_order)

        return df

    @staticmethod    
    def clean_user_data(df: DataFrame) -> DataFrame:
        """
        Clean user data in the DataFrame.

        Parameters:
        - df (pyspark.sql.DataFrame): Input DataFrame.

        Returns:
        - pyspark.sql.DataFrame: Cleaned DataFrame.

        """
        # Drop duplicates in the DataFrame
        df = df.dropDuplicates()

        # Create a new 'user_name' column by concatenating 'first_name' and 'last_name', then drop 'first_name' and 'last_name'
        df = df.withColumn("user_name", concat(col("first_name"), lit(" "), col("last_name"))).drop("first_name", "last_name")

        # Convert the 'date_joined' column from string to timestamp data type
        df = df.withColumn("date_joined", col("date_joined").cast("timestamp"))

        # Reorder the DataFrame columns
        column_order: list = ["ind", "user_name", "age", "date_joined"]
        df = df.select(column_order)

        return df

