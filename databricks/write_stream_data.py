# Databricks notebook source
# MAGIC %md
# MAGIC # Stream Data Processing using Kinesis on Databricks
# MAGIC
# MAGIC Kinesis Data Streams is a highly customisable AWS streaming solution. Highly customisable means that all parts involved with stream processing, such as data ingestion, monitoring, elasticity, and consumption are done programmatically when creating the stream.
# MAGIC
# MAGIC For this project three data streams were created using Kinesis Data Streams, one for each Pinterest table.
# MAGIC
# MAGIC - `streaming-0ab336d6fcf7-pin`
# MAGIC - `streaming-0ab336d6fcf7-geo`
# MAGIC - `streaming-0ab336d6fcf7-user`
# MAGIC
# MAGIC This notebook will clean the data from the three Kinesis Data Streams and write the data to Delta Tables. Within Databricks three Delta Tables will be created to hold this data:
# MAGIC
# MAGIC - `0ab336d6fcf7_pin_table` for the Pinterest post data
# MAGIC - `0ab336d6fcf7_geo_table` for the geolocation data
# MAGIC - `0ab336d6fcf7_user_table` for the user data.
# MAGIC
# MAGIC This notebook will falcitate the following procedures:
# MAGIC
# MAGIC - Load the stream data
# MAGIC - Clean the stream data
# MAGIC - Write the stream data to Delta Tables
# MAGIC
# MAGIC This notebook uses the ***loading*** methods and dataframe creation methods from the `databricks_load_data` notebook located in the `classes` folder.
# MAGIC
# MAGIC This notebook also uses the dataframe cleaning methods from the `databricks_clean_data` file also located in the `classes` folder.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import loading methods 
# MAGIC
# MAGIC The following cell allows access to the methods from the `S3DataLoader` class within the `databricks_load_data` notebook.

# COMMAND ----------

# MAGIC %run "./classes/databricks_load_data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import cleaning methods
# MAGIC
# MAGIC The following cell allows access to the methods from the `DataCleaning` class within the `databricks_clean_data` notebook

# COMMAND ----------

# MAGIC %run "./classes/databricks_clean_data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Instantiate S3DataLoader and DataCleaning
# MAGIC
# MAGIC The following cell instantiates the required variables for the `S3DataLoader` class and `DataCleaning` class.

# COMMAND ----------

if __name__ == "__main__":
    credentials_path = "dbfs:/user/hive/warehouse/authentication_credentials"
    iam_username = "0ab336d6fcf7"
    topics = ['pin', 'geo', 'user']
    data_loader = S3DataLoader(credentials_path, iam_username, topics)
    data_cleaner = DataCleaning()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Dataframes
# MAGIC
# MAGIC The following cell will create three dataframes from the data stored in the S3 bucket.

# COMMAND ----------

if __name__ == "__main__":
    data_loader.create_stream_dataframes()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Stream Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean df.pin
# MAGIC
# MAGIC To clean the `df_pin` DataFrame the following cell will perform the following transformations:
# MAGIC
# MAGIC - Replace empty entries and entries with no relevant data in each column with `Nones`
# MAGIC - Perform the necessary transformations on the `follower_count` to ensure every entry is a number. Make sure the data type of this column is an `int`.
# MAGIC - Ensure that each column containing numeric data has a `numeric` data type
# MAGIC - Clean the data in the `save_location` column to include only the save location path
# MAGIC - Rename the `index` column to `ind`.
# MAGIC - Reorder the DataFrame columns to have the following column order: (`ind`, `unique_id`, `title`, `description`, `follower_count`, `poster_name`, `tag_list`, `is_image_or_video`, `image_src`, `save_location`, `category`) 

# COMMAND ----------

if __name__ == "__main__":
    cleaned_df_pin = data_cleaner.clean_pin_data(df_pin)
    print("Schema for original pin dataframe")
    df_pin.printSchema()
    print("Schema for cleaned pin dataframe")
    cleaned_df_pin.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean df.geo
# MAGIC
# MAGIC To clean the `df_geo` DataFrame the follwoing cell will perform the following transformations:
# MAGIC
# MAGIC - Create a new column `coordinates` that contains an array based on the `latitude` and `longitude` columns
# MAGIC - Drop the `latitude` and `longitude` columns from the DataFrame
# MAGIC - Convert the `timestamp` column from a `string` to a `timestamp` data type
# MAGIC - Reorder the DataFrame columns to have the following column order: (`ind`, `country`, `coordinates`, `timestamp`)

# COMMAND ----------

if __name__ == "__main__":
    cleaned_df_geo = data_cleaner.clean_geo_data(df_geo)
    print("Schema for original geo dataframe")
    df_geo.printSchema()
    print("Schema for cleaned geo dataframe")
    cleaned_df_geo.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean df.user
# MAGIC
# MAGIC To clean the `df_user` DataFrame the following cell will perform the following transformations:
# MAGIC
# MAGIC - Create a new column user_name that concatenates the information found in the `first_name` and `last_name` columns
# MAGIC - Drop the `first_name` and `last_name` columns from the DataFrame
# MAGIC - Convert the `date_joined` column from a `string` to a `timestamp` data type
# MAGIC - Reorder the DataFrame columns to have the following column order: (`ind`, `user_name`, `age`, `date_joined`)
# MAGIC

# COMMAND ----------

if __name__ == "__main__":
    cleaned_df_user = data_cleaner.clean_user_data(df_user)
    print("Schema for original user dataframe")
    df_user.printSchema()
    print("Schema for cleaned user dataframe")
    cleaned_df_user.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stream Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clear checkpoint
# MAGIC
# MAGIC The .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") allows the previous state of a query to be recovered in case of failure. 
# MAGIC
# MAGIC Before running the writeStream function again, the following cell will need activated to delete the checkpoint folder.

# COMMAND ----------

if __name__ == "__main__":
    data_loader.clear_delta_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the streaming data to Delta Tables
# MAGIC
# MAGIC Once the streaming data has been cleaned, each stream will be saved in a Delta Table:
# MAGIC
# MAGIC - `0ab336d6fcf7_pin_table` for the Pinterest post data
# MAGIC - `0ab336d6fcf7_geo_table` for the geolocation data
# MAGIC - `0ab336d6fcf7_user_table` for the user data.

# COMMAND ----------

if __name__ == "__main__":
    data_loader.write_stream(cleaned_df_pin)
    data_loader.write_stream(cleaned_df_geo)
    data_loader.write_stream(cleaned_df_user)
