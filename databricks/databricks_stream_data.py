# Databricks notebook source
# MAGIC %run "./databricks_load_data"

# COMMAND ----------

# MAGIC %run "./databricks_clean_data"

# COMMAND ----------

if __name__ == "__main__":
    credentials_path = "dbfs:/user/hive/warehouse/authentication_credentials"
    iam_username = "0ab336d6fcf7"
    topics = ['pin', 'geo', 'user']
    data_loader = S3DataLoader(credentials_path, iam_username, topics)
    data_cleaner = DataCleaning()

# COMMAND ----------

if __name__ == "__main__":
    data_loader.create_stream_dataframes()

# COMMAND ----------

if __name__ == "__main__":
    cleaned_df_pin = data_cleaner.clean_pin_data(df_pin)
    print("Schema for original pin dataframe")
    df_pin.printSchema()
    print("Schema for cleaned pin dataframe")
    cleaned_df_pin.printSchema()

# COMMAND ----------

if __name__ == "__main__":
    cleaned_df_geo = data_cleaner.clean_geo_data(df_geo)
    print("Schema for original geo dataframe")
    df_geo.printSchema()
    print("Schema for cleaned geo dataframe")
    cleaned_df_geo.printSchema()

# COMMAND ----------

if __name__ == "__main__":
    cleaned_df_user = data_cleaner.clean_user_data(df_user)
    print("Schema for original user dataframe")
    df_user.printSchema()
    print("Schema for cleaned user dataframe")
    cleaned_df_user.printSchema()

# COMMAND ----------

if __name__ == "__main__":
    dbutils.fs.rm("/tmp/kinesis/0ab336d6fcf7_pin_table_checkpoints/", True)
    dbutils.fs.rm("/tmp/kinesis/0ab336d6fcf7_geo_table_checkpoints/", True)
    dbutils.fs.rm("/tmp/kinesis/0ab336d6fcf7_user_table_checkpoints/", True)

# COMMAND ----------

if __name__ == "__main__":
    data_loader.clear_delta_tables()

# COMMAND ----------

if __name__ == "__main__":
    data_loader.write_stream(cleaned_df_pin)
    data_loader.write_stream(cleaned_df_geo)
    data_loader.write_stream(cleaned_df_user)
