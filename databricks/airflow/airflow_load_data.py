# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Batch Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import methods 
# MAGIC
# MAGIC The following cell allows access to the methods from the S3DataLoader class within the databricks_mount_data notebook.

# COMMAND ----------

# MAGIC %run "../classes/databricks_load_data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Instantiate S3DataLoader
# MAGIC
# MAGIC The following cell instantiates the required variables for the S3DataLoader class.

# COMMAND ----------

if __name__ == "__main__":
    credentials_path = "dbfs:/user/hive/warehouse/authentication_credentials"
    iam_username = "0ab336d6fcf7"
    topics = ['pin', 'geo', 'user']
    data_loader = S3DataLoader(credentials_path, iam_username, topics)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data

# COMMAND ----------

if __name__ == "__main__":
    data_loader.create_dataframes(mounted=False)
