# Databricks notebook source
# MAGIC %md
# MAGIC ## Clean Geo Batch Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import methods
# MAGIC
# MAGIC The following cell allows access to the methods from the DataCleaning class within the databricks_clean_data notebook

# COMMAND ----------

# MAGIC %run "../classes/databricks_clean_data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean df.geo
# MAGIC
# MAGIC To clean the df_geo DataFrame the follwoing cell will perform the following transformations:
# MAGIC
# MAGIC - Create a new column coordinates that contains an array based on the latitude and longitude columns
# MAGIC - Drop the latitude and longitude columns from the DataFrame
# MAGIC - Convert the timestamp column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns to have the following column order: (ind, country, coordinates, timestamp)
# MAGIC

# COMMAND ----------

if __name__ == "__main__":
    df_geo  = spark.table("global_temp.df_geo")
    data_cleaner = DataCleaning()
    cleaned_df_geo = data_cleaner.clean_geo_data(df_geo)
    cleaned_df_geo.createOrReplaceGlobalTempView("cleaned_df_geo")