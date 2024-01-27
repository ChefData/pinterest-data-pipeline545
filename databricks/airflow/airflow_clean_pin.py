# Databricks notebook source
# MAGIC %md
# MAGIC ## Clean Pin Batch Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import methods
# MAGIC
# MAGIC The following cell allows access to the methods from the DataCleaning class within the databricks_clean_data notebook

# COMMAND ----------

# MAGIC %run "../classes/databricks_clean_data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean df.pin
# MAGIC
# MAGIC To clean the df_pin DataFrame the following cell will perform the following transformations:
# MAGIC
# MAGIC - Replace empty entries and entries with no relevant data in each column with Nones
# MAGIC - Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
# MAGIC - Ensure that each column containing numeric data has a numeric data type
# MAGIC - Clean the data in the save_location column to include only the save location path
# MAGIC - Rename the index column to ind.
# MAGIC - Reorder the DataFrame columns to have the following column order: (ind, unique_id, title, description, follower_count, poster_name, tag_list, is_image_or_video, image_src, save_location, category) 

# COMMAND ----------

if __name__ == "__main__":
    df_pin  = spark.table("global_temp.df_pin")
    data_cleaner = DataCleaning()
    cleaned_df_pin = data_cleaner.clean_pin_data(df_pin)
    cleaned_df_pin.createOrReplaceGlobalTempView("cleaned_df_pin")
