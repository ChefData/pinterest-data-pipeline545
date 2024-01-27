# Databricks notebook source
# MAGIC %md
# MAGIC ## Clean User Batch Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import methods
# MAGIC
# MAGIC The following cell allows access to the methods from the DataCleaning class within the databricks_clean_data notebook

# COMMAND ----------

# MAGIC %run "../classes/databricks_clean_data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean df.user
# MAGIC
# MAGIC To clean the df_user DataFrame the following cell will perform the following transformations:
# MAGIC
# MAGIC - Create a new column user_name that concatenates the information found in the first_name and last_name columns
# MAGIC - Drop the first_name and last_name columns from the DataFrame
# MAGIC - Convert the date_joined column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns to have the following column order: (ind, user_name, age, date_joined)
# MAGIC

# COMMAND ----------

if __name__ == "__main__":
    df_user = spark.table("global_temp.df_user")
    data_cleaner = DataCleaning()
    cleaned_df_user = data_cleaner.clean_user_data(df_user)
    cleaned_df_user.createOrReplaceGlobalTempView("cleaned_df_user")
