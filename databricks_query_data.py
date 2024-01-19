# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Data Processing using Spark on Databricks
# MAGIC
# MAGIC - Mount batch data
# MAGIC - Clean batch data
# MAGIC - Query batch data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Batch Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import methods 
# MAGIC
# MAGIC The following cell allows access to the methods from the S3DataLoader class within the databricks_mount_data notebook.

# COMMAND ----------

# MAGIC %run "./classes/databricks_mount_data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Instantiate S3DataLoader
# MAGIC
# MAGIC The following cell instantiates the required variables for the S3DataLoader class.

# COMMAND ----------

if __name__ == "__main__":
    delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
    iam_username = "0ab336d6fcf7"
    topics = ['pin', 'geo', 'user']
    data_loader = S3DataLoader(delta_table_path, iam_username, topics)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display unmounted data
# MAGIC
# MAGIC Databricks no longer recommends mounting external data locations to Databricks Filesystem.
# MAGIC
# MAGIC The following cell has been supplyed as an alternative method to accessing the data.
# MAGIC
# MAGIC If this cell is used, the rest of the steps within **Mount Batch Data** can be skipped.

# COMMAND ----------

if __name__ == "__main__":
    data_loader.create_dataframes(mounted=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount S3 bucket
# MAGIC
# MAGIC As per the project instructions the following cell mounts an external S3 bucket to Databricks Filesystem.

# COMMAND ----------

if __name__ == "__main__":
    # Load AWS keys and configure S3 bucket
    data_loader.mount_s3_bucket()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Show file structure
# MAGIC
# MAGIC The following cell will show the file structure within the mount path.

# COMMAND ----------

if __name__ == "__main__":
    # Display the mounted S3 bucket
    data_loader.display_s3_bucket()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create dataframes
# MAGIC
# MAGIC The following cell will create three dataframes from the data stored in the mount path.

# COMMAND ----------

if __name__ == "__main__":
    # Create dataframes
    data_loader.create_dataframes(mounted=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unmount S3 Bucket
# MAGIC
# MAGIC If required, the following cell will unmount the S3 bucket from Databricks Filesystem.

# COMMAND ----------

if __name__ == "__main__":
    data_loader.unmount_s3_bucket()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Batch Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import methods
# MAGIC
# MAGIC The following cell allows access to the methods from the DataCleaning class within the databricks_clean_data notebook

# COMMAND ----------

# MAGIC %run "./classes/databricks_clean_data"

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
    data_cleaner = DataCleaning()
    cleaned_df_pin = data_cleaner.clean_pin_data(df_pin)

    print("Schema for original dataframe")
    df_pin.printSchema()
    print("Schema for cleaned dataframe")
    cleaned_df_pin.printSchema()

    print("Original dataframe:")
    display(df_pin)
    print("Cleaned dataframe:")
    display(cleaned_df_pin)

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
    data_cleaner = DataCleaning()
    cleaned_df_geo = data_cleaner.clean_geo_data(df_geo)

    print("Schema for original dataframe")
    df_geo.printSchema()
    print("Schema for cleaned dataframe")
    cleaned_df_geo.printSchema()

    print("Original dataframe:")
    display(df_geo)
    print("Cleaned dataframe:")
    display(cleaned_df_geo)

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
    data_cleaner = DataCleaning()
    cleaned_df_user = data_cleaner.clean_user_data(df_user)

    print("Schema for original dataframe")
    df_user.printSchema()
    print("Schema for cleaned dataframe")
    cleaned_df_user.printSchema()
    
    print("Original dataframe:")
    display(df_user)
    print("Cleaned dataframe:")
    display(cleaned_df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Batch Data
# MAGIC
# MAGIC Before querieing the data the three dataframes (df_pin, df_geo, and df_user) are joined together on the common column heading 'ind'.
# MAGIC
# MAGIC To make sure that df_all is a valid DataFrame it will be created and registered as a temporary table or view before executing any SQL queries. To do this df_all is registered as a temporary view using df_all.createOrReplaceTempView("df_all").
# MAGIC
# MAGIC However df_all is a non-Delta table with many small files. Therefore to improve the performance of queries, df_all has been converted to Delta with the OPTIMIZE command. The new df_optimised table will accelerate queries.

# COMMAND ----------

from delta.tables import DeltaTable

# Join the three dataframes
df_all = cleaned_df_pin.join(cleaned_df_geo, 'ind').join(cleaned_df_user, 'ind')

# Write the Delta table
delta_table_path = "/delta/my_table/v1"
df_all.write.format("delta").mode("overwrite").save(delta_table_path)

# Use DeltaTable API to optimize the Delta table
delta_table = DeltaTable.forPath(spark, delta_table_path)
delta_table.vacuum()
df_optimised = delta_table.toDF()

# Create a temperory table of the combined dataframes
df_optimised.createOrReplaceTempView("df_optimised")

# Display the optimized Delta tables
display(df_optimised)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 1: Find the most popular category in each country
# MAGIC
# MAGIC - Find the most popular Pinterest category people post to based on their country.
# MAGIC - The query should return a DataFrame that contains the following columns: (country, category, category_count)
# MAGIC

# COMMAND ----------

top_category_per_country = spark.sql("""
    WITH ranked_categories AS (
        SELECT
            country, 
            category, 
            COUNT(category) AS category_count, 
            RANK() OVER (PARTITION BY country ORDER BY count(category) DESC) AS category_rank 
        FROM df_optimised 
        GROUP BY country, category 
    ) 
    SELECT country, category, category_count 
    FROM ranked_categories 
    WHERE category_rank = 1 
    ORDER BY country
""")
display(top_category_per_country)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 2: Find which was the most popular category each year
# MAGIC
# MAGIC - Find how many posts each category had between 2018 and 2022.
# MAGIC - The query will return a DataFrame that contains the following columns: (post_year, category, category_count)
# MAGIC

# COMMAND ----------

top_category_per_year = spark.sql("""
    WITH ranked_categories AS (
        SELECT
            category,
            EXTRACT(YEAR FROM timestamp) AS post_year,
            COUNT(category) AS category_count,
            RANK() OVER (PARTITION BY EXTRACT(YEAR FROM timestamp) ORDER BY count(category) DESC) AS category_rank 
        FROM df_optimised 
        GROUP BY EXTRACT(YEAR FROM timestamp), category
    )
    SELECT post_year, category, category_count 
    FROM ranked_categories 
    WHERE category_rank = 1 AND post_year BETWEEN 2018 AND 2022
    ORDER BY post_year
""")
display(top_category_per_year)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 3: Find the user with most followers in each country
# MAGIC
# MAGIC - Step 1: For each country find the user with the most followers.
# MAGIC   - Your query should return a DataFrame that contains the following columns: (country, poster_name, follower_count)
# MAGIC - Step 2: Based on the above query, find the country with the user with most followers.
# MAGIC   - Your query should return a DataFrame that contains the following columns: (country, follower_count)
# MAGIC   - This DataFrame should have only one entry.

# COMMAND ----------

# Step 1:
top_user_per_country = spark.sql("""
    SELECT country, poster_name, MAX(follower_count) AS follower_count
    FROM df_optimised
    GROUP BY country, poster_name
    ORDER BY follower_count DESC
""")
display(top_user_per_country)

# Step 2:
country_with_top_user = spark.sql("""
    WITH top_users AS (
        SELECT country, poster_name, MAX(follower_count) AS follower_count
        FROM df_optimised
        GROUP BY country, poster_name
        ORDER BY follower_count DESC
    )
    SELECT country, follower_count
    FROM top_users
    ORDER BY follower_count DESC
    LIMIT 1
""")
display(country_with_top_user)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 4: Find the most popular category for different age groups
# MAGIC
# MAGIC - What is the most popular category people post to based on the following age groups: (18-24, 25-35, 36-50, +50)
# MAGIC - The query should return a DataFrame that contains the following columns: (age_group, category, category_count)

# COMMAND ----------

top_category_per_age = spark.sql("""
    WITH age_groups AS (
        SELECT 
            CASE 
                WHEN Age BETWEEN 18 AND 24 THEN '18-24'
                WHEN Age BETWEEN 25 AND 35 THEN '25-35'
                WHEN Age BETWEEN 36 AND 50 THEN '36-50'
                WHEN Age >= 51 THEN '50+'
                ELSE 'NA'
            END AS age_group,
            category, 
            COUNT(category) AS category_count
        FROM df_optimised
        GROUP BY age_group, category
    ),
    ranked_ages AS (
        SELECT 
            age_group, 
            category, 
            category_count, 
            RANK() OVER (PARTITION BY age_group ORDER BY category_count DESC) AS category_rank
        FROM age_groups
    )
    SELECT age_group, category, category_count
    FROM ranked_ages
    WHERE category_rank = 1
""")
display(top_category_per_age)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 5: Find the median follower count for different age groups
# MAGIC
# MAGIC - What is the median follower count for users in the following age groups: (18-24, 25-35, 36-50, +50)
# MAGIC - The query should return a DataFrame that contains the following columns: (age_group, median_follower_count)
# MAGIC

# COMMAND ----------

median_follower_per_age_group = spark.sql("""
    SELECT 
        CASE 
            WHEN Age BETWEEN 18 AND 24 THEN '18-24'
            WHEN Age BETWEEN 25 AND 35 THEN '25-35'
            WHEN Age BETWEEN 36 AND 50 THEN '36-50'
            WHEN Age >= 51 then '50+'
            ELSE 'NA'
        End as age_group,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY follower_count) AS median_follower_count 
    FROM df_optimised
    GROUP BY age_group
    ORDER BY median_follower_count DESC
""")
display(median_follower_per_age_group)                                                                

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 6: Find how many users have joined each year?
# MAGIC
# MAGIC - Find how many users have joined between 2015 and 2020.
# MAGIC - The query should return a DataFrame that contains the following columns: (post_year, number_users_joined)

# COMMAND ----------

users_joined_per_year = spark.sql("""
    SELECT
        EXTRACT(YEAR FROM date_joined) AS post_year,
        COUNT(DISTINCT(user_name)) AS number_users_joined
    FROM df_optimised 
    WHERE EXTRACT(YEAR FROM date_joined) BETWEEN 2015 AND 2020
    GROUP BY post_year
""")
display(users_joined_per_year)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 7: Find the median follower count of users based on their joining year
# MAGIC
# MAGIC - Find the median follower count of users have joined between 2015 and 2020.
# MAGIC - Your query should return a DataFrame that contains the following columns: (post_year, median_follower_count)

# COMMAND ----------

median_follower_per_join_year = spark.sql("""
    SELECT
        EXTRACT(YEAR FROM date_joined) AS post_year,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY follower_count) AS median_follower_count
    FROM df_optimised 
    WHERE EXTRACT(YEAR FROM date_joined) BETWEEN 2015 AND 2020
    GROUP BY post_year
""")
display(median_follower_per_join_year)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Question 8: Find the median follower count of users based on their joining year and age group
# MAGIC
# MAGIC - Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
# MAGIC - The query should return a DataFrame that contains the following columns: (age_group, post_year, median_follower_count)

# COMMAND ----------

median_follower_per_join_year_age_group = spark.sql("""
    SELECT
        CASE 
            WHEN Age BETWEEN 18 AND 24 THEN '18-24'
            WHEN Age BETWEEN 25 AND 35 THEN '25-35'
            WHEN Age BETWEEN 36 AND 50 THEN '36-50'
            WHEN Age >= 51 then '50+'
            ELSE 'NA'
        End as age_group,
        EXTRACT(YEAR FROM date_joined) AS post_year,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY follower_count) AS median_follower_count
    FROM df_optimised 
    WHERE EXTRACT(YEAR FROM date_joined) BETWEEN 2015 AND 2020
    GROUP BY age_group, post_year
    Order by age_group, post_year
""")
display(median_follower_per_join_year_age_group)
