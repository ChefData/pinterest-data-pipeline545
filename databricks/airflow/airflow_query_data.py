# Databricks notebook source
# MAGIC %md
# MAGIC ## Query Batch Data
# MAGIC
# MAGIC Before querieing the data the three dataframes (`df_pin`, `df_geo`, and `df_user`) are joined together on the common column heading `ind`.
# MAGIC
# MAGIC To make sure that `df_all` is a valid DataFrame it will be created and registered as a temporary table or view before executing any SQL queries. To do this `df_all` is registered as a temporary view using `df_all.createOrReplaceTempView("df_all")`.
# MAGIC
# MAGIC However `df_all` is a non-Delta table with many small files. Therefore to improve the performance of queries, `df_all` has been converted to Delta. The new `df_all` table will accelerate queries.

# COMMAND ----------

if __name__ == "__main__":
    cleaned_df_pin = spark.table("global_temp.cleaned_df_pin")
    cleaned_df_geo = spark.table("global_temp.cleaned_df_geo")
    cleaned_df_user = spark.table("global_temp.cleaned_df_user")

# COMMAND ----------

from delta.tables import DeltaTable

if __name__ == "__main__":
    # Join the three dataframes
    df_all = cleaned_df_pin.join(cleaned_df_geo, 'ind').join(cleaned_df_user, 'ind')

    # Write the Delta table
    delta_table_path = "/delta/my_table/v1"
    df_all.write.format("delta").mode("overwrite").save(delta_table_path)

    # Use DeltaTable API to optimize the Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_table.vacuum()
    df_all = delta_table.toDF()

    # Create a temperory table of the combined dataframes
    df_all.createOrReplaceTempView("df_all")


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
        FROM df_all 
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
        FROM df_all 
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
    FROM df_all
    GROUP BY country, poster_name
    ORDER BY follower_count DESC
""")
display(top_user_per_country)

# Step 2:
country_with_top_user = spark.sql("""
    WITH top_users AS (
        SELECT country, poster_name, MAX(follower_count) AS follower_count
        FROM df_all
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
        FROM df_all
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
    FROM df_all
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
    FROM df_all 
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
    FROM df_all 
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
    FROM df_all 
    WHERE EXTRACT(YEAR FROM date_joined) BETWEEN 2015 AND 2020
    GROUP BY age_group, post_year
    Order by age_group, post_year
""")
display(median_follower_per_join_year_age_group)
