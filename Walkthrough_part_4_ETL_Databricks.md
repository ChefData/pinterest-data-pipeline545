# Walkthrough of Pinterest Data Pipeline Project: Part 4

Pinterest crunches billions of data points daily to decide how to provide more value to its users.

This walkthrough will describe using the AWS Cloud to emulate Pinterest's data processing system. This walkthrough will explain the following:

- [Part 1](Walkthrough_part_1_EC2_Kafka) will describe how to configure a `EC2 Kafka client`
- [Part 2](Walkthrough_part_2_MSK_S3) will describe how to connect an `MSK cluster` to an `S3 bucket`
- [Part 3](Walkthrough_part_3_API) will describe how to configure an `API` in `API Gateway`
- [Part 4](Walkthrough_part_4_ETL_Databricks) will describe how to read, clean and query data on `Databricks`
- [Part 5](Walkthrough_part_5_Airflow) will describe how to orchestrate `Databricks` Workloads on `MWAA`
- [Part 6](Walkthrough_part_6_Streaming) will describe how to send streaming data to `Kinesis` and read this data in `Databricks`

## Table of Contents

- [Technologies used](#technologies-used)
  - [Apache Spark](#apache-spark)
  - [Databricks](#databricks)
- [Setup S3 credentials in Databricks](#setup-s3-credentials-in-databricks)
  - [Create AWS Access Key and Secret Access Key for Databricks](#create-aws-access-key-and-secret-access-key-for-databricks)
  - [Upload credential csv file to Databricks](#upload-credential-csv-file-to-databricks)
- [Mount and Read data into Databricks](#mount-and-read-data-into-databricks)
  - [Mount an AWS S3 bucket to Databricks](#mount-an-aws-s3-bucket-to-databricks)
  - [Reading JSON files from mounted S3 bucket](#reading-json-files-from-mounted-s3-bucket)
  - [Creating dataframes from JSON files](#creating-dataframes-from-json-files)
- [Clean data from JSON files within Databricks](#clean-data-from-json-files-within-databricks)
  - [Clean pin df](#clean-pin-df)
  - [Clean geo df](#clean-geo-df)
  - [Clean user df](#clean-user-df)
- [Querying the Batch Data](#querying-the-batch-data)
- [Conclusion](#conclusion)

## Technologies used

[Part 1](Walkthrough_part_1_EC2_Kafka) will give an overview of how this project used [Amazon RDS](Walkthrough_part_1_EC2_Kafka#amazon-rds), [Amazon EC2](Walkthrough_part_1_EC2_Kafka#amazon-ec2), [Apache Kafka](Walkthrough_part_1_EC2_Kafka#apache-kafka), and [AWS IAM](Walkthrough_part_1_EC2_Kafka#aws-iam)

[Part 2](Walkthrough_part_2_MSK_S3) will give an overview of how this project used [Amazon MSK](Walkthrough_part_2_MSK_S3#amazon-msk) and [Amazon S3](Walkthrough_part_2_MSK_S3#amazon-s3)

[Part 3](Walkthrough_part_3_API) will give an overview of how this project used [Amazon API Gateway](Walkthrough_part_3_API#amazon-api-gateway) and [Confluent REST Proxy for Kafka](Walkthrough_part_3_API#confluent-rest-proxy-for-kafka)

[Part 4](Walkthrough_part_4_ETL_Databricks) will give an overview of how this project used [Apache Spark](Walkthrough_part_4_ETL_Databricks#apache-spark) and [Databricks](Walkthrough_part_4_ETL_Databricks#databricks)

[Part 5](Walkthrough_part_5_Airflow) will give an overview of how this project used [Apache Airflow](Walkthrough_part_4_ETL_Databricks#apache-airflow) and [Amazon MWAA](Walkthrough_part_4_ETL_Databricks#amazon-mwaa)

[Part 6](Walkthrough_part_6_Streaming) will give an overview of how this project used [Apache Spark Structured Streaming](Walkthrough_part_6_Streaming#apache-spark-structured-streaming), [Apache Delta Lake](Walkthrough_part_6_Streaming#apache-delta-lake) and [AWS Kinesis](Walkthrough_part_6_Streaming#aws-kinesis)

### Apache Spark

Apache Spark is an open-source, distributed computing framework designed for large-scale data processing and analytics. It provides an easy-to-use and high-performance platform for processing big data, enabling users to build scalable and efficient data applications.

Spark SQL allows users to query structured data using SQL-like syntax. It supports the creation of DataFrames, providing a programming interface for working with structured and semi-structured data.

This project will utilise Spark to perform data cleaning and computations on Databricks.

### Databricks

Databricks is a unified analytics platform designed for big data and machine learning. It provides an integrated environment for collaborative data science, allowing data engineers, data scientists, and analysts to work together on large-scale data processing and analytics tasks. Data engineers can use Databricks to build and manage data pipelines. The platform supports ETL (Extract, Transform, Load) tasks and integrates with various data sources and storage systems. The platform integrates with popular languages like Python, Scala, SQL, and R, providing a unified environment for different roles.

Databricks is built on top of Apache Spark, an open-source distributed computing framework. It enhances Spark with additional features and provides a managed Spark environment with optimised performance and scalability.

Users can create and run interactive notebooks using languages such as Python, Scala, SQL, and R. Notebooks allow for code execution, data exploration, and the creation of visualisations, making it easy to iterate on data analysis tasks.

This project will utilise Databricks to read data from AWS.

## Setup S3 credentials in Databricks

> [!Note]
>
> During this project, the Databricks account was already granted full access to S3, so a new Access Key and Secret Access Key were not required for Databricks. This file authentication_credentials.csv was already uploaded to Databricks.

The following are the steps to create a new Access Key and Secret Access Key and upload them to Databricks if they are required.

### Create AWS Access Key and Secret Access Key for Databricks

In the `IAM` console:

- Select `Users` under the `Access management` section on the left side of the console
- Click on the `Create user` button
- On the `Specify user details` page, enter the desired `User name` and click `Next`
- On the `Set permissions` page, select the '`Attach policies directly`' choice
- In the search bar, type `AmazonS3FullAccess` and check the box (This will allow full access to S3, meaning Databricks will be able to connect to any existing buckets on the AWS account.)
- Skip the next sections until you reach the `Review` page. Here, select the `Create user` button
- Now that you have created the IAM User, you will need to assign it a programmatic access key:
  - In the `Security Credentials` tab, select `Create Access Key`
  - On the subsequent page, select `Command Line Interface (CLI)`
  - Navigate to the bottom of the page and click `I understand`
  - On the next page, give the key-pair a description and select `Create Access Key`
  - Click the `Download.csv file` button to download the credentials

### Upload credential CSV file to Databricks

In the `Databricks` UI:

- Click the `Catalog` icon and then click the `+ Add` --> `Add data` button.
- Click on `Create or modify table` and then drop the credentials file downloaded from AWS.
- Once the file has been successfully uploaded, click `Create table` to finalise the process.
- The credentials will be uploaded in the following location: `dbfs:/user/hive/warehouse/`
- Now that the access keys are available from within the Databricks file store, they can be used in Databricks notebooks.

## Mount and Read data into Databricks

The S3 bucket will be mounted to a Databricks account to clean and query the data from the three Kafka topics. The following steps need to be taken to read data from an Amazon S3 bucket into Databricks:

The Python script [databricks_load_data.py](../databricks/classes/databricks_load_data.py) defines a class called `S3DataLoader`. It supplies the following methods:

- Reads in the credentials authentication_credentials.csv file and retrieves the Access Key and Secret Access Key
- Check if the S3 bucket is already mounted
- Mounts the S3 bucket if not already mounted
- Unmounts the S3 bucket
- Displays the contents of the S3 bucket
- Reads the JSON files from the S3 bucket
- Creates PySpark DataFrames from the JSON files

### Mount an AWS S3 bucket to Databricks

> [!Note]
>
> This project uses mounting to connect to the AWS S3 bucket.
>
> Databricks no longer recommends mounting external data locations to Databricks' file system.
> Databricks recommends migrating away from using mounts and instead managing data governance with Unity Catalog to configure access to S3 and volumes for direct interaction with files.

Databricks enables users to mount cloud object storage to the Databricks File System (DBFS) to simplify data access patterns.

Databricks mounts create a link between a workspace and cloud object storage, which enables interaction with cloud object storage using familiar file paths relative to the Databricks file system. Mounts work by creating a local alias under the /mnt directory that stores the following information:

- Location of the cloud object storage.
- Driver specifications to connect to the storage account or container.
- Security credentials required to access the data.

### Reading JSON files from mounted S3 bucket

To read the data from the uploaded source, spark.read methods were used in notebooks utilising the Python programming language.

When reading in the JSONs from S3, the complete path to the JSON objects was used, as seen in the S3 bucket (e.g. `topics/USER_ID.pin/partition=0/`).

### Creating dataframes from JSON files

Within Databricks, three DataFrames will be created to hold the data:

- `df_pin` for the Pinterest post data
- `df_geo` for the geolocation data
- `df_user` for the user data.

However, these dataframes are non-Delta tables with many small files. Therefore, to improve the performance of queries, these dataframes are converted to Delta with the DeltaTable API. The new Delta tables will accelerate queries.

A Delta table refers to a storage layer that brings ACID (Atomicity, Consistency, Isolation, Durability) transactions to Apache Spark. Delta tables are designed to improve the reliability, performance, and manageability of data lakes. Key features of Delta tables include:

- ACID Transactions: Delta tables provide support for ACID transactions, which ensures that operations on the data are atomic, consistent, isolated, and durable. This is particularly important for data consistency and reliability in a distributed and parallel processing environment.
- Schema Evolution: Delta tables support schema evolution, allowing you to modify the structure of the data over time without requiring a full rewrite of the entire dataset. This is useful when dealing with evolving data requirements.
- Time Travel: Delta tables enable time travel, allowing you to query the data as it existed at a specific point in time. This is beneficial for auditing, debugging, or rolling back to a previous state of the data.
- Concurrency Control: Delta tables provide concurrency control mechanisms to handle multiple users or applications trying to modify the same data concurrently. This helps avoid conflicts and ensures data consistency.
- Unified Batch and Streaming: Delta supports both batch and streaming workloads, making it suitable for a wide range of data processing scenarios. You can use Delta tables for both batch data processing using Spark jobs and real-time streaming data using Structured Streaming.
- Metadata Management: Delta tables maintain metadata that tracks the changes made to the data, enabling efficient management and optimisation of data operations.

You can then perform various operations on the Delta table, taking advantage of its ACID properties and other features. Delta tables are particularly useful for managing and processing large-scale data in a robust and efficient manner.

## Clean data from JSON files within Databricks

The Python script [databricks_clean_data.py](../databricks/classes/databricks_clean_data.py) defines a class called `DataCleaning`. It supplies the following methods:

- Cleans the pin data in the PySpark DataFrame
- Cleans the geo data in the PySpark DataFrame
- Cleans the user data in the PySpark DataFrame

Cleaning data that was read from JSON files involves handling missing values, filtering out irrelevant information, and transforming the data into a suitable format. The following sections detail how the data from the three Kafka topics from the S3 bucket were cleaned within Databricks.

### Clean pin df

To clean the `df_pin` DataFrame, the following cell will perform the following transformations:

- Replace empty entries and entries with no relevant data in each column with `Nones`
- Perform the necessary transformations on the `follower_count` to ensure every entry is a number. Make sure the data type of this column is an `int`.
- Ensure that each column containing `numeric` data has a `numeric` data type
- Clean the data in the `save_location` column to include only the save location path
- Rename the `index` column to `ind`.
- Reorder the DataFrame columns to have the following column order: (`ind`, `unique_id`, `title`, `description`, `follower_count`, `poster_name`, `tag_list`, `is_image_or_video`, `image_src`, `save_location`, `category`)

### Clean geo df

To clean the `df_geo` DataFrame, the following cell will perform the following transformations:

- Create a new column coordinates that contains an array based on the `latitude` and `longitude` columns
- Drop the `latitude` and `longitude` columns from the DataFrame
- Convert the `timestamp` column from a `string` to a `timestamp` data type
- Reorder the DataFrame columns to have the following column order: (`ind`, `country`, `coordinates`, `timestamp`)

### Clean user df

To clean the `df_user` DataFrame, the following cell will perform the following transformations:

- Create a new column, `user_name`, that concatenates the information found in the `first_name` and `last_name` columns
- Drop the `first_name` and `last_name` columns from the DataFrame
- Convert the `date_joined` column from a `string` to a `timestamp` data type
- Reorder the DataFrame columns to have the following column order: (`ind`, `user_name`, `age`, `date_joined`)

## Querying the Batch Data

The notebook [query_batch_data_mount.ipynb](databricks/query_batch_data_mount.ipynb) holds the script for mounting the S3 Bucket, producing and cleaning the dataframes, then querying the data to produce valuable insights. This notebook uses the methods from the two classes discussed in the previous scripts.

The notebook [query_batch_data_direct.ipynb](databricks/query_batch_data_direct.ipynb) holds an alternative script for accessing the data in the S3 Bucket, along with cleaning and querying the data. This is due to Databricks no longer recommending mounting external data locations to Databricks Filesystem. This notebook uses the methods from the two classes discussed in the previous scripts.

Before querying the data the three dataframes (`df_pin`, `df_geo`, and `df_user`) are joined together on the common column heading `ind` into a single dataframe called `df_all`. df_all will be created and registered as a temporary table before executing any SQL queries to make sure that it is a valid DataFrame. To do this, `df_all` is registered as a temporary view using `df_all.createOrReplaceTempView("df_all")`.

## Conclusion

In conclusion, this section of the project centred around Apache Spark and Databricks, two pivotal technologies in the realm of big data processing. Databricks, a unified analytics platform built on Apache Spark, facilitated data processing tasks.

The process involved setting up S3 credentials in Databricks, ensuring seamless connectivity between the data processing environment and AWS S3 buckets. Detailing the steps, including creating AWS Access Keys, managing IAM permissions, and uploading credential files, were explained to streamline this crucial setup.

Subsequently, the walkthrough delved into the intricacies of mounting and reading data into Databricks. The process involved mounting an AWS S3 bucket to Databricks, reading JSON files, and creating PySpark DataFrames. The adoption of Delta tables was introduced to enhance performance, bringing ACID transactions to the data processing pipeline.

The data cleaning process, a crucial step in preparing data for analysis, involved handling missing values, transforming data types, and ensuring data consistency across the three PySpark DataFrames—df_pin, df_geo, and df_user.

The walkthrough also showcased two alternative notebooks: one dedicated to mounting the S3 bucket and the other to accessing the data directly. These notebooks demonstrated how to produce valuable insights by joining the three dataframes and executing SQL queries.
