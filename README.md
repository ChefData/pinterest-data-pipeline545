# AWS Data Pipeline Project

This data pipeline project emulates a data management system similar to what a large social media platform, such as Pinterest, might use. It provides a practical look into how data can be efficiently collected, processed, and stored in a cloud environment like AWS.

## Architecture Design

![Alt text](README_Images/pinterest_data_pipeline.png)

## Project Navigation

The [wiki](https://github.com/ChefData/pinterest-data-pipeline545/wiki) supplies six project walkthrough documents detailing the process of emulating the Pinterest system of processing data using the AWS Cloud.

- [Part 1](https://github.com/ChefData/pinterest-data-pipeline545/wiki/1:-EC2-instance-as-a-Apache-Kafka-machine) will describe how to configure a `EC2 Kafka client`.
- [Part 2](https://github.com/ChefData/pinterest-data-pipeline545/wiki/2:-MSK-cluster-to-S3-bucket) will describe how to connect an `MSK cluster` to an `S3 bucket`.
- [Part 3](https://github.com/ChefData/pinterest-data-pipeline545/wiki/3:-Configuring-API-Gateway) will describe configuring an `API` in `API Gateway`.
- [Part 4](https://github.com/ChefData/pinterest-data-pipeline545/wiki/4:-ETL-in-Databricks) will describe how to read, clean and query data on `Databricks`.
- [Part 5](https://github.com/ChefData/pinterest-data-pipeline545/wiki/5:-Managed-Workflows-for-Apache-Airflow) will describe how to orchestrate `Databricks` Workloads on `MWAA`.
- [Part 6](https://github.com/ChefData/pinterest-data-pipeline545/wiki/6:-Kinesis-Streaming) will describe how to create data streams using `Kinesis Data Streams`.

## Aim of the project

This project aims to provide hands-on experience setting up and managing a data pipeline. It offers insights into how large-scale applications like Pinterest handle vast amounts of data, ensuring it's processed efficiently and stored securely. The aim is to create a robust data pipeline that enables us to:

- **Data Emulation**: Develop a script that retrieves data from an Amazon RDS to effectively emulate the process of posting data as it would occur on a platform like Pinterest.
- **Data Processing with Kafka**: Implement Apache Kafka to process the influx of data efficiently, ensuring smooth data flow and scalability.
- **Data Storage in S3**: Utilise Amazon S3 buckets to securely store processed data and easily access it for future analysis.
- **API Integration for Data Streaming**: Develop an API to facilitate data streaming into the Kafka cluster and for data distribution to an S3 data lake.
- **Data Analysis in Databricks**: To extract batch data from AWS S3 and transform it in Databricks using pySpark, and to conduct comprehensive batch analysis on the stored Pinterest data
- **Workflow Orchestration with MWAA**: Employ Managed Workflows for Apache Airflow (MWAA) to orchestrate complex data workflows using Directed Acyclic Graphs (DAGs), which enhances the automation and monitoring of the data pipeline.
- **Real-time Data Handling with Kinesis**: Integrate AWS Kinesis Data Streams to extend the pipeline's capabilities for real-time data management using a Spark cluster on Databricks.

## TODO: Resource provisioning

## Project Structure

### Data files

The project uses an RDS database containing three tables resembling data received by the Pinterest API when a user makes a POST request by uploading data to Pinterest:

- `pinterest_data`: Contains data about posts which users upload to Pinterest
- `geolocation_data`: Contains data about the geolocation of each Pinterest post found in pinterest_data
- `user_data`: Contains data about the user that has uploaded each post found in pinterest_data

The data within these tables will emulate Pinterest's data pipeline.

### Local Scripts

```mermaid
graph LR;
    rds_db_connector.py-->data_processor.py;
    api_communicator.py-->data_processor.py;
    data_processor.py-->streaming_batch.py;
    data_processor.py-->streaming_kinesis.py;
```

- `rds_db_connector.py`: Contains the `RDSDBConnector` class for connecting to a database, reading credentials from a YAML file, creating a database URL, initialising an SQLAlchemy engine, and performing database operations.
- `api_communicator.py`: Contains the `APICommunicator` class for communicating with an API and sending data to Kafka topics or Kinesis streams.
- `data_processor.py`: The `DataProcessor` class is responsible for processing data from various sources and sending it to an API.
- `streaming_batch.py`: Contains a script that extracts Pinterest data from MySQL database and uploads it to an S3 bucket through an API Gateway that goes through an MSK cluster on an EC2 instance.
- `streaming_kinesis.py`: Contains a script that streams real-time data to AWS Kinesis

### Spark Scripts

```mermaid
graph LR;
    databricks_load_data.py-->query_batch_data_direct.ipynb;
    databricks_clean_data.py-->query_batch_data_direct.ipynb;
    databricks_load_data.py-->query_batch_data_mount.ipynb;
    databricks_clean_data.py-->query_batch_data_mount.ipynb;
    databricks_load_data.py-->write_stream_data.ipynb;
    databricks_clean_data.py-->write_stream_data.ipynb;
```

- `databricks_load_data.py`: Contains the `S3DataLoader` class for loading data from AWS S3 into PySpark DataFrames.
- `databricks_clean_data.py`: Contains the `DataCleaning` class for cleaning data in PySpark DataFrames.
- `query_batch_data_direct.ipynb`: A script to directly load data from the S3 bucket, clean that data, and query the cleaned data for information.
- `query_batch_data_mount.ipynb`: A script to mount, clean and query data for information.
- `write_stream_data.ipynb`: A script to read real-time kinesis data, clean it, and save it in the delta table on Databricks.

### Data Orchestration

- `0ab336d6fcf7_dag.py`: A dag file which runs the following notebooks on databricks daily.

```mermaid
graph LR;
    airflow_load_data.ipynb-->airflow_clean_geo.ipynb;
    airflow_load_data.ipynb-->airflow_clean_pin.ipynb;
    airflow_load_data.ipynb-->airflow_clean_user.ipynb;
    airflow_clean_geo.ipynb-->airflow_query_data.ipynb;
    airflow_clean_pin.ipynb-->airflow_query_data.ipynb;
    airflow_clean_user.ipynb-->airflow_query_data.ipynb;
```

- `airflow_load_data.ipynb`: A script to mount the S3 bucket onto databricks.
- `airflow_clean_geo.ipynb`: This script reads JSON files from the mounted S3 bucket, stores the contents as DataFrames and performs cleaning operations.
- `airflow_clean_pin.ipynb`: A script that reads JSON files from the mounted S3 bucket, stores the contents as DataFrames and performs cleaning operations.
- `airflow_clean_user.ipynb`: This script reads JSON files from the mounted S3 bucket, stores the contents as DataFrames and performs cleaning operations.
- `airflow_query_data.ipynb`: A script to query the cleaned data for information.

### File structure of the project

```text
AWS Data Pipeline

Local Machine
.
├── USER_ID-key-pair.pem
├── AiCore-Project-PDP-env.yaml
├── README.md
├── README_Images
├── classes
│   ├── __init__.py
│   ├── api_communicator.py
│   ├── aws_db_connector.py
│   └── rds_db_connector.py
├── creds.yaml
├── databricks
│   ├── airflow
│   │   ├── 0ab336d6fcf7_dag.py
│   │   ├── airflow_clean_geo.ipynb
│   │   ├── airflow_clean_pin.ipynb
│   │   ├── airflow_clean_user.ipynb
│   │   ├── airflow_load_data.ipynb
│   │   └── airflow_query_data.ipynb
│   ├── classes
│   │   ├── databricks_clean_data.py
│   │   └── databricks_load_data.py
│   ├── query_batch_data_direct.ipynb
│   ├── query_batch_data_mount.ipynb
│   └── write_stream_data.ipynb
├── streaming_batch.py
├── streaming_kinesis.py
├── .gitignore
└── .env

EC2 Instance
├── kafka_2.12-2.8.1
│   ├── bin
│   │   └── client.properties
│   └── libs
│       └── aws-msk-iam-auth-1.1.5-all.jar
├── kafka-connect-s3
│   └── confluentinc-kafka-connect-s3-10.0.3.zip
└── confluent-7.2.0
    └── etc
        └── kafka-rest
            └── kafka-rest.properties
```

## Troubleshooting

If you encounter any issues during the installation or setup process, please open an issue in the repository.
