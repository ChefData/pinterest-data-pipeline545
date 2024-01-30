# Pinterest Data Pipeline Project

Pinterest is a visual discovery platform and social commerce network. Building on AWS storage and compute solutions, Pinterest uses sophisticated machine learning engines to deliver personalised content to its users. Pinterest hosts billions of images for over 450 million users to explore, save, and share as Pins to personalised digital inspiration boards.

Pinterest’s services run on the Amazon Web Services (AWS) Cloud, where Pinterest can scale processing, storage, and analysis of its rapidly increasing data. Amazon Web Services (AWS) is a comprehensive cloud computing platform provided by Amazon. It offers various services, including computing power, storage, databases, machine learning, analytics, content delivery, Internet of Things (IoT), and security.

A data pipeline is a systematic and automated process for the efficient and reliable movement, transformation, and management of data from one point to another within a computing environment. It plays a crucial role in modern data-driven organisations by enabling the seamless flow of information across various stages of data processing. Pinterest has a large volume of data from multiple sources. However, raw data is not as valuable as it can be; it must be moved, sorted, filtered, reformatted, and analysed for business intelligence.

A data pipeline includes various technologies to verify, summarise, and find patterns in data to inform business decisions. Well-organised data pipelines support various big data projects, such as visualisations, exploratory data analyses, and machine learning tasks. Pinterest uses the AWS Data Pipeline to process and move data between different AWS compute and storage services and on-premises data sources at specified intervals.

## Table of Contents

- [Description of the project](#description-of-the-project)
  - [Aim of the project](#aim-of-the-project)
- [Installation instructions](#installation-instructions)
- [Usage instructions](#usage-instructions)
  - [Environment Setup](#environment-setup)
  - [Credential Setup](#credential-setup)
  - [Project Navigation](#project-navigation)
- [Classes and Methods](#classes-and-methods)
  - [RDSDBConnector](#rdsdbconnector)
  - [APICommunicator](#apicommunicator)
  - [AWSDBConnector](#awsdbconnector)
  - [S3DataLoader](#s3dataloader)
  - [DataCleaning](#datacleaning)
- [File structure of the project](#file-structure-of-the-project)
- [Technologies Used](#technologies-used)
- [Troubleshooting](#troubleshooting)
- [License information](#license-information)

## Description of the project

Pinterest crunches billions of data points daily to decide how to provide more value to its users. This project has been designed to emulate this system using AWS Cloud infrastructure.

### Aim of the project

The aim of this project is as follows:

- To develop an end-to-end data processing pipeline hosted on AWS based on Pinterest’s experimental processing pipeline
- To develop an API using AWS API Gateway and integrating with AWS MSK and MSK Connect for data distribution to an S3 data lake
- To extract batch data from AWS S3 and transform it in Databricks using pySpark
- To use AWS MWAA to orchestrate Databricks Workloads
- To implement real-time data streaming via AWS Kinesis and conduct near real-time analysis using a Spark cluster on Databricks

The project uses an RDS database containing three tables resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest:

- pinterest_data contains data about posts being updated on Pinterest
- geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
- user_data contains data about the user that has uploaded each post found in pinterest_data

The data within these tables will emulate Pinterest’s data pipeline.

![Alt text](README_Images/pinterest_data_pipeline.png)

## Installation instructions

To use the functionality provided by this project, follow these steps:

> [!NOTE]
> Make sure you have the following installed:
>
> - A Code editor such as Visual Studio Code
> - Conda (optional but recommended)

### 1. Clone the Repository

Clone the repository to your local machine using the following:

#### Windows

1. Install [Git](https://git-scm.com/download/win).
2. Open the command prompt or Git Bash.
3. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/ChefData/pinterest-data-pipeline545
    ```

#### macOS

1. Open the Terminal.
2. If you don’t have git installed, you can install it using [Homebrew](https://brew.sh/):

    ```bash
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    brew install git
    ```

3. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/ChefData/pinterest-data-pipeline545
    ```

#### Linux: Ubuntu or Debian-based systems

1. Open the terminal.
2. Install git:

    ```bash
    sudo apt-get update
    sudo apt-get install git
    ```

3. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/ChefData/pinterest-data-pipeline545
    ```

#### Linux: Fedora

1. Open the terminal.
2. Install git:

    ```bash
    sudo dnf install git
    ```

3. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/ChefData/pinterest-data-pipeline545
    ```

## Usage instructions

> [!NOTE]
> It is assumed that you are a support engineer at AiCore and have the relevant credentials to download the data that has been stored in the different sources

Follow these instructions to set up and install the project on your local machine.

### Environment Setup

1. Create a Conda Virtual Environment to isolate the project dependencies (Optional but Recommended)

    ```bash
    conda create -n AiCore-Project-PDP -c conda-forge python=3.11 ipykernel sqlalchemy requests pymysql python-decouple PyYAML databricks-connect
    ```

2. Or import the conda environment from the supplied YAML file

    ```bash
    conda env create -f AiCore-Project-PDP-env.yaml
    ```

3. Activate the conda virtual environment:
    - On Windows:

        ```bash
        activate AiCore-Project-PDP
        ```

    - On macOS and Linux:

        ```bash
        conda activate AiCore-Project-PDP
        ```

### Credential Setup

1. Create a YAML file containing the RDS database credentials. The YAML file should be structured as follows:

    ```yaml
    HOST: your_host
    USER: your_username
    PASSWORD: your_password
    DATABASE: your_database
    PORT: 3306
    ```

2. Create a .env text file in your repository’s root directory in the form:

    ```env
    # Datebase Credentials
    creds_path = /your_path_to_rds_database_credentials.yaml

    # AWS IAM Username
    iam_username = <USER_ID>

    # Invoke URL
    invoke_URL = <INVOKE_URL>

    # Deployment Stage
    deployment_stage = <DEPLOYMENT_STAGE>

    # Consumer Group
    consumer_group = <CONSUMER_GROUP>

    # Bootstrap Servers
    bootstrap_servers = <BOOTSTRAP_SERVER_STRING>
    ```

3. Create a local key pair file, which ends in the .pem extension. This file will allow you to connect to the EC2 instance. The process of creating this file is detailed in [Walkthrough Part 1 Setting up permissions](Walkthrough_part_1_EC2_Kafka.md#setting-up-permissions)

> [!NOTE]
>
> In Amazon EC2, a key pair is a secure method of accessing your EC2 instances. It consists of a public key and a corresponding private key. The public key encrypts data that can only be decrypted using the private key. Key pairs are essential for establishing secure remote access to your EC2 instances.
>
> The public key is stored on the instance associated with it, allowing it to authenticate the private key when you attempt to connect. To securely access the EC2 instance after creation, use the private key to authenticate yourself.

## Project Navigation

Six project walkthrough documents have been supplied detailing the process taken to emulate the Pinterest system of processing data using the AWS Cloud. These walkthroughs will explain the following:

- [Part 1](Walkthrough_part_1_EC2_Kafka) will describe how to configure a `EC2 Kafka client`
- [Part 2](Walkthrough_part_2_MSK_S3) will describe how to connect an `MSK cluster` to an `S3 bucket`
- [Part 3](Walkthrough_part_3_API) will describe how to configure an `API` in `API Gateway`
- [Part 4](Walkthrough_part_4_ETL_Databricks) will describe how to read, clean and query data on `Databricks`
- [Part 5](Walkthrough_part_5_Airflow) will describe how to orchestrate `Databricks` Workloads on `MWAA`
- [Part 6](Walkthrough_part_6_Streaming) will describe how to create data streams using `Kinesis Data Streams`

## Classes and Methods

### RDSDBConnector

A class for connecting to a database, reading credentials from a YAML file, creating a database URL, initialising an SQLAlchemy engine, and performing database operations.

Private Methods:

- __init__(self) -> None: Initialises a DatabaseConnector object
- __read_db_creds(self) -> Dict[str, str]: Reads database credentials from a YAML file.
- __validate_db_creds(db_creds: Dict[str, str]) -> None: Validates the database credentials.
- __build_url_object(db_creds: Dict[str, str]) -> URL: Builds a SQLAlchemy URL object from database credentials.

Protected Methods:

- _init_db_engine(self) -> create_engine: Initialises the database engine.

### APICommunicator

A class for communicating with an API and sending data to Kafka topics or Kinesis streams.

Private Methods:

- __init__(self) -> None: Initialises the APICommunicator with the necessary configuration.
- __make_request_with_retry(method: str, url: str, headers: Dict[str, str], payload: Optional[str]) -> Optional[requests.Response]: Makes an HTTP request to the specified URL with a retry mechanism.
- __encode_datetime(self, obj: Union[datetime, None]) -> Optional[str]: Encodes a datetime object to its ISO format if not None.
- __encode_to_json(self, data: Dict) -> str: Encodes a dictionary to a JSON string, handling datetime objects.
- __send_data_to_api(self, method: str, URL: str, payload_dict: Dict, headers: Dict[str, str], topic_name: str) -> None: Sends data to a specified endpoint through the configured API.

Protected Methods:

- _send_data_batch_to_api(self, topic_name: str, data: Dict) -> None: Sends data to a specified Kafka topic through the configured API.
- _send_data_stream_to_api(self, topic_name: str, data: Dict) -> None: Sends data to a specified Kinesis stream through the configured API.

### AWSDBConnector

Class for connecting to a database and sending data to an API.

Private Methods:

- __init__(self, topics_dict: Dict[str, str]) -> None: Initialise the database and API connections.
- __handle_signal(self, signum: int, frame) -> None: Handle the termination signal.
- __get_random_row(connection, table_name: str, random_row: int) -> Dict[str, Union[int, str]]: Get a random row from the specified table.
- __simulate_asynchronous_data_fetching(self) -> None: Introduce a random sleep to simulate asynchronous data fetching.
- __process_data(self, streaming: bool) -> None: Fetch random rows from different tables and send data to the API.

Public Methods:

- run_infinite_post_data_loop(self, streaming: bool) -> None: Run an infinite loop to simulate continuous data processing.

### S3DataLoader

Class for loading data from AWS S3 into PySpark DataFrames.

Private Methods:

- __init__(self, credentials_path: str, iam_username: str, topics: List[str]): Initialise Spark session and set instance variables
- __load_aws_keys(self) -> Tuple[str, str, str, str]: Load AWS keys from Delta table.
- __is_mounted(self) -> bool: Check if the S3 bucket is already mounted.
- __read_json_files(self, mounted: bool = True) -> Dict[str, DataFrame]: Read JSON files from the S3 bucket into PySpark DataFrames.
- __read_stream_files(self) -> Dict[str, DataFrame]: Read streaming data from AWS Kinesis and return a dictionary of PySpark DataFrames.
- __get_pin_schema() -> StructType: Define the 'pin' data schema.
- __get_geo_schema() -> StructType: Define the 'geo' data schema.
- __get_user_schema() -> StructType: Define the 'user' data schema.

Public Methods:

- mount_s3_bucket(self) -> None: Mount the S3 bucket if not already mounted.
- unmount_s3_bucket(self) -> None: Unmount the S3 bucket.
- display_s3_bucket(self) -> None: Display the contents of the S3 bucket.
- create_dataframes(self, mounted: bool = True) -> None: Create global DataFrames from JSON files in the S3 bucket.
- create_stream_dataframes(self) -> None: Process streaming data and create global temporary views of Delta tables.
- write_stream(self, df: DataFrame) -> None: Write streaming DataFrame to Delta table.
- clear_delta_tables(self) -> None: Clear Delta table checkpoints.

### DataCleaning

Class for cleaning data in PySpark DataFrames.

Public Methods:

- clean_pin_data(df: DataFrame) -> DataFrame: Clean pin data in the DataFrame.
- clean_geo_data(df: DataFrame) -> DataFrame: Clean geo data in the DataFrame.
- clean_user_data(df: DataFrame) -> DataFrame: Clean user data in the DataFrame.

## File structure of the project

```bash
.
├── USER_ID-key-pair.pem
├── AiCore-Project-PDP-env.yaml
├── README.md
├── README_Images
├── Walkthrough_part_1_EC2_Kafka_Client.md
├── Walkthrough_part_2_MSK_S3.md
├── Walkthrough_part_3_API.md
├── Walkthrough_part_4_ETL_Databricks.md
├── Walkthrough_part_5_Airflow.md
├── Walkthrough_part_6_Streaming.md
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
├── user_posting_emulation_batch.py
├── user_posting_emulation_stream.py
├── .gitignore
└── .env
```

## Technologies Used

[Part 1](Walkthrough_part_1_EC2_Kafka) will give an overview of how this project used [Amazon RDS](Walkthrough_part_1_EC2_Kafka#amazon-rds), [Amazon EC2](Walkthrough_part_1_EC2_Kafka#amazon-ec2), [Apache Kafka](Walkthrough_part_1_EC2_Kafka#apache-kafka), and [AWS IAM](Walkthrough_part_1_EC2_Kafka#aws-iam)

[Part 2](Walkthrough_part_2_MSK_S3) will give an overview of how this project used [Amazon MSK](Walkthrough_part_2_MSK_S3#amazon-msk) and [Amazon S3](Walkthrough_part_2_MSK_S3#amazon-s3)

[Part 3](Walkthrough_part_3_API) will give an overview of how this project used [Amazon API Gateway](Walkthrough_part_3_API#amazon-api-gateway) and [Confluent REST Proxy for Kafka](Walkthrough_part_3_API#confluent-rest-proxy-for-kafka)

[Part 4](Walkthrough_part_4_ETL_Databricks) will give an overview of how this project used [Apache Spark](Walkthrough_part_4_ETL_Databricks#apache-spark) and [Databricks](Walkthrough_part_4_ETL_Databricks#databricks)

[Part 5](Walkthrough_part_5_Airflow) will give an overview of how this project used [Apache Airflow](Walkthrough_part_4_ETL_Databricks#apache-airflow) and [Amazon MWAA](Walkthrough_part_4_ETL_Databricks#amazon-mwaa)

[Part 6](Walkthrough_part_6_Streaming) will give an overview of how this project used [Apache Spark Structured Streaming](Walkthrough_part_6_Streaming#apache-spark-structured-streaming), [Apache Delta Lake](Walkthrough_part_6_Streaming#apache-delta-lake) and [AWS Kinesis](Walkthrough_part_6_Streaming#aws-kinesis)

## Tools Used

- Visual Studio Code: Code editor used for development.
- Python: Programming language used for the game logic.
  - PyYAML: YAML parser and emitter for Python
  - sqlalchemy: Open-source SQL toolkit and object-relational mapper
  - requests: Python HTTP library allows users to send HTTP requests to a specified URL.
  - Decouple: separates the settings parameters from your source code.
- Databricks: Databricks is a unified analytics platform designed for big data and machine learning and provides a managed Spark environment with optimised performance and scalability.
- Git: Version control system for tracking changes in the project.
- GitHub: Hosting platform for version control and collaboration.
- Amazon Web Services: cloud computing services
- AiCore: Educational programme for tasks and milestones used for development progression

## Troubleshooting

If you happen to encounter any issues during the installation or setup process, please open an issue in the repository.

## License information

MIT License

Copyright (c) 2024 Nick Armstrong

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software") to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the software, and to permit persons to whom the software is furnished to do so, subject to the following conditions:

The above copyright and permission notice shall be included in all copies or substantial portions of the software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
