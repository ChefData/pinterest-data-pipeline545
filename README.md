# Pinterest Data Pipeline

## Table of Contents

- [Description of the project](#description-of-the-project)
  - [What the project does](#what-the-project-does)
  - [Aim of the project](#aim-of-the-project)
  - [Lessons learned](#lessons-learned)
- [Installation instructions](#installation-instructions)
- [Usage instructions](#usage-instructions)
  - [Environment Setup](#environment-setup)
  - [Credential Setup](#credential-setup)
  - [Project Navigation](#project-navigation)
- [Classes and Methods](#classes-and-methods)
- [File structure of the project](#file-structure-of-the-project)
- [Technologies Used](#technologies-used)
- [Troubleshooting](#troubleshooting)
- [License information](#license-information)

## Description of the project

A data pipeline is a systematic and automated process for the efficient and reliable movement, transformation, and management of data from one point to another within a computing environment. It plays a crucial role in modern data-driven organizations by enabling the seamless flow of information across various stages of data processing.

Pinterest uses the AWS Data Pipeline to process and move data between different AWS compute and storage services, as well as on-premises data sources, at specified intervals.

At Pinterest, there are two primary categories of data sets: online service logs and database dumps. The pipeline for collecting service logs is usually composed of three stages: log collection, log transportation, and log persistence.

Pinterest crunches billions of data points every day to decide how to provide more value to their users. This project has been designed to create a similar system using AWS Cloud infrastructure.

The project uses a RDS database containing three tables resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest:

- pinterest_data contains data about posts being updated to Pinterest
- geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
- user_data contains data about the user that has uploaded each post found in pinterest_data

### What the project does

### Aim of the project

THe aim of this project is as follows:

- To develop an end-to-end data processing pipeline hosted on AWS, based on Pinterest’s experimental processing pipeline.
- To develop an API using AWS API Gateway and integrating with AWS MSK and MSK Connect for data distribution to an S3 data lake.
- To extract batch data from AWS S3 and transformed it in Databricks using pySpark
- To use AWS MWAA to orchestrate Databricks Workloads
- To implement real-time data streaming via AWS Kinesis and conduct near real-time analysis using a Spark cluster on Databricks.

### Lessons learned

> [!NOTE]
>
> The following were already configured in the AWS account:
>
> - EC2 instance
> - S3 bucket
> - IAM role that can write to the S3 bucket
> - VPC Endpoint to S3
> - Security rules for the EC2 instance to allow communication with the MSK cluster
> - REST type API with regional endpoint
>
> The following were already configured in the Databricks account:
>
> - The Databricks account was granted full access to S3, so a new Access Key and Secret Access Key was not created for Databricks.
> - The credentials were already uploaded to Databricks.

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
2. If you don't have git installed, you can install it using [Homebrew](https://brew.sh/):

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
    conda create -n AiCore-Project-PDP -c conda-forge python=3.11 ipykernel sqlalchemy requests boto3 pymysql python-decouple PyYAML
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

1. Thoughout this project credentials were used specific to the infrastructure. Take a note of the following credentials for the purposes of the project:
    - USER_ID
        - Provided by AiCore
    - KEY_PAIR_NAME
        - Navigate to the EC2 console.
        - Identify and select the instance with the unique USER_ID.
        - Under the Details section find the Key pair name.




    - IAM_ARN
        - x
    - BOOTSTRAP_SERVER_STRING
        - x
    - PLAINTEXT_APACHE_ZOOKEEPER_CONNECTION_STRING
        - x
    - CONSUMER_GROUP
        - x



    - BUCKET_NAME
        - Navigate to the S3 console.
        - Find the bucket associated with the USER_ID.
        - The bucket name should have the following format: user-<USER_ID>-bucket.
    - KAFKA_CLIENT_EC2_INSTANCE_PUBLIC_DNS
        - Navigate to the EC2 console.
        - Select 'Instances'
        - Select the client EC2 machine
        - Copy the 'Public IPv4 DNS'
    - INVOKE_URL
        - Produced during the [Build a Kafka REST proxy integration method for the API](#build-a-kafka-rest-proxy-integration-method-for-the-api)
    - DEPLOYMENT_STAGE
        - Produced during the [WALKTHROUGH.md/Build a Kafka REST proxy integration method for the API](#build-a-kafka-rest-proxy-integration-method-for-the-api)

2. Create a YAML file containing the RDS database credentials. The YAML file should be stuctured as follows:

    ```yaml
    HOST: your_host
    USER: your_username
    PASSWORD: your_password
    DATABASE: your_database
    PORT: 3306
    ```

3. Create a .env text file in your repository’s root directory in the form:

    ```env
    # Datebase Credentials
    creds_path = /Users/your_path_to_rds_database.yaml

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

4. Create a key pair file locally, which is a file ending in the .pem extension. This file will allow you to connect to the EC2 instance.
    - To do this, navigate to Parameter Store in your AWS account.
    - Find the specific key pair associated with your EC2 instance.
    - Select this key pair and under the Value field select Show. This will reveal the content of your key pair.
    - Copy its entire value (including the BEGIN and END header) and paste it in the .pem file in VSCode.
    - Save the created file in VSCode using the following format: KEY_PAIR_NAME.pem

> [!NOTE]
>
> In Amazon EC2, a key pair is a secure method of accessing your EC2 instances. It consists of a public key and a corresponding private key. The public key is used to encrypt data that can only be decrypted using the private key. Key pairs are essential for establishing secure remote access to your EC2 instances.
>
> The public key associated with the instance is stored on the instance itself, allowing it to authenticate the private key when you attempt to connect. To securely access the EC2 instance after creation, use the private key to authenticate yourself.

## File structure of the project

The project is built around three classes and the Python file needed to send data to the API:

```bash
    .
    ├── AiCore-Project-PDP-env.yaml
    ├── user_posting_emulation.py
    ├── README.md
    ├── WALKTHROUGH.md
    ├── classes
    │   ├── __init__.py
    │   ├── api_communicator.py
    │   ├── aws_db_connector.py
    │   └── database_connector.py
    ├── <USER_ID>-key-pair.pem
    ├── creds.yaml
    ├── .gitignore
    └── .env
```

## Technologies Used

- Apache Kafka is an open-source technology for distributed data storage, optimised for ingesting and processing streaming data in real-time.
- Amazon Managed Streaming for Apache Kafka (Amazon MSK) is a fully managed service used to build and run applications that use Apache Kafka to process data.
- Amazon MSK Connect is a feature of AWS MSK, that allows users to stream data to and from their MSK-hosted Apache Kafka clusters. With MSK Connect, you can deploy fully managed connectors that move data into or pull data from popular data stores like Amazon S3 and Amazon OpenSearch Service, or that connect Kafka clusters with external systems, such as databases and file systems.
- Amazon Simple Storage Service (S3) is a scalable and highly available object storage service provided by AWS. Its primary purpose is to store and retrieve large amounts of data reliably, securely, and cost-effectively.
- Amazon Elastic Compute Cloud (EC2) is a key component of Amazon Web Services (AWS) and plays a vital role in cloud computing. EC2 provides a scalable and flexible infrastructure for hosting virtual servers, also known as instances, in the cloud.
- Virtual Private Cloud (VPC) is a virtual network infrastructure that allows you to provision a logically isolated section of the AWS cloud where you can launch AWS resources.
- IAM Roles are a fundamental component of Identity and Access Management (IAM) in cloud computing environments, designed to manage and control access to various AWS resources and services. An IAM Role is an entity with a set of permissions that determine what actions can be performed on specific resources.
- Amazon API Gateway is an AWS service that allows the creation, maintenance and securing of scalable REST, HTTP and Websocket APIs. APIs can be created to access AWS services and other web services or data stored in the AWS Cloud.




- Spark Structured Streaming
- Databricks
- Airflow
- AWS MWAA
- AWS Kinesis.




## Tools Used

- Visual Studio Code: Code editor used for development.
- Python: Programming language used for the game logic.
  - PyYAML: YAML parser and emitter for Python
  - sqlalchemy: Open-source SQL toolkit and object-relational mapper
  - requests: Python HTTP library allows users to send HTTP requests to a specified URL.
  - boto3: Boto3 is an AWS SDK for Python that enables developers to integrate their Python applications, libraries, or scripts with AWS services such as Amazon S3, Amazon EC2, and Amazon DynamoDB
  - Decouple: helps you to organize your settings so that you can change parameters without having to redeploy your app.




urllib3.util
logging
json
time
signal
random




- Git: Version control system for tracking changes in the project.
- GitHub: Hosting platform for version control and collaboration.
- Amazon Web Services: cloud computing services
- AiCore: Educational programme for tasks and milestones used for development progression

## Troubleshooting

If you encounter any issues during the installation or setup process, please open an issue in the repository.

## License information

MIT License

Copyright (c) 2023 Nick Armstrong

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
