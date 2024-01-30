# Walkthrough of Pinterest Data Pipeline Project: Part 1

Pinterest crunches billions of data points daily to decide how to provide more value to its users.

This walkthrough will describe using the AWS Cloud to emulate Pinterest's data processing system. This walkthrough will explain the following:

- [Part 1](Walkthrough_part_1_EC2_Kafka) will describe how to configure a `EC2 Kafka client`
- [Part 2](Walkthrough_part_2_MSK_S3) will describe how to connect an `MSK cluster` to an `S3 bucket`
- [Part 3](Walkthrough_part_3_API) will describe how to configure an `API` in `API Gateway`
- [Part 4](Walkthrough_part_4_ETL_Databricks) will describe how to read, clean and query data on `Databricks`
- [Part 5](Walkthrough_part_5_Airflow) will describe how to orchestrate `Databricks` Workloads on `MWAA`
- [Part 6](Walkthrough_part_6_Streaming) will describe how to send streaming data to `Kinesis` and read this data in `Databricks`

## Table of Contents

- [Batch Processing](#batch-processing)
- [Technologies used](#technologies-used)
  - [Amazon RDS](#amazon-rds)
  - [Amazon EC2](#amazon-ec2)
  - [Apache Kafka](#apache-kafka)
  - [AWS IAM](#aws-iam)
- [Setting up permissions](#setting-up-permissions)
  - [Connect to the EC2 instance](#connect-to-the-ec2-instance)
  - [Set up Kafka on the EC2 instance](#set-up-kafka-on-the-ec2-instance)
  - [Setting up IAM authentication](#setting-up-iam-authentication)
  - [Setting up CLASSPATH environment variable](#setting-up-classpath-environment-variable)
  - [Configure Kafka client to use AWS IAM](#configure-kafka-client-to-use-aws-iam)
- [Conclusion](#conclusion)

## Batch Processing

Batch processing is a technique in which collected data is processed and stored in chunks or batches at scheduled intervals. Unlike real-time processing, where data is handled as it arrives, batch processing involves processing accumulated data over a specific period. Batch processing is suitable for scenarios where the delay between data collection and processing is acceptable, and real-time responsiveness is not critical. It offers advantages in terms of resource efficiency, scalability, and ease of management for processing large volumes of data systematically and controlled.

Data is collected over a predefined period, such as hourly, daily, or weekly intervals. Once a batch of data is gathered, it is processed as a group. Processing often involves running predefined operations or tasks on the entire batch. Typical tasks include sorting, filtering, aggregating, and transforming the data. Batch processing is typically scheduled to run at specific times, often during non-peak hours, to minimise impact on system resources. Batch processing is resource-efficient as it allows the system to process data in large volumes without the need for real-time responsiveness. Once processing is complete, the results are typically stored or outputted to a destination, such as a database, data warehouse, or another file. Output data may be used for reporting, analysis, or other downstream processes.

Pinterest employs batch processing in various aspects of its data analytics and platform optimisation. While real-time streaming is crucial for immediate user interactions and recommendations, batch processing plays a significant role in tasks that can be performed periodically or don't require instant responses.

Pinterest likely uses batch processing for tasks that do not require immediate responses and can be efficiently performed in scheduled intervals. This approach allows them to manage and analyse large volumes of data systematically, providing valuable insights for business intelligence, model training, and overall platform optimisation.
Batch processing may allow Pinterest to gather large volumes of data, perform complex analytics, and generate comprehensive reports for business insights. Before analysis or reporting, raw data often needs to be cleaned, transformed, or aggregated. Batch processing may enable Pinterest to preprocess data in batches, ensuring that it is in the desired format for subsequent analyses. Pinterest likely utilises batch processing to transfer and process data for storage in data warehouses. Data warehousing would enable Pinterest to store historical data and perform in-depth analyses for strategic planning and decision-making.

## Technologies used

[Part 1](Walkthrough_part_1_EC2_Kafka) will give an overview of how this project used [Amazon RDS](Walkthrough_part_1_EC2_Kafka#amazon-rds), [Amazon EC2](Walkthrough_part_1_EC2_Kafka#amazon-ec2), [Apache Kafka](Walkthrough_part_1_EC2_Kafka#apache-kafka), and [AWS IAM](Walkthrough_part_1_EC2_Kafka#aws-iam)

[Part 2](Walkthrough_part_2_MSK_S3) will give an overview of how this project used [Amazon MSK](Walkthrough_part_2_MSK_S3#amazon-msk) and [Amazon S3](Walkthrough_part_2_MSK_S3#amazon-s3)

[Part 3](Walkthrough_part_3_API) will give an overview of how this project used [Amazon API Gateway](Walkthrough_part_3_API#amazon-api-gateway) and [Confluent REST Proxy for Kafka](Walkthrough_part_3_API#confluent-rest-proxy-for-kafka)

[Part 4](Walkthrough_part_4_ETL_Databricks) will give an overview of how this project used [Apache Spark](Walkthrough_part_4_ETL_Databricks#apache-spark) and [Databricks](Walkthrough_part_4_ETL_Databricks#databricks)

[Part 5](Walkthrough_part_5_Airflow) will give an overview of how this project used [Apache Airflow](Walkthrough_part_4_ETL_Databricks#apache-airflow) and [Amazon MWAA](Walkthrough_part_4_ETL_Databricks#amazon-mwaa)

[Part 6](Walkthrough_part_6_Streaming) will give an overview of how this project used [Apache Spark Structured Streaming](Walkthrough_part_6_Streaming#apache-spark-structured-streaming), [Apache Delta Lake](Walkthrough_part_6_Streaming#apache-delta-lake) and [AWS Kinesis](Walkthrough_part_6_Streaming#aws-kinesis)

### Amazon RDS

Amazon Relational Database Service (Amazon RDS) is a fully managed relational database service provided by Amazon Web Services (AWS). It simplifies the process of setting up, operating, and scaling a relational database in the cloud. RDS supports various popular relational database engines, including MySQL, PostgreSQL, MariaDB, Oracle, and Microsoft SQL Server. Users can choose the database engine that best suits their application requirements. For this project, a MySQL database engine was used.

Amazon RDS is widely used by developers, businesses, and enterprises to deploy and manage relational databases in a scalable, secure, and cost-effective manner. Its managed nature simplifies many aspects of database administration and allows users to focus on building and optimising their applications.

This project uses an RDS database containing three tables resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest.

### Amazon EC2

Amazon Elastic Compute Cloud (Amazon EC2) is a web service provided by AWS that offers resizable compute capacity in the cloud. EC2 allows users to run virtual servers, known as instances, to host their applications and workloads. EC2 instances are typically accessed using secure shell (SSH) for Linux instances or Remote Desktop Protocol (RDP) for Windows instances. Key pairs are used to connect to instances securely.

This project uses an Amazon EC2 instance to use as an Apache Kafka machine.

### Apache Kafka

Apache Kafka is an open-source distributed event streaming platform that is designed for high-throughput, fault-tolerant, and scalable data streaming. Kafka is widely used for building data pipelines and streaming applications.

Messages in Kafka are organised into topics, which are logical channels or categories. Kafka follows a publish-subscribe messaging model. Producers publish messages to topics, and consumers subscribe to topics to receive and process those messages. Topics help organise and categorise data flow, allowing for scalable and flexible data distribution. Each message is associated with a topic, and topics are divided into partitions for parallel processing.

While Apache Kafka is primarily designed for real-time streaming and event-driven architectures, it is possible to use it in conjunction with other tools for batch-processing scenarios.

During this project, a topic will be created for each of the three tables in the RDS database. Each topic acts as a logical channel for a specific type of data. The data will be ingested into the Kafka topics, and then a batch processing framework (e.g., Apache Spark) will be utilised to consume data from the Kafka topics and process the data in batches. The processed results will be stored in a data lakehouse on Databricks.

### AWS IAM

AWS Identity and Access Management (IAM) is a web service provided by AWS that allows you to control access to AWS resources securely. IAM enables the management of users, groups, roles, and their corresponding access permissions within the AWS environment. IAM plays a crucial role in ensuring the security of AWS resources by providing a centralised and granular way to manage and control access.

This project uses IAM roles and groups to implement the principle of least privilege and follow security best practices in AWS environments to protect all other users.

## Configure Amazon EC2 instance to use as Apache Kafka machine

The following walkthrough will describe the process of configuring an Amazon `EC2` instance to use as an `Apache Kafka` machine.

### Setting up permissions

> [!Note]
>
> During this project, a `key pair parameter` was already created by the administrator.

Within the `EC2` console:

- Select `Instances` from the left-hand panel
- Identify and select the instance with the unique `` as supplied by AiCore.
- Under the `Details` tab, find the `Key pair assigned at launch`
- For this walkthrough, it will be referred to as `KEY_PAIR_NAME`

Within the `AWS Parameter Store` console:

- Find the specific `key pair parameter` associated with the EC2 instance
- Select this `key pair parameter`, and under the `Value` field, select `Show`. This will reveal the content of the `key pair`
- Copy its entire value (including the `BEGIN` and `END` header)

Within the VSCode:

- Create a `key pair` file locally, which is a file ending in the `.pem` extension. This file will allow you to connect to the EC2 instance
- Paste the `key pair parameter` value into the `.pem` file
- Save the created file using the following naming format (replacing `KEY_PAIR_NAME` as described above): `KEY_PAIR_NAME.pem`

In the local machine terminal:

- Use the following command to set the appropriate permissions for the private key file to ensure it is only accessible by the owner:

    ```bash
    chmod 400 /path/to/KEY_PAIR_NAME.pem
    ```

> [!NOTE]
>
> The command `chmod` is used to change the ownership and permission. In Linux, there are three types of ownership (`User`, `Group`, `Other`) and three types of permission (`Read`, `Write`, `Execute`).
>
> In the command `chmod 400`, the three digits are associated with:
>
> - 1st position shows ownership of `USER` === Here, 4
> - 2nd position shows ownership of `GROUP` === Here, 0
> - 3rd position shows ownership of `OTHER` === Here, 0
>
> The numbers represent:
>
> - 0=>`No permission`
> - 1=>`Excute`
> - 2=>`Write`
> - 3=>`Execute` + `Write`
> - 4=>`Read`
> - 5=>`Read` + `Execute`
> - 6=>`Read` + `Write`
> - 7=>`Read` + `Write` + `Execute`
>
> The command `chmod 400` therefore represents:
>
> - `READ` permission for `USER`
> - `NO` permission for `GROUP`
> - `NO` permission for `OTHER`

### Connect to the EC2 instance

In the local machine terminal:

- Use the `SSH` command to connect to the instance (replacing `/path/to/KEY_PAIR_NAME.pem` as described in [Setting up permissions](Walkthrough_part_1_EC2_Kafka#setting-up-permissions)):

    ```bash
    ssh -i "/path/to/KEY_PAIR_NAME.pem" ec2-user@public_dns_name
    ```

When accessing the `EC2` client using `SSH` for the first time, a message may be encountered about the authenticity of the host. This message is prompted because the `SSH` client does not recognise the remote host and wants to verify its authenticity to ensure secure communication.

Type `yes` to confirm and continue connecting. By doing so, the key fingerprint will be stored in the `SSH` client's `known_hosts` file, and future connections to the same host will not prompt the same message.

If, during this process, the local machine is logged off the instance, just run the `ssh` command again to reconnect.

### Set up Kafka on the EC2 instance

To connect to the `IAM authenticated MSK cluster`, the appropriate packages need to be installed on the `EC2` client machine.

On the client `EC2` machine:

- Install `Java 1.8.0` with the following command:

    ```bash
    sudo yum install java-1.8.0
    ```

- Install `Kafka 2.12-2.8.1` with the following command:

    ```bash
    wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz  
    ```

- Extract the downloaded Kafka archive by running the following command:

    ```bash
    tar -xzf kafka_kafka_2.12-2.8.1.tar
    ```

- Running the following command will output the following image:

    ```bash
    cd /home/ec2-user/kafka_2.12-2.8.1
    ls -al
    ```

    ![Alt](README_Images/Kafka_Install.png)

### Setting up IAM authentication

To connect to a cluster that uses IAM authentication, follow these additional steps.

On the client `EC2` machine:

- Inside the `libs` folder, download the `IAM MSK authentication` package from Github using the following command:

    ```bash
    cd /home/ec2-user/kafka_2.12-2.8.1/libs
    wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar
    ```

### Setting up CLASSPATH environment variable

In order to ensure that the Amazon MSK IAM libraries are easily accessible to the Kafka client, regardless of the location from which commands are executed, the environment variable `CLASSPATH` needs to be set up.

On the client EC2 machine:

- To set up the `CLASSPATH` environment variable, use the following command:

    ```bash
    export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar
    ```

When opening a new session or restarting an `EC2` instance, any environment variables that were set in previous sessions will not persist. Therefore, to maintain the `CLASSPATH` environment variable across sessions, add the export command to the `.bashrc` file located in the home directory of the `ec2-user`.

On the client `EC2` machine:

- Open `.bashrc` using the command:

    ```bash
    cd /home/ec2-user/
    nano ~/.bashrc
    ```

- Add the same export command to the `.bashrc` file:

    ```bash
    export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar
    ```

- Once the export command has been inserted into the `.bashrc`, make sure to save the changes before exiting.
- After making changes to the `.bashrc` file, run the source command to apply the changes to the current session:

    ```bash
    source ~/.bashrc
    ```

- To verify if the `CLASSPATH` environment variable was correctly set, use the `echo` command to display its value:

    ```bash
    echo $CLASSPATH
    ```

- If the `CLASSPATH` was set correctly, this command will output the path assigned to it, which in this case is `/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar`

### Configure Kafka client to use AWS IAM

Within the `IAM` console:

- Select `Roles` from the left-hand panel
- Select the desired `ec2-access-role`
- Note the `ARN` for the role
- For this walkthrough, it will be referred to as `IAM_ARN`

On the client `EC2` machine:

- To configure a Kafka client to use AWS `IAM` for authentication, create a `client.properties` file using the following command:

    ```bash
    cd /home/ec2-user/kafka_2.12-2.8.1/bin
    nano client.properties
    ```

- The `client.properties` file should contain the following (replacing `IAM_ARN` as described above):

    ```bash
    # Sets up TLS for encryption and SASL for authN.
    security.protocol = SASL_SSL

    # Identifies the SASL mechanism to use.
    sasl.mechanism = AWS_MSK_IAM

    # Binds SASL client implementation. Uses the specified profile name to look for credentials.
    sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="IAM_ARN";

    # Encapsulates constructing a SigV4 signature based on extracted credentials.
    # The SASL client bound by "sasl.jaas.config" invokes this class.
    sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
    ```

- Once the previous text has been inserted into the `client.properties` file, make sure to save the changes before exiting.

## Conclusion

In conclusion, this walkthrough delves into the integration of Amazon RDS, Amazon EC2, Apache Kafka, and AWS IAM within the project. Amazon RDS, a fully managed relational database service, is employed to store data resembling information received by the Pinterest API. Amazon EC2 serves as the host for the Apache Kafka machine, while Kafka, with its high-throughput and fault-tolerant design, is utilised for building data pipelines.

The AWS IAM service is central to ensuring secure access control for AWS resources. The configuration of the EC2 instance as an Apache Kafka machine is meticulously explained, covering aspects such as setting up permissions, connecting to the EC2 instance, and installing necessary packages like Java and Kafka.

Additionally, the walkthrough outlines the steps for IAM authentication setup, including downloading the IAM MSK authentication package and configuring the CLASSPATH environment variable. The client EC2 machine is configured to use AWS IAM for authentication, enhancing security measures.

In essence, this walkthrough equips users with a detailed understanding of emulating Pinterest's data processing system through batch processing on the AWS Cloud. The combination of Amazon RDS, Amazon EC2, Apache Kafka, and AWS IAM forms a robust framework for handling large volumes of data systematically and efficiently, providing valuable insights for business intelligence, model training, and overall platform optimisation.
