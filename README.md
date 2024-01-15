# Pinterest Data Pipeline

## Table of Contents

- [Description of the project](#description-of-the-project)
    - [What the project does](#what-the-project-does)
    - [Aim of the project](#aim-of-the-project)
    - [Lessons learned](#lessons-learned)
- [Installation instructions](#installation-instructions)
- [Usage instructions](#usage-instructions)
    - [Environment Setup](#environment-setup)
    - [AWS Configuration](#aws-configuration)
    - [Credential Setup](#credential-setup)
    - [Project Navigation](#project-navigation)
- [Classes and Methods](#classes-and-methods)
- [File structure of the project](#file-structure-of-the-project)
- [Technologies Used](#technologies-used)
- [Troubleshooting](#troubleshooting)
- [License information](#license-information)

In this project, we'll use different services running in the AWS cloud.

The user_posting_emulation.py contains the login credentials for a RDS database, which contains three tables with data resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest:

- pinterest_data contains data about posts being updated to Pinterest
- geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
- user_data contains data about the user that has uploaded each post found in pinterest_data

Run the provided script and print out pin_result, geo_result and user_result. These each represent one entry in their corresponding table.

## Batch Processing: Configure the EC2 Kafka client

### Create a .pem key file locally

You will first need to create a key pair file locally, which is a file ending in the .pem extension. This file will ultimately allow you to connect to your EC2 instance. To do this, first navigate to Parameter Store in your AWS account.

Find the specific key pair associated with your EC2 instance. Select this key pair and under the Value field select Show. This will reveal the content of your key pair. Copy its entire value (including the BEGIN and END header) and paste it in the .pem file in VSCode.

Navigate to the EC2 console and identify the instance with your unique UserId. Select this instance, and under the Details section find the Key pair name and make a note of this.

Save the previously created file in the VSCode using the following format: Key pair name.pem.

### Connect to the EC2 instance

You are now ready to connect to your EC2 instance. Follow the Connect instructions (SSH client) on the EC2 console to do this.

1. Ensure you have the private key file (.pem) associated with the key pair used for the instance.
2. Open the terminal on your local machine. You will need to set the appropriate permissions for the private key file to ensure it is only accessible by the owner: chmod 400 “/path/to/private_key.pem”
3. Use the SSH command to connect to the instance. You can find the exact command to connect to your EC2 instance under Example in the SSH client tab. If you are already in the folder where your .pem file is located you don't need to specify the file path. The command should have the following structure:

    ```bash
    ssh -i "/path/to/private_key.pem" ec2-user@public_dns_name
    ```

4. When accessing the EC2 client using SSH for the first time you may encounter a message about the authenticity of the host. This message is prompted because the SSH client does not recognise the remote host and wants to verify its authenticity to ensure secure communication. You can type yes to confirm and continue connecting. By doing so, the key fingerprint will be stored in your SSH client's known_hosts file, and future connections to the same host will not prompt the same message. If during this process you are logged off the instance just run the ssh command again and you will be reconnected.

### Set up Kafka on the EC2 instance

In order to connect to the IAM authenticated MSK cluster, you will need to install the appropriate packages on your EC2 client machine.
Don't worry about setting up the security rules for the EC2 instance to allow communication with the MSK cluster, as they have already been set up for you.

1. Install Java 1.8.0 on your client EC2 machine.

    ```bash
    sudo yum install java-1.8.0
    ```

2. Install Kafka on your client EC2 machine. Make sure to install the same version of Kafka as the one the cluster is running on (in this case 2.12-2.8.1), otherwise you won't be able to communicate with the MSK cluster.

    ```bash
    wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz  
    ```

3. Extract the downloaded Kafka archive by running the following command:

    ```bash
    tar -xzf kafka_kafka_2.12-2.8.1.tar
    ```

4. Move into the Kafka directory by running the command:

    ```bash
    cd /home/ec2-user/kafka_2.12-2.8.1
    ```

5. Now if you run the following the output of this command should look similar to the following:

    ```bash
    ls -al
    ```

    ![Alt](README_Images/Kafka_Install.png)

    > [!NOTE]
    > MSK clusters also support IAM authentication. IAM access control allows MSK to enable both authentication and authorisation for clusters. This means, that if a client tries to write something to the cluster, MSK uses IAM to check whether the client is an authenticated identity and also whether it is authorised to produce to the cluster.
    > To connect to a cluster that uses IAM authentication, we will need to follow additional steps before we are ready to create a topic on our client machine.

6. Navigate to the libs folder.

    ```bash
    cd /home/ec2-user/kafka_2.12-2.8.1/libs
    ```

7. Inside here we will download the IAM MSK authentication package from Github, using the following command:

    ```bash
    wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar
    ```

    > [!NOTE]
    > After downloading the file aws-msk-iam-auth-1.1.5-all.jar from the provided link, you will find it located inside the libs directory. In order to ensure that the Amazon MSK IAM libraries are easily accessible to the Kafka client, regardless of the location from which commands are executed, we will set up an environment variable called CLASSPATH.

8. To set up the CLASSPATH environment variable, you can use the following command:

    ```bash
    export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar
    ```

    > [!NOTE]
    > When you open a new session or restart an EC2 instance, any environment variables that were set in previous sessions will not persist. Therefore, if you want to maintain the CLASSPATH environment variable across sessions, you will need to re-run the export command each time you open a new session or restart the EC2 instance.
    > To automate this process and avoid manually running the export command every time, you can add the export command to the .bashrc file located in the home directory of the ec2-user (/home/ec2-user/.bashrc).

9. Open .bashrc using the command:

    ```bash
    nano ~/.bashrc
    ```

10. Add the same export command to the .bashrc file, it will be executed automatically whenever a new session is started for that user. Once you have typed the export command in the .bashrc, make sure to save the changes before exiting.

    ```bash
    export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar
    ```

11. After making changes to the .bashrc file, you need to run the source command to apply the changes to the current session:

    ```bash
    source ~/.bashrc
    ```

12. To verify if the CLASSPATH environment variable was set properly, you can use the echo command to display its value. If the CLASSPATH was set correctly, the command will output the path you assigned to it, which in your case is /home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar:

    ```bash
    echo $CLASSPATH
    ```

13. To configure a Kafka client to use AWS IAM for authentication you should first navigate to your Kafka installation folder, and then in the bin folder.

    ```bash
    cd /home/ec2-user/kafka_2.12-2.8.1/bin
    ```

14. Here, you should create a client.properties file, using the following command:

    ```bash
    nano client.properties
    ```

15. The client.properties file should contain the following, with the 'Credential Profile Name' changed:

```bash
# Sets up TLS for encryption and SASL for authN.
security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation. Uses the specified profile name to look for credentials.
sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="IAM ARN";

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

### Create Kafka topics

1. To create a topic, make sure you are inside your <KAFKA_FOLDER>/bin 

    ```bash
    cd /home/ec2-user/kafka_2.12-2.8.1/bin
    ```

2. Then run the following commands, replacing BootstrapServerString with the connection string you have previously saved, and <UserId> with the AWS IAM Username:

    - For the Pinterest posts data

    ```bash
    ./kafka-topics.sh --bootstrap-server BootstrapServerString --command-config client.properties --create --topic <UserId>.pin
    ```

    - For the post geolocation data

    ```bash
    ./kafka-topics.sh --bootstrap-server BootstrapServerString --command-config client.properties --create --topic <UserId>.geo
    ```

    - For the post user data

    ```bash
    ./kafka-topics.sh --bootstrap-server BootstrapServerString --command-config client.properties --create --topic <UserId>.user
    ```

## Batch Processing: Connect a MSK cluster to a S3 bucket

### Create a custom plugin with MSK Connect

> [!NOTE]
> The following were already configured in the AWS account
>
> - a S3 bucket
> - an IAM role that can write to this bucket
> - a VPC Endpoint to S3

1. In the S3 console:
    - Find the bucket associated with the UserId.
    - The bucket name should have the following format: user-<UserId>-bucket.
    - Make a note of the bucket name, as it will needed in the following steps.

2. In the EC2 client machine:
    - Download the Confluent.io Amazon S3 Connector and copy it to the S3 bucket.
    - This connector is a sink connector that exports data from Kafka topics to S3 objects in either JSON, Avro or Bytes format.
    - To download & copy this connector run the code below inside terminal, on your EC2 client machine, making sure to change the <BUCKET_NAME>:

    ```bash
    # assume admin user privileges
    sudo -u ec2-user -i
    # create directory where we will save our connector
    mkdir kafka-connect-s3 && cd kafka-connect-s3
    # download connector from Confluent
    wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip
    # copy connector to our S3 bucket
    aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/
    ```

3. In the S3 console:
    - Open the bucket associated with the UserId.
    - Open the newly created folder kafka-connect-s3/
    - Select the zip file and copy the S3 URI

4. In the MSK console:
    - Select 'Customised plugins' under the 'MSK Connect' section on the left side of the console
    - Choose Create custom plugin
    - Paste the S3 URI for the zip file
    - Create a custom plugin with the following name: <UserId>-plugin

### Create a connector with MSK Connect

1. In the MSK console:
    - Select Connectors under the MSK Connect section on the left side of the console
    - Choose Create connector
    - In the list of plugins, select the plugin you have just created, and then click Next.
    - For this project the AWS account only had permissions to create a connector with the following name: <UserId>-connector.
    - For the connector name choose the desired name, and then choose the pinterest-msk-cluster from the cluster list.
    - In the Connector configuration settings paste the following configuration, making sure to use the correct configurations for your connector:
        - The bucket name should be user-<UserId>-bucket
        - The topics.regex field in the connector configuration should follow the structure: <UserId>.*. This will ensure that data going through all the three previously created Kafka topics will get saved to the S3 bucket.

    ```bash
    connector.class=io.confluent.connect.s3.S3SinkConnector
    # same region as our bucket and cluster
    s3.region=us-east-1
    flush.size=1
    schema.compatibility=NONE
    tasks.max=3
    # include nomeclature of topic name, given here as an example will read all data from topic names starting with msk.topic....
    topics.regex=<UserId>.*
    format.class=io.confluent.connect.s3.format.json.JsonFormat
    partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
    value.converter.schemas.enable=false
    value.converter=org.apache.kafka.connect.json.JsonConverter
    storage.class=io.confluent.connect.s3.storage.S3Storage
    key.converter=org.apache.kafka.connect.storage.StringConverter
    s3.bucket.name=<BUCKET_NAME>
    ```

    - Change the 'Connector capacity type' to 'Provisioned'; ensure both the MCU count per worker and Number of workers are set to 1
    - Change 'Worker Configuration' to 'Use a customised configuration', then pick 'confluent-worker'
    - Under 'Access permissions' select the IAM role previously created: the role has the following format <UserId>-ec2-access-role
    - Leave the rest of the configurations as default.
    - Skip the rest of the pages.
    - Once your connector is up and running you will be able to visualise it in the Connectors tab in the MSK console.

    - Now that you have built the plugin-connector pair, data passing through the IAM authenticated cluster, will be automatically stored in the designated S3 bucket.
    - You are now ready to send data from your MSK cluster to your S3 bucket. Any data that will pass through your MSK cluster will now be automatically uploaded to its designated S3 bucket, in a newly created folder called topics.

## Batch Processing: Configuring an API in API Gateway

### Build a Kafka REST proxy integration method for the API

> [!NOTE]
For this project an API has been provided.

We will build a Kafka REST Proxy integration, which provides a RESTful interface to a Kafka cluster. This makes it easy to produce and consume messages, view the state of a cluster, or perform administrative actions without using native Kafka protocols or clients.

- In the 'EC2' console
  - Select 'Instances'
  - Select the client EC2 machine
  - Copy the 'Public IPv4 DNS'

- In the 'API Gateway' console
  - Select previously created REST type API with regional endpoint
  - Select 'Create resource' to create a new child resource for our API

  ![Alt text](README_Images/API_Gateway>APIs>Create_resource.png)

  - Select the 'Proxy resource' toggle
  - For 'Resource Name' enter {proxy+}
  - Enable 'CORS (Cross Origin Resource Sharing)'
  - Select 'Create Resource'

  ![Alt text](README_Images/API_Gateway>APIs>Resources.png)
  - To set up an integration click on the 'ANY' resource

  ![Alt text](README_Images/API_Gateway>APIs>Resources>ANY.png)
  - Then click on the 'Edit integration' button

  ![Alt text](README_Images/API_Gateway>APIs>Resources>Edit_integration_request.png)
  - For 'Integration type' select 'HTTP'
  - Select the 'HTTP proxy integration' toggle

  HTTP proxy integration is a simple, yet powerful way of building APIs that allow web applications to access multiple resources on the integrated HTTP endpoint. In HTTP proxy integration, API Gateway simply passes client-submitted method requests to the backend. In turn the backend HTTP endpoint parses the incoming data request to determine the appropriate return responses.

  - For 'HTTP method' select 'ANY'

  By creating a proxy resource with the {proxy+} parameter and the ANY method, you can provide your integration with access to all available resources.

  > [!GitHub]
  > Creating a {proxy+} resource with HTTP proxy integration allows a streamlined setup where the API Gateway submits all request data directly to the backend with no intervention from API Gateway.
  > All requests and responses are handled by the backend - in this case the Confluent REST Proxy on the EC2 instance.

  - For the Endpoint URL, enter the Kafka Client Amazon EC2 Instance Public IPv4 DNS with the following format: http://<KafkaClientEC2InstancePublicDNS>:8082/{proxy}

  8082 is the default port the Confluent REST Proxy listens to

  > [!GitHub]
  > By installing a Confluent REST Proxy for Kafka on the EC2 instance, we can post data from the Pinterest emulator to a REST API on the Amazon API Gateway which in turn sends it via the proxy to update the Kafka topics on the MSK cluster without having to create and maintain producer programs locally on the EC2 instance.

  - Click 'Save'
  ![Alt text](README_Images/API_Gateway>APIs>Resources>Deply_API_button.png)
  - To deploy the API use the 'Deploy API' button in the top-right corner of the API page.
  ![Alt text](README_Images/API_Gateway>APIs>Resources>Deply_API.png)
  - For Deployment stage, choose 'New Stage'
  - For Stage name enter the desired stage name, for example 'Test'.
  - Click 'Deploy'
  - Make note of the Invoke URL after deploying the API. It will have the following structure: https://APIInvokeURL/Test

### Set up the Kafka REST proxy on the EC2 client

Now the Kafka REST Proxy integration for the API has been set up, it is time to set up the Kafka REST Proxy on the EC2 client machine.

Step 1:
First, install the Confluent package for the Kafka REST Proxy on your EC2 client machine.

Step 2:
Allow the REST proxy to perform IAM authentication to the MSK cluster by modifying the kafka-rest.properties file.

Step 3:
Start the REST proxy on the EC2 client machine.

1. Install Confluent package for REST proxy on EC2 client

- To be able to consume data using MSK from the API we have just created, we will need to download some additional packages on a client EC2 machine, that will be used to communicate with the MSK cluster.
- To install the REST proxy package run the following commands on your EC2 instance:

```bash
sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz
tar -xvzf confluent-7.2.0.tar.gz
```

- You should now be able to see a confluent-7.2.0 directory on your EC2 instance.

To configure the REST proxy to communicate with the desired MSK cluster, and to perform IAM authentication you first need to navigate to confluent-7.2.0/etc/kafka-rest.

```bash
cd /home/ec2-user/confluent-7.2.0/etc/kafka-rest
```

Inside here run the following command to modify the kafka-rest.properties file:

```bash
nano kafka-rest.properties
```

![Alt text](README_Images/kafka-rest.properties.png)

Firstly, you need to modify the bootstrap.servers and the zookeeper.connect variables in this file, with the corresponding Bootstrap server string and Plaintext Apache Zookeeper connection string respectively. This will allow the REST proxy to connect to our MSK cluster.

Secondly, to surpass the IAM authentication of the MSK cluster, we will make use of the IAM MSK authentication package again (can be found here: https://github.com/aws/aws-msk-iam-auth). That means, you should add the following to your kafka-rest.properties file:

```bash
# Sets up TLS for encryption and SASL for authN.
client.security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
client.sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="<IAM ARN>";

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Notice the difference from the Kafka client.properties file we set-up in the MSK Essentials lesson and this one.
To allow communication between the REST proxy and the cluster brokers, all configurations should be prefixed with client.

Starting the REST proxy
Before sending messages to the API, in order to make sure they are consumed in MSK, we need to start our REST proxy. To do this, first navigate to the confluent-7.2.0/bin folder, and then run the following command:

```bash
cd /home/ec2-user/confluent-7.2.0/bin
./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties
```

If everything went well, and your proxy is ready to receive requests from the API, you should see a INFO Server started, listening for requests... in your EC2 console.

![Alt text](README_Images/listening_for_requests.png)

The Kafka related modules are now ready to accept data from our Pinterest users.

### Send data to the API

You are ready to send data to your API, which in turn will send the data to the MSK Cluster using the plugin-connector pair previously created.

Step 1:
The user_posting_emulation.py has been supplied to send data to the Kafka topics using the API Invoke URL. It will send data from the three tables to their corresponding Kafka topic.

To run this program:

```bash
python user_posting_emulation.py
```

Step 2:
Check data is sent to the cluster by running a Kafka consumer (one per topic). If everything has been set up correctly, you should see messages being consumed.

To create a message consumer open a new window on your client machine and run the following command:

```bash
cd /home/ec2-user/kafka_2.12-2.8.1/bin
./kafka-console-consumer.sh --bootstrap-server <BootstrapServerString> --consumer.config client.properties --group <consumer_group> --topic <topic_name> --from-beginning
```

Replace <your_bootstrap_servers> with the comma-separated list of bootstrap servers for your Kafka cluster and <your_topic_name> with the actual topic name.
If you are using SSL for authentication, you might need to provide additional configurations such as --consumer.config with the path to a consumer properties file containing security settings.
Ensure that the Kafka consumer is running while you execute the _send_data_to_api method in your Python code. If everything is set up correctly, you should see the messages being consumed by the Kafka consumer.
Keep in mind that the --from-beginning option is used to start consuming from the beginning of the topic. If you want to see only new messages, omit this option.

```bash
cd /home/ec2-user/kafka_2.12-2.8.1/bin
./kafka-console-consumer.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --consumer.config client.properties --group students --topic 0ab336d6fcf7.pin --from-beginning

cd /home/ec2-user/kafka_2.12-2.8.1/bin
./kafka-console-consumer.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --consumer.config client.properties --group students --topic 0ab336d6fcf7.geo --from-beginning

cd /home/ec2-user/kafka_2.12-2.8.1/bin
./kafka-console-consumer.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --consumer.config client.properties --group students --topic 0ab336d6fcf7.user --from-beginning
```

Step 3:
Check if data is getting stored in the S3 bucket. Notice the folder organization (e.g topics/<your_UserId>.pin/partition=0/) that your connector creates in the bucket.





If you are using Confluent REST Proxy for Kafka, you can make HTTP requests to it to produce and consume messages. You won't need the confluent_kafka library in this case. Here's a modified version of the KafkaConsumer class that uses requests to interact with Confluent REST Proxy:
Make sure to replace 'example_topic' with your actual Kafka topic name and 'http://your_rest_proxy_ip:your_rest_proxy_port' with the actual URL of your Confluent REST Proxy.
This code uses the HTTP REST API provided by Confluent REST Proxy to consume messages. It continuously sends HTTP requests to the REST Proxy to retrieve messages from the specified Kafka topic.












## Technologies Used

- Apache Kafka is an open-source technology for distributed data storage, optimised for ingesting and processing streaming data in real-time.
- Amazon Managed Streaming for Apache Kafka (Amazon MSK) is a fully managed service used to build and run applications that use Apache Kafka to process data.
- Amazon MSK Connect is a feature of AWS MSK, that allows users to stream data to and from their MSK-hosted Apache Kafka clusters. With MSK Connect, you can deploy fully managed connectors that move data into or pull data from popular data stores like Amazon S3 and Amazon OpenSearch Service, or that connect Kafka clusters with external systems, such as databases and file systems.
- Amazon Simple Storage Service (S3) is a scalable and highly available object storage service provided by AWS. Its primary purpose is to store and retrieve large amounts of data reliably, securely, and cost-effectively.
- Amazon Elastic Compute Cloud (EC2) is a key component of Amazon Web Services (AWS) and plays a vital role in cloud computing. EC2 provides a scalable and flexible infrastructure for hosting virtual servers, also known as instances, in the cloud.
- Virtual Private Cloud (VPC) is a virtual network infrastructure that allows you to provision a logically isolated section of the AWS cloud where you can launch AWS resources.
- IAM Roles are a fundamental component of Identity and Access Management (IAM) in cloud computing environments, designed to manage and control access to various AWS resources and services. An IAM Role is an entity with a set of permissions that determine what actions can be performed on specific resources.
- Amazon API Gateway is an AWS service that allows the creation, maintenance and securing of scalable REST, HTTP and Websocket APIs. APIs can be created to access AWS services and other web services or data stored in the AWS Cloud.

## License information

MIT License

Copyright (c) 2023 Nick Armstrong

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
