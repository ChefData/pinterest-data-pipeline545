from decouple import config
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import logging
import json
import requests


# Configure the logging module
logging.basicConfig(level=logging.INFO)

class APICommunicator:
    """
    A class for communicating with an API and sending data to Kafka topics.
    """

    def __init__(self) -> None:
        """
        Initializes the APICommunicator with the necessary configuration.
        """
        invoke_url = config('invoke_URL')
        deployment_stage = config('deployment_stage')
        iam_username = config('iam_username')
        self.invoke_url = f"{invoke_url}/{deployment_stage}/topics/{iam_username}"

    @staticmethod
    def make_request_with_retry(url, headers, payload):
        """
        Makes an HTTP POST request to the specified URL with retry mechanism.

        Parameters:
        - url (str): The URL to make the request to.
        - headers (dict): The headers for the request.
        - payload (str): The payload to be sent in the request.

        Returns:
        - response (Response): The response object if the request is successful, else None.
        """
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))

        try:
            response = session.post(url, headers=headers, data=payload, timeout=(3, 30))
            # Raise an HTTPError for bad responses
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as error:
            logging.error(f"Error: {error}")
            return None

    def _send_data_to_api(self, topic_name, data) -> None:
        """
        Sends data to a specified Kafka topic through the configured API.

        Parameters:
        - topic_name (str): The name of the Kafka topic.
        - data (dict): The data to be sent to the Kafka topic.
        """
        topic_url = f"{self.invoke_url}.{topic_name}"
        
        # To send JSON messages they need to follow this structure 
        payload = json.dumps({
            "records": [{"value": data}]
        })

        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

        # Use the make_request_with_retry function to handle retries
        response = self.make_request_with_retry(topic_url, headers, payload)

        if response:
            # Log the successful connection
            logging.info(f'Successful connection for {topic_name}: {response.status_code}')
            logging.info(response.json())
        else:
            logging.error(f'Request failed for {topic_name} even after retries.')