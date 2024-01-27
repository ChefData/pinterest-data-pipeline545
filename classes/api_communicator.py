from datetime import datetime
from decouple import config
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import logging, json, requests, uuid


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
        self.invoke_url = config('invoke_URL')
        self.deployment_stage = config('deployment_stage')
        self.iam_username = config('iam_username')

    @staticmethod
    def make_request_with_retry(method, url, headers, payload):
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
            response = session.request(method, url, headers=headers, data=payload, timeout=(3, 30))
            # Raise an HTTPError for bad responses
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as error:
            logging.error(f"Error: {error}")
            return None

    def __encode_datetime(self, obj):
        return obj.isoformat() if isinstance(obj, datetime) else None

    def __encode_to_json(self, data):
        return json.dumps(data, default=self.__encode_datetime)

    def _send_data_to_api(self, method, url, payload_dict, headers, topic_name) -> None:
        """
        Sends data to a specified endpoint through the configured API.

        Parameters:
        - url (str): The endpoint URL.
        - data (dict): The data to be sent to the endpoint.
        - headers (dict): The headers for the request.
        - topic_name (str): The name of the Kafka topic or stream.

        Returns:
        - None
        """
        payload = self.__encode_to_json(payload_dict)
        # Use the make_request_with_retry function to handle retries
        response = self.make_request_with_retry(method, url, headers, payload)
        if response:
            # Log the successful connection
            logging.info(f'Successful connection for {topic_name}: {response.status_code}')
            logging.info(response.json())
        else:
            logging.error(f'Request failed for {topic_name} even after retries.')

    def _send_data_batch_to_api(self, topic_name, data) -> None:
        """
        Sends data to a specified Kafka topic through the configured API.

        Parameters:
        - topic_name (str): The name of the Kafka topic.
        - data (dict): The data to be sent to the Kafka topic.
        """
        method = 'POST'
        url = f"{self.invoke_url}/{self.deployment_stage}/topics/{self.iam_username}.{topic_name}"
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        payload_dict = {"records": [{"value": data}]}
        self._send_data_to_api(method, url, payload_dict, headers, topic_name)

    def _send_data_stream_to_api(self, topic_name, data) -> None:
        """
        Sends data to a specified Kafka stream through the configured API.

        Parameters:
        - topic_name (str): The name of the Kafka stream.
        - data (dict): The data to be sent to the Kafka stream.
        """
        method = 'PUT'
        stream_name = f"streaming-{self.iam_username}-{topic_name}"
        url = f"{self.invoke_url}/{self.deployment_stage}/streams/{stream_name}/record"
        headers = {'Content-Type': 'application/json'}
        payload_dict = {"StreamName": stream_name, "Data": data, "PartitionKey": str(uuid.uuid4())}
        self._send_data_to_api(method, url, payload_dict, headers, stream_name)
