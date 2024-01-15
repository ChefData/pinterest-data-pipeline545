from classes.api_communicator import APICommunicator
from classes.database_connector import DatabaseConnector
from sqlalchemy import text, bindparam
from time import sleep
import logging
import signal
import random

# Sets the seed for the random number generator, ensuring that if the program is ran multiple times, it will produce the same sequence of random numbers each time.
random.seed(100)

# Configure the logging module
logging.basicConfig(level=logging.INFO)

# Constants
PINTEREST_TABLE = "pinterest_data"
GEOLOCATION_TABLE = "geolocation_data"
USER_TABLE = "user_data"

class AWSDBConnector:
    """Class for connecting to a database and sending data to an API."""

    def __init__(self) -> None:
        """Initialize the database and API connections."""
        try:
            # Initialize connections
            self.database_connector = DatabaseConnector()
            self.api_communicator = APICommunicator()
            # Register a signal handler for graceful termination
            self.stop_flag: bool = False
            signal.signal(signal.SIGINT, self.__handle_signal)
        except Exception as error:
            logging.error(f"Error during initialization: {error}")
            raise

    def __handle_signal(self, signum: int, frame) -> None:
        """Handle the termination signal."""
        logging.info("Received signal to stop. Exiting gracefully.")
        self.stop_flag = True

    @staticmethod
    def __get_random_row(connection, table_name: str, random_row: int) -> dict:
        """
        Get a random row from the specified table.

        Parameters:
        - connection: The database connection.
        - table_name: The name of the table to query.
        - random_row: The randomly chosen row number.

        Returns:
        A dictionary representing the fetched row.
        """
        try:
            # Use prepared statement with parameter binding to prevent SQL injection
            query = text(f"SELECT * FROM {table_name} LIMIT :random_row, 1")
            query = query.bindparams(bindparam("random_row", random_row))
            selected_row = connection.execute(query)
            for row in selected_row:
                return dict(row._mapping)
        except Exception as error:
            logging.error(f"Error during database query: {error}")
            raise

    @staticmethod
    def __convert_datetime(data_dict: dict, key: str) -> None:
        """
        Convert datetime objects in the data dictionary to formatted strings.

        Parameters:
        - data_dict: The dictionary containing data.
        - key: The key representing the datetime field to convert.
        """
        try:
            # Convert datetime objects to formatted strings
            if key in data_dict and data_dict[key] is not None:
                data_dict[key] = data_dict[key].strftime('%Y-%m-%d %H:%M:%S')
        except Exception as error:
            logging.error(f"Error during datetime conversion: {error}")
            raise

    def _send_data_batch_to_api(self, data_type: str, data_list: list) -> None:
        """
        Convert datetime fields and send data to the API in batches.

        Parameters:
        - data_type: The type of data being sent.
        - data_list: A list of dictionaries representing the data to send.
        """
        try:
            # Convert datetime fields and send data to the API in batches
            for data in data_list:
                self.__convert_datetime(data, "timestamp")
                self.__convert_datetime(data, "date_joined")
                self.api_communicator._send_data_to_api(data_type, data)
        except Exception as error:
            logging.error(f"Error during API communication: {error}")
            raise

    def run_infinite_post_data_loop(self) -> None:
        """Run an infinite loop to simulate continuous data processing."""
        try:
            while not self.stop_flag:
                # Introduce a random sleep to simulate asynchronous data fetching
                sleep(random.randrange(0, 2))
                random_row = random.randint(0, 11000)

                with self.database_connector.db_engine.connect() as connection:
                    # Fetch random rows from different tables
                    pin_result = self.__get_random_row(connection, PINTEREST_TABLE, random_row)
                    geo_result = self.__get_random_row(connection, GEOLOCATION_TABLE, random_row)
                    user_result = self.__get_random_row(connection, USER_TABLE, random_row)

                    # Log the fetched data
                    logging.info('Random example of pinterest_data: %s', pin_result)
                    logging.info('Random example of geolocation_data: %s', geo_result)
                    logging.info('Random example of user_data: %s', user_result)

                    # Send data to the API in batches
                    self._send_data_batch_to_api("pin", [pin_result])
                    self._send_data_batch_to_api("geo", [geo_result])
                    self._send_data_batch_to_api("user", [user_result])
        except Exception as error:
            logging.error(f"Unexpected error: {error}")
