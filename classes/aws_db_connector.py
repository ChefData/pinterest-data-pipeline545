from classes.api_communicator import APICommunicator
from classes.rds_db_connector import RDSDBConnector
from sqlalchemy import text, bindparam
from time import sleep
from typing import Dict, Union
import logging, signal, random


# Sets the seed for the random number generator, ensuring that if the program is ran multiple times, it will produce the same sequence of random numbers each time.
random.seed(100)

# Configure the logging module
log_level: int = logging.INFO
logging.basicConfig(level=log_level)

class AWSDBConnector:
    """Class for connecting to a database and sending data to an API."""

    def __init__(self, topics_dict: Dict[str, str]) -> None:
        """
        Initialise the database and API connections.

        Parameters:
        - topics_dict (Dict[str, str]): A dictionary mapping topic names to corresponding database table names.
        """
        try:
            # Initialise connections
            self.database_connector = RDSDBConnector()
            self.api_communicator = APICommunicator()
            # Register a signal handler for graceful termination
            self.stop_flag: bool = False
            self.topics_dict: Dict[str, str] = topics_dict
            signal.signal(signal.SIGINT, self.__handle_signal)
        except Exception as error:
            raise RuntimeError(f"Error during initialisation: {error}")

    def __handle_signal(self, signum: int, frame) -> None:
        """
        Handle the termination signal.

        Parameters:
        - signum (int): The signal number.
        - frame: The current execution frame.
        """
        logging.info("Received signal to stop. Exiting gracefully.")
        self.stop_flag = True

    @staticmethod
    def __get_random_row(connection, table_name: str, random_row: int) -> Dict[str, Union[int, str]]:
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
            return dict(selected_row.first()._mapping) if selected_row.rowcount > 0 else {}
        except Exception as error:
            logging.error(f"Error during database query: {error}")
            raise

    def __simulate_asynchronous_data_fetching(self) -> None:
        """
        Introduce a random sleep to simulate asynchronous data fetching.
        """
        try:
            sleep(random.uniform(0, 2))
        except Exception as error:
            logging.error(f"Error during data fetching simulation: {error}")

    def __process_data(self, streaming: bool) -> None:
        """
        Fetch random rows from different tables and send data to the API.

        Args:
            streaming (bool): If True, send data to the API in a streaming manner;
                              otherwise, send data in batches.
        """
        try:
            random_row = random.randint(0, 11000)
            with self.database_connector.db_engine.connect() as connection:
                for topic, table_name in self.topics_dict.items():
                    result: Dict[str, Union[int, str]] = self.__get_random_row(connection, table_name, random_row)
                    logging.info(f'Random example of {table_name}: %s', result)
                    data_to_send = [result]
                    if streaming:
                        self.api_communicator._send_data_stream_to_api(topic, data_to_send)
                    else:
                        self.api_communicator._send_data_batch_to_api(topic, data_to_send)
        except Exception as error:
            logging.error(f"Error during data processing: {error}")

    def run_infinite_post_data_loop(self, streaming: bool) -> None:
        """
        Run an infinite loop to simulate continuous data processing.

        Args:
            streaming (bool): If True, send data to the API in a streaming manner;
                              otherwise, send data in batches.
        """
        while not self.stop_flag:
            try:
                self.__simulate_asynchronous_data_fetching()
                self.__process_data(streaming)
            except Exception as error:
                logging.error(f"Unexpected error: {error}")


