from classes.api_communicator import APICommunicator
from classes.rds_db_connector import RDSDBConnector
from sqlalchemy import text, bindparam
from time import sleep
import logging, signal, random


# Sets the seed for the random number generator, ensuring that if the program is ran multiple times, it will produce the same sequence of random numbers each time.
random.seed(100)

# Configure the logging module
log_level = logging.INFO
logging.basicConfig(level=log_level)

class AWSDBConnector:
    """Class for connecting to a database and sending data to an API."""

    def __init__(self, topics_dict: dict) -> None:
        """Initialize the database and API connections."""
        # Initialize connections
        self.database_connector = RDSDBConnector()
        self.api_communicator = APICommunicator()
        # Register a signal handler for graceful termination
        self.stop_flag: bool = False
        self.topics_dict: dict = topics_dict
        signal.signal(signal.SIGINT, self.__handle_signal)
        
    def __handle_signal(self, signum, frame) -> None:
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
            return dict(selected_row.first()._mapping) if selected_row.rowcount > 0 else {}
        except Exception as error:
            logging.error(f"Error during database query: {error}")
            raise

    def _simulate_asynchronous_data_fetching(self) -> None:
        """
        Introduce a random sleep to simulate asynchronous data fetching.
        """
        #sleep(random.randrange(0, 2))
        sleep(random.uniform(0, 2))

    def _process_data(self, streaming: bool) -> None:
        """
        Fetch random rows from different tables and send data to the API.

        Args:
            streaming (bool): If True, send data to the API in a streaming manner;
                              otherwise, send data in batches.
        """
        random_row = random.randint(0, 11000)
        with self.database_connector.db_engine.connect() as connection:
            for topic, table_name in self.topics_dict.items():
                result = self.__get_random_row(connection, table_name, random_row)
                logging.info(f'Random example of {table_name}: %s', result)
                if streaming:
                    self.api_communicator._send_data_stream_to_api(topic, [result])
                else:
                    self.api_communicator._send_data_batch_to_api(topic, [result])

    def run_infinite_post_data_loop(self, streaming: bool) -> None:
        """
        Run an infinite loop to simulate continuous data processing.

        Args:
            streaming (bool): If True, send data to the API in a streaming manner;
                              otherwise, send data in batches.
        """
        while not self.stop_flag:
            try:
                self._simulate_asynchronous_data_fetching()
                self._process_data(streaming)
            except Exception as error:
                logging.error(f"Unexpected error: {error}")


