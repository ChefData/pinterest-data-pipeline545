from decouple import config
from sqlalchemy import create_engine, URL
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List
import yaml


class RDSDBConnector:
    """
    A class for connecting to a database using SQLAlchemy.
    """

    def __init__(self) -> None:
        """
        Initialises a DatabaseConnector object.

        Reads database credentials from a YAML file, validates them, and initialises the database engine.
        """
        try:
            self.creds_file: str = config('creds_path')
            self.db_engine: create_engine = self._init_db_engine()
        except Exception as error:
            raise RuntimeError(f"Error during initialisation: {error}")

    def __read_db_creds(self) -> Dict[str, str]:
        """
        Reads database credentials from a YAML file.

        Returns:
            dict: A dictionary containing the database credentials.
        Raises:
            FileNotFoundError: If the credentials file is not found.
            yaml.YAMLError: If there is an error loading YAML from the file.
        """
        try:
            # Read the database credentials from the YAML file
            with open(self.creds_file, 'r') as file:
                return yaml.safe_load(file)
        # Handle file not found error
        except FileNotFoundError as error:
            raise FileNotFoundError(f"Error: database credentials file '{self.creds_file}' not found: {error}")
        # Handle YAML parsing error
        except yaml.YAMLError as error:
            raise yaml.YAMLError(f"Error: Unable to load YAML from '{self.creds_file}': {error}")

    @staticmethod
    def __validate_db_creds(db_creds: Dict[str, str]) -> None:
        """
        Validates the database credentials.

        Args:
            db_creds (dict): Database credentials.
        Raises:
            ValueError: If any required key is missing in the credentials.
        """
        # Check if all required keys are present in the credentials
        required_keys: List[str]  = ['USER', 'PASSWORD', 'HOST', 'PORT', 'DATABASE']
        if any(key not in db_creds for key in required_keys):
            raise ValueError("Error: Database credentials are missing or incomplete.")

    @staticmethod
    def __build_url_object(db_creds: Dict[str, str]) -> URL:
        """
        Builds a SQLAlchemy URL object from database credentials.

        Args:
            db_creds (dict): Database credentials.
        Returns:
            URL: A SQLAlchemy URL object.
        Raises:
            ValueError: If there is an error creating the database URL.
        """
        try:
            # Create a SQLAlchemy URL object
            return URL.create(
                'mysql+pymysql',
                username=db_creds['USER'],
                #password=urllib.parse.quote_plus(db_creds['PASSWORD']),
                password=db_creds['PASSWORD'],
                host=db_creds['HOST'],
                port=db_creds['PORT'],
                database=db_creds['DATABASE'],
                query={'charset': 'utf8mb4'}
            )
        # Handle any errors that may occur during URL creation
        except ValueError as error:
            raise ValueError(f"Error creating database URL: {error}")

    def _init_db_engine(self) -> create_engine:
        """
        Initialises the database engine.

        Reads database credentials, validates them, builds a URL object, and creates the database engine.
        Returns:
            create_engine: A SQLAlchemy database engine.
        Raises:
            SQLAlchemyError: If there is an error initialising the database engine.
        """
        try:
            # Read database credentials from the YAML file
            db_creds: Dict[str, str] = self.__read_db_creds()
            # Check if all required keys are present in the credentials
            self.__validate_db_creds(db_creds)
            # Create a SQLAlchemy database URL
            db_url: URL = self._build_url_object(db_creds)
            # Initialise the database engine
            engine = create_engine(db_url)
            return engine
        except SQLAlchemyError as error:
            raise SQLAlchemyError(f"Error initialising database engine: {error}")