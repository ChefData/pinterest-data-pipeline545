from classes.aws_db_connector import AWSDBConnector
import logging

# Configure the logging module
log_level: int = logging.INFO
logging.basicConfig(level=log_level)

if __name__ == "__main__":
    try:
        # Start the data processing loop
        topics_dict: dict = {'pin': 'pinterest_data', 'geo': 'geolocation_data', 'user': 'user_data'}
        
        try:
            connector = AWSDBConnector(topics_dict)
            logging.info('Working')
            connector.run_infinite_post_data_loop(streaming=False)
        except KeyboardInterrupt:
            # Gracefully handle KeyboardInterrupt (Ctrl+C)
            logging.info('KeyboardInterrupt: Stopping the data processing loop.')
        except Exception as connector_error:
            logging.error(f"Error during AWSDBConnector initialisation: {connector_error}")
        finally:
            logging.info('Exiting the script')

    except Exception as main_error:
        logging.error(f"Error during main execution: {main_error}")
