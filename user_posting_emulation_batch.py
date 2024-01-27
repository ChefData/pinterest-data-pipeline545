from classes.aws_db_connector import AWSDBConnector
import logging


if __name__ == "__main__":
    try:
        # Configure logging and start the data processing loop
        logging.basicConfig(level=logging.INFO)
        topics_dict = {'pin': 'pinterest_data', 'geo': 'geolocation_data', 'user': 'user_data'}
        connector = AWSDBConnector(topics_dict)
        try:
            print('Working')
            connector.run_infinite_post_data_loop(streaming = False)
        except KeyboardInterrupt:
            # Gracefully handle KeyboardInterrupt (Ctrl+C)
            pass
        finally:
            print('Exiting the script')
    except Exception as e:
        logging.error(f"Error during main execution: {e}")