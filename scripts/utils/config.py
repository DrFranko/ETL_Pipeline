import logging

# Setup logging configuration
def setup_logging():
    logging.basicConfig(
        filename='etl_pipeline.log',  # Log file path
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
