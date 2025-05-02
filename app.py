import logging
import time
import os
import signal
import sys
from dotenv import load_dotenv
from src.storage.mongodb_client import MongoDBClient
from src.storage.snowflake_client import SnowflakeClient
from src.storage.firebase_client import FirebaseClient
from src.data_ingestion.twitter_client import TwitterClient
from src.data_ingestion.google_ads_client import GoogleAdsClient
from src.data_processing.spark_processor import SparkProcessor
from src.ml.sentiment_analyzer import SentimentAnalyzer
from src.api.flask_api import start_api_server
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the application."""
    load_dotenv()
    
    logger.info("Starting ICC Champions Trophy 2025 Analytics System")
    
    # Initialize storage clients
    logging.info("Initializing storage clients...")

    mongo_conn_str = os.getenv("MONGODB_CONNECTION_STRING")
    mongo_db_name = os.getenv("MONGODB_DATABASE")

    mongodb_client = MongoDBClient(connection_string=mongo_conn_str, database_name=mongo_db_name)
    #mongodb_client = MongoDBClient()
    


    # Try to initialize Snowflake with error handling
    try:
        snowflake_client = SnowflakeClient(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD')
        )
    except Exception as e:
        logger.error(f"Failed to initialize Snowflake client: {str(e)}")
        logger.warning("Continuing without Snowflake. Some features will be limited.")
        snowflake_client = None
    
    # Initialize Firebase with error handling
    try:
        firebase_client = FirebaseClient(
            credentials_path=os.getenv('FIREBASE_CREDENTIALS_PATH'),
            database_url=os.getenv('FIREBASE_DATABASE_URL')
        )
    except Exception as e:
        logger.error(f"Failed to initialize Firebase client: {str(e)}")
        logger.warning("Continuing without Firebase. Real-time updates will be disabled.")
        firebase_client = None
    
    # Initialize data source clients
    logger.info("Initializing data source clients...")
    try:
        twitter_client = TwitterClient(
            bearer_token=os.getenv('TWITTER_BEARER_TOKEN'),
            keywords=["cricket", "ICC", "Champions Trophy"]
        )
    except Exception as e:
        logger.error(f"Failed to initialize Twitter client: {str(e)}")
        logger.warning("Twitter data collection will be disabled.")
        twitter_client = None
    
    try:
        google_ads_client = GoogleAdsClient(
            client_id=os.getenv('GOOGLE_ADS_CLIENT_ID'),
            client_secret=os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
            developer_token=os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
            refresh_token=os.getenv('GOOGLE_ADS_REFRESH_TOKEN'),
            customer_id=os.getenv('GOOGLE_ADS_CUSTOMER_ID')
        )
    except Exception as e:
        logger.error(f"Failed to initialize Google Ads client: {str(e)}")
        logger.warning("Google Ads data collection will be disabled.")
        google_ads_client = None
    
    # Initialize processing components
    logger.info("Initializing processing components...")
    sentiment_analyzer = SentimentAnalyzer()
    spark_processor = SparkProcessor(
        mongodb_client=mongodb_client,
        snowflake_client=snowflake_client,
        firebase_client=firebase_client,
        sentiment_analyzer=sentiment_analyzer
    )
    
    
    # Start API server in a separate thread
    api_thread = threading.Thread(
        target=start_api_server,
        args=(mongodb_client, snowflake_client, firebase_client),
        daemon=True
    )
    api_thread.start()
    logger.info("API server started")
    
    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Shutting down...")
        if mongodb_client:
            mongodb_client.close()
        if snowflake_client:
            snowflake_client.close()
        if firebase_client:
            firebase_client.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Main processing loop
    try:
        logger.info("Starting main processing loop")
        while True:
            # Process data
            # This is a placeholder for the actual data processing logic
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        # Clean up resources
        if mongodb_client:
            mongodb_client.close()
        if snowflake_client:
            snowflake_client.close()
        if firebase_client:
            firebase_client.close()
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    main()
