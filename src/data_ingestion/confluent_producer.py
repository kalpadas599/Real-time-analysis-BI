# Confluent Kafka producer to ingest data from various sources

from confluent_kafka import Producer
import json
import logging
import threading
import time
import os

logger = logging.getLogger(__name__)

def setup_confluent_producer(twitter_client, google_ads_client):
    """
    Set up Confluent Kafka producer and start data ingestion threads
    
    Args:
        twitter_client: Twitter API client
        google_ads_client: Google Ads API client
        
    Returns:
        Producer: Configured Confluent Kafka producer
    """
    # Read Kafka configuration from environment variables
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    api_key = os.getenv('KAFKA_API_KEY')
    api_secret = os.getenv('KAFKA_API_SECRET')

    if not bootstrap_servers or not api_key or not api_secret:
        logger.error("Kafka configuration environment variables are missing")
        raise ValueError("Kafka configuration environment variables are missing")
    
    # Create Confluent Kafka producer configuration
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
    }
    
    # Create Confluent Kafka producer
    producer = Producer(conf)
    
    # Delivery callback function
    def delivery_callback(err, msg):
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    # Start Twitter data ingestion thread
    twitter_thread = threading.Thread(
        target=ingest_twitter_data,
        args=(producer, twitter_client, delivery_callback),
        daemon=True
    )
    twitter_thread.start()
    
    # Start Google Ads data ingestion thread
    google_ads_thread = threading.Thread(
        target=ingest_google_ads_data,
        args=(producer, google_ads_client, delivery_callback),
        daemon=True
    )
    google_ads_thread.start()
    
    # Start mock viewership data thread (in a real scenario, this would come from streaming platforms)
    viewership_thread = threading.Thread(
        target=ingest_mock_viewership_data,
        args=(producer, delivery_callback),
        daemon=True
    )
    viewership_thread.start()
    
    return producer

def ingest_twitter_data(producer, twitter_client, callback):
    """
    Continuously ingest data from Twitter API and send to Confluent Kafka
    
    Args:
        producer: Confluent Kafka producer
        twitter_client: Twitter API client
        callback: Delivery callback function
    """
    logger.info("Starting Twitter data ingestion")
    while True:
        try:
            # Get tweets from Twitter API
            tweets = twitter_client.get_tweets()
            
            # Send each tweet to Kafka
            for tweet in tweets:
                # Convert tweet to JSON
                tweet_json = json.dumps(tweet).encode('utf-8')
                
                # Produce message to Kafka
                producer.produce('twitter-data', value=tweet_json, callback=callback)
                
                # Trigger any available delivery callbacks
                producer.poll(0)
            
            logger.info(f"Sent {len(tweets)} tweets to Kafka")
            
            # Sleep for a short period to avoid hitting rate limits
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error ingesting Twitter data: {e}")
            time.sleep(30)  # Wait longer if there's an error

def ingest_google_ads_data(producer, google_ads_client, callback):
    """
    Continuously ingest data from Google Ads API and send to Confluent Kafka
    
    Args:
        producer: Confluent Kafka producer
        google_ads_client: Google Ads API client
        callback: Delivery callback function
    """
    logger.info("Starting Google Ads data ingestion")
    while True:
        try:
            # Get ad performance data from Google Ads API
            ad_data = google_ads_client.get_ad_performance()
            
            # Convert ad data to JSON
            ad_data_json = json.dumps(ad_data).encode('utf-8')
            
            # Produce message to Kafka
            producer.produce('ad-data', value=ad_data_json, callback=callback)
            
            # Trigger any available delivery callbacks
            producer.poll(0)
            
            logger.info("Sent Google Ads data to Kafka")
            
            # Sleep for a longer period as ad data doesn't change as frequently
            time.sleep(60)
        except Exception as e:
            logger.error(f"Error ingesting Google Ads data: {e}")
            time.sleep(120)  # Wait longer if there's an error

def ingest_mock_viewership_data(producer, callback):
    """
    Generate and ingest mock viewership data
    In a real scenario, this would come from streaming platforms' APIs
    
    Args:
        producer: Confluent Kafka producer
        callback: Delivery callback function
    """
    logger.info("Starting mock viewership data ingestion")
    
    import random
    from datetime import datetime
    
    regions = ["India", "Pakistan", "Australia", "UK", "USA", "UAE", "South Africa", "New Zealand"]
    platforms = ["Hotstar", "JioCinema", "YouTube", "Facebook", "Twitch"]
    
    while True:
        try:
            # Generate timestamp
            timestamp = datetime.now().isoformat()
            
            # Generate mock viewership data
            total_viewers = random.randint(2000000, 3000000)
            
            # Generate regional breakdown
            regional_data = []
            remaining_viewers = total_viewers
            
            for region in regions[:-1]:  # All except the last region
                if remaining_viewers > 0:
                    # Assign a random portion of remaining viewers to this region
                    region_viewers = min(
                        random.randint(100000, 1000000),
                        remaining_viewers
                    )
                    remaining_viewers -= region_viewers
                    
                    regional_data.append({
                        "region": region,
                        "viewers": region_viewers,
                        "engagement_rate": round(random.uniform(5, 15), 2)
                    })
            
            # Assign remaining viewers to the last region
            if remaining_viewers > 0:
                regional_data.append({
                    "region": regions[-1],
                    "viewers": remaining_viewers,
                    "engagement_rate": round(random.uniform(5, 15), 2)
                })
            
            # Generate platform breakdown
            platform_data = []
            remaining_viewers = total_viewers
            
            for platform in platforms[:-1]:  # All except the last platform
                if remaining_viewers > 0:
                    # Assign a random portion of remaining viewers to this platform
                    platform_viewers = min(
                        random.randint(100000, 1000000),
                        remaining_viewers
                    )
                    remaining_viewers -= platform_viewers
                    
                    platform_data.append({
                        "platform": platform,
                        "viewers": platform_viewers,
                        "buffer_rate": round(random.uniform(0.1, 5), 2),
                        "avg_resolution": random.choice(["480p", "720p", "1080p", "1440p", "4K"])
                    })
            
            # Assign remaining viewers to the last platform
            if remaining_viewers > 0:
                platform_data.append({
                    "platform": platforms[-1],
                    "viewers": remaining_viewers,
                    "buffer_rate": round(random.uniform(0.1, 5), 2),
                    "avg_resolution": random.choice(["480p", "720p", "1080p", "1440p", "4K"])
                })
            
            # Create viewership data object
            viewership_data = {
                "timestamp": timestamp,
                "total_viewers": total_viewers,
                "regional_data": regional_data,
                "platform_data": platform_data
            }
            
            # Convert viewership data to JSON
            viewership_data_json = json.dumps(viewership_data).encode('utf-8')
            
            # Produce message to Kafka
            producer.produce('viewership-data', value=viewership_data_json, callback=callback)
            
            # Trigger any available delivery callbacks
            producer.poll(0)
            
            logger.info("Sent mock viewership data to Kafka")
            
            # Sleep for a short period
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error generating mock viewership data: {e}")
            time.sleep(30)  # Wait longer if there's an error
