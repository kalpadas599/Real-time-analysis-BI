import os
from src.data_ingestion.google_ads_client import GoogleAdsClient
from src.data_ingestion.twitter_client import TwitterClient
from src.data_ingestion.confluent_producer import setup_confluent_producer

class DummyGoogleAdsClient(GoogleAdsClient):
    def __init__(self):
        pass

    def get_ad_performance(self):
        # Return dummy ad performance data
        return {
            "timestamp": "2025-05-01T12:00:00Z",
            "ads": [
                {
                    "type": "Pre-roll",
                    "impressions": 500000,
                    "clicks": 25000,
                    "ctr": 5.0,
                    "cost": 30000.0,
                    "conversions": 2000,
                    "cost_per_conversion": 15.0,
                    "regions": [
                        {"region": "India", "impressions": 200000, "clicks": 10000, "ctr": 5.0},
                        {"region": "USA", "impressions": 150000, "clicks": 7500, "ctr": 5.0},
                        {"region": "UK", "impressions": 150000, "clicks": 7500, "ctr": 5.0}
                    ]
                }
            ]
        }

def main():
    twitter_bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
    twitter_keywords = ["cricket", "ICC", "Champions Trophy"]
    twitter_client = TwitterClient(bearer_token=twitter_bearer_token, keywords=twitter_keywords)

    google_ads_client = DummyGoogleAdsClient()

    setup_confluent_producer(twitter_client, google_ads_client)

    import time
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
