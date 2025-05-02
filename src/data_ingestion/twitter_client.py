# Twitter API client to fetch tweets

import requests
import logging
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class TwitterClient:
    """Client for interacting with the Twitter API"""
    
    def __init__(self, bearer_token, keywords):
        """
        Initialize Twitter client
        
        Args:
            bearer_token (str): Twitter API bearer token
            keywords (list): List of keywords to track
        """
        self.bearer_token = bearer_token
        self.keywords = keywords
        self.base_url = "https://api.twitter.com/2/tweets/search/recent"
        self.headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json"
        }
    
    def get_tweets(self, max_results=100):
        """
        Get recent tweets matching the keywords
        
        Args:
            max_results (int): Maximum number of tweets to retrieve
            
        Returns:
            list: List of tweet objects
        """
        # Calculate start time (15 minutes ago)
        start_time = (datetime.utcnow() - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Construct query from keywords
        query = " OR ".join([f'"{keyword}"' for keyword in self.keywords])
        
        # Set up parameters
        params = {
            "query": query,
            "max_results": max_results,
            "start_time": start_time,
            "tweet.fields": "created_at,public_metrics,lang",
            "user.fields": "name,username,location,verified,profile_image_url",
            "expansions": "author_id"
        }
        
        try:
            # Make request to Twitter API
            response = requests.get(self.base_url, headers=self.headers, params=params)
            
            # Check if request was successful
            if response.status_code == 200:
                data = response.json()
                
                # Process tweets
                processed_tweets = self._process_tweets(data)
                
                logger.info(f"Retrieved {len(processed_tweets)} tweets")
                return processed_tweets
            else:
                logger.error(f"Twitter API error: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error retrieving tweets: {e}")
            return []
    
    def _process_tweets(self, data):
        """
        Process raw Twitter API response into a more usable format
        
        Args:
            data (dict): Raw Twitter API response
            
        Returns:
            list: List of processed tweet objects
        """
        processed_tweets = []
        
        # Check if we have tweets and users
        if "data" not in data or "includes" not in data or "users" not in data["includes"]:
            return processed_tweets
        
        # Create a map of user_id to user data
        users = {user["id"]: user for user in data["includes"]["users"]}
        
        # Process each tweet
        for tweet in data["data"]:
            # Get user data
            user = users.get(tweet["author_id"], {})
            
            # Create processed tweet object
            processed_tweet = {
                "id": tweet["id"],
                "text": tweet["text"],
                "created_at": tweet["created_at"],
                "lang": tweet.get("lang", "unknown"),
                "metrics": tweet.get("public_metrics", {}),
                "user": {
                    "id": user.get("id", ""),
                    "name": user.get("name", ""),
                    "username": user.get("username", ""),
                    "location": user.get("location", ""),
                    "verified": user.get("verified", False),
                    "profile_image_url": user.get("profile_image_url", "")
                },
                "source": "twitter",
                "keywords": [keyword for keyword in self.keywords if keyword.lower() in tweet["text"].lower()],
                "processed_at": datetime.utcnow().isoformat()
            }
            
            processed_tweets.append(processed_tweet)
        
        return processed_tweets