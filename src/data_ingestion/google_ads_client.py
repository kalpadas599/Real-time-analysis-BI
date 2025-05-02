# Google Ads API client to fetch ad performance data

import logging
from datetime import datetime, timedelta
import random  # For mock data generation

logger = logging.getLogger(__name__)

class GoogleAdsClient:
    """Mock Google Ads Client for demo/testing."""
    
    def __init__(self, client_id=None, client_secret=None, developer_token=None, refresh_token=None, customer_id=None):
        import logging
        logging.info("Initialized Google Ads client (using mock data for demonstration)")

    def fetch_ad_data(self, keywords):
        """Return simulated ad data"""
        return [
            {"keyword": keyword, "impressions": 1000, "clicks": 150, "cost": 25.0}
            for keyword in keywords
        ]
  
    def get_ad_performance(self):
        """
        Get ad performance data
        
        Returns:
            dict: Ad performance data
        """
        # In a real implementation, we would query the Google Ads API
        # For this example, we'll generate mock data
        
        # Generate timestamp
        timestamp = datetime.now().isoformat()
        
        # Define ad types
        ad_types = ["Pre-roll", "Mid-roll", "Banner", "Overlay", "Sponsored"]
        
        # Generate mock ad performance data
        ad_data = {
            "timestamp": timestamp,
            "ads": []
        }
        
        for ad_type in ad_types:
            # Generate random metrics
            impressions = random.randint(100000, 1000000)
            clicks = random.randint(1000, int(impressions * 0.05))  # Up to 5% CTR
            ctr = round((clicks / impressions) * 100, 2)
            cost = round(clicks * random.uniform(0.5, 2.0), 2)  # Cost per click between $0.50 and $2.00
            conversions = random.randint(10, int(clicks * 0.1))  # Up to 10% conversion rate
            
            # Add ad data
            ad_data["ads"].append({
                "type": ad_type,
                "impressions": impressions,
                "clicks": clicks,
                "ctr": ctr,
                "cost": cost,
                "conversions": conversions,
                "cost_per_conversion": round(cost / conversions if conversions > 0 else 0, 2),
                "regions": self._generate_regional_breakdown(impressions)
            })
        
        logger.info("Generated mock Google Ads performance data")
        return ad_data
    
    def _generate_regional_breakdown(self, total_impressions):
        """
        Generate mock regional breakdown of impressions
        
        Args:
            total_impressions (int): Total number of impressions
            
        Returns:
            list: Regional breakdown
        """
        regions = ["India", "Pakistan", "Australia", "UK", "USA", "UAE", "South Africa", "New Zealand"]
        regional_data = []
        remaining_impressions = total_impressions
        
        for region in regions[:-1]:  # All except the last region
            if remaining_impressions > 0:
                # Assign a random portion of remaining impressions to this region
                region_impressions = min(
                    random.randint(10000, 200000),
                    remaining_impressions
                )
                remaining_impressions -= region_impressions
                
                # Calculate other metrics
                clicks = random.randint(100, int(region_impressions * 0.05))  # Up to 5% CTR
                
                regional_data.append({
                    "region": region,
                    "impressions": region_impressions,
                    "clicks": clicks,
                    "ctr": round((clicks / region_impressions) * 100, 2)
                })
        
        # Assign remaining impressions to the last region
        if remaining_impressions > 0:
            clicks = random.randint(100, int(remaining_impressions * 0.05))  # Up to 5% CTR
            
            regional_data.append({
                "region": regions[-1],
                "impressions": remaining_impressions,
                "clicks": clicks,
                "ctr": round((clicks / remaining_impressions) * 100, 2)
            })
        
        return regional_data