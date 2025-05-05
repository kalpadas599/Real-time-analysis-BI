# Flask API for Power BI integration

import logging
import threading
import json
from datetime import datetime, timedelta
from flask import Flask, jsonify, request

logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

# Global variables for clients
mongodb_client = None
snowflake_client = None
firebase_client = None

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/viewership/current', methods=['GET'])
def current_viewership():
    """Get current viewership data"""
    try:
        # Get latest viewership data from Firebase
        data = firebase_client.get('viewership')
        
        if data:
            return jsonify(data)
        else:
            return jsonify({
                'error': 'No viewership data available'
            }), 404
    except Exception as e:
        logger.error(f"Error getting current viewership: {e}")
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/viewership/history', methods=['GET'])
def viewership_history():
    """Get historical viewership data"""
    try:
        # Get parameters
        hours = request.args.get('hours', default=1, type=int)
        
        # Calculate start time
        start_time = datetime.now() - timedelta(hours=hours)
        
        # Query Snowflake for regional viewership data
        query = f"""
            SELECT TIMESTAMP, REGION, VIEWERS, ENGAGEMENT_RATE
            FROM CRICKET_ANALYTICS.REGIONAL_VIEWERSHIP
            WHERE TIMESTAMP >= '{start_time.isoformat()}'
            ORDER BY TIMESTAMP
        """
        
        results = snowflake_client.execute_query(query, (start_time,))
        
        # Format results
        data = []
        for row in results:
            data.append({
                'timestamp': row[0].isoformat() if isinstance(row[0], datetime) else row[0],
                'region': row[1],
                'viewers': row[2],
                'engagement_rate': row[3]
            })
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting viewership history: {e}")
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/sentiment/current', methods=['GET'])
def current_sentiment():
    """Get current sentiment data"""
    try:
        # Get latest sentiment data from Firebase
        data = firebase_client.get('sentiment')
        
        if data:
            return jsonify(data)
        else:
            return jsonify({
                'error': 'No sentiment data available'
            }), 404
    except Exception as e:
        logger.error(f"Error getting current sentiment: {e}")
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/sentiment/history', methods=['GET'])
def sentiment_history():
    """Get historical sentiment data"""
    try:
        # Get parameters
        hours = request.args.get('hours', default=1, type=int)
        
        # Calculate start time
        start_time = datetime.now() - timedelta(hours=hours)
        
        # Query Snowflake for sentiment data
        query = f"""
            SELECT WINDOW_END, AVG_POSITIVE, AVG_NEUTRAL, AVG_NEGATIVE, AVG_COMPOUND
            FROM CRICKET_ANALYTICS.SENTIMENT
            WHERE WINDOW_END >= '{start_time.isoformat()}'
            ORDER BY WINDOW_END
        """
        
        results = snowflake_client.execute_query(query)
        
        # Format results
        data = []
        for row in results:
            data.append({
                'timestamp': row[0].isoformat() if isinstance(row[0], datetime) else row[0],
                'positive': row[1],
                'neutral': row[2],
                'negative': row[3],
                'compound': row[4]
            })
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting sentiment history: {e}")
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/ads/current', methods=['GET'])
def current_ads():
    """Get current ad performance data"""
    try:
        # Get latest ad data from Firebase
        data = firebase_client.get('ads')
        
        if data:
            return jsonify(data)
        else:
            return jsonify({
                'error': 'No ad data available'
            }), 404
    except Exception as e:
        logger.error(f"Error getting current ad performance: {e}")
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/ads/history', methods=['GET'])
def ads_history():
    """Get historical ad performance data"""
    try:
        # Get parameters
        hours = request.args.get('hours', default=1, type=int)
        
        # Calculate start time
        start_time = datetime.now() - timedelta(hours=hours)
        
        # Query Snowflake for ad performance data
        query = f"""
            SELECT TIMESTAMP, TYPE, IMPRESSIONS, CLICKS, CTR, COST, CONVERSIONS, COST_PER_CONVERSION
            FROM CRICKET_ANALYTICS.AD_PERFORMANCE
            WHERE TIMESTAMP >= '{start_time.isoformat()}'
            ORDER BY TIMESTAMP
        """
        
        results = snowflake_client.execute_query(query)
        
        # Format results
        data = []
        for row in results:
            data.append({
                'timestamp': row[0].isoformat() if isinstance(row[0], datetime) else row[0],
                'type': row[1],
                'impressions': row[2],
                'clicks': row[3],
                'ctr': row[4],
                'cost': row[5],
                'conversions': row[6],
                'cost_per_conversion': row[7]
            })
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting ad performance history: {e}")
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/tweets/recent', methods=['GET'])
def recent_tweets():
    """Get recent tweets"""
    try:
        # Get parameters
        limit = request.args.get('limit', default=10, type=int)
        
        # Query MongoDB for recent tweets
        tweets = mongodb_client.find_many('tweets', {}, limit=limit)
        
        # Format results
        data = []
        for tweet in tweets:
            # Remove MongoDB _id field
            if '_id' in tweet:
                del tweet['_id']
            
            data.append(tweet)
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting recent tweets: {e}")
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/api/export/powerbi', methods=['GET'])
def export_for_powerbi():
    """Export data in a format suitable for Power BI"""
    try:
        # Get parameters
        data_type = request.args.get('type', default='all')
        hours = request.args.get('hours', default=1, type=int)
        
        # Calculate start time
        start_time = datetime.now() - timedelta(hours=hours)
        
        result = {}
        
        # Get viewership data if requested
        if data_type in ['all', 'viewership']:
            # Query Snowflake for regional viewership data
            query = f"""
                SELECT TIMESTAMP, REGION, VIEWERS, ENGAGEMENT_RATE
                FROM CRICKET_ANALYTICS.REGIONAL_VIEWERSHIP
                WHERE TIMESTAMP >= '{start_time.isoformat()}'
                ORDER BY TIMESTAMP
            """
            
            viewership_results = snowflake_client.execute_query(query)
            
            # Format results
            viewership_data = []
            for row in viewership_results:
                viewership_data.append({
                    'timestamp': row[0].isoformat() if isinstance(row[0], datetime) else row[0],
                    'region': row[1],
                    'viewers': row[2],
                    'engagement_rate': row[3]
                })
            
            result['viewership'] = viewership_data
        
        # Get sentiment data if requested
        if data_type in ['all', 'sentiment']:
            # Query Snowflake for sentiment data
            query = f"""
                SELECT WINDOW_END, AVG_POSITIVE, AVG_NEUTRAL, AVG_NEGATIVE, AVG_COMPOUND
                FROM CRICKET_ANALYTICS.SENTIMENT
                WHERE WINDOW_END >= '{start_time.isoformat()}'
                ORDER BY WINDOW_END
            """
            
            sentiment_results = snowflake_client.execute_query(query)
            
            # Format results
            sentiment_data = []
            for row in sentiment_results:
                sentiment_data.append({
                    'timestamp': row[0].isoformat() if isinstance(row[0], datetime) else row[0],
                    'positive': row[1],
                    'neutral': row[2],
                    'negative': row[3],
                    'compound': row[4]
                })
            
            result['sentiment'] = sentiment_data
        
        # Get ad data if requested
        if data_type in ['all', 'ads']:
            # Query Snowflake for ad performance data
            query = f"""
                SELECT TIMESTAMP, TYPE, IMPRESSIONS, CLICKS, CTR, COST, CONVERSIONS, COST_PER_CONVERSION
                FROM CRICKET_ANALYTICS.AD_PERFORMANCE
                WHERE TIMESTAMP >= '{start_time.isoformat()}'
                ORDER BY TIMESTAMP
            """
            
            ad_results = snowflake_client.execute_query(query)
            
            # Format results
            ad_data = []
            for row in ad_results:
                ad_data.append({
                    'timestamp': row[0].isoformat() if isinstance(row[0], datetime) else row[0],
                    'type': row[1],
                    'impressions': row[2],
                    'clicks': row[3],
                    'ctr': row[4],
                    'cost': row[5],
                    'conversions': row[6],
                    'cost_per_conversion': row[7]
                })
            
            result['ads'] = ad_data
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error exporting data for Power BI: {e}")
        return jsonify({
            'error': str(e)
        }), 500

def start_api_server(mongodb_client_instance, snowflake_client_instance, firebase_client_instance):
    """
    Start the Flask API server in a separate thread
    
    Args:
        mongodb_client_instance: MongoDB client
        snowflake_client_instance: Snowflake client
        firebase_client_instance: Firebase client
    """
    global mongodb_client, snowflake_client, firebase_client
    
    # Set global client instances
    mongodb_client = mongodb_client_instance
    snowflake_client = snowflake_client_instance
    firebase_client = firebase_client_instance
    
    # Start Flask in a separate thread
    def run_flask():
        app.run(host='0.0.0.0', port=5001)
    
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    logger.info("Started Flask API server on port 5000")