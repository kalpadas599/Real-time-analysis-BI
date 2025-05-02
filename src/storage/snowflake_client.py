import logging
import pandas as pd
from snowflake.connector import connect
from snowflake.connector.errors import OperationalError, DatabaseError
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class SnowflakeClient:
    """Client for interacting with Snowflake data warehouse."""
    
    def __init__(self, account=None, user=None, password=None, database='CRICKET_ANALYTICS', schema='PUBLIC'):
        """
        Initialize Snowflake client.
        
        Args:
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            database: Database name
            schema: Schema name
        """
        load_dotenv()
        
        self.account = account or os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = user or os.getenv('SNOWFLAKE_USER')
        self.password = password or os.getenv('SNOWFLAKE_PASSWORD')
        self.database = database
        self.schema = schema
        self.conn = None
        
        try:
            # Add insecure_mode=True to bypass certificate validation (for development only)
            self.conn = connect(
                user=self.user,
                password=self.password,
                account=self.account,
                database=self.database,
                schema=self.schema,
                insecure_mode=True,  # Add this parameter to bypass certificate validation
                ocsp_response_cache_filename=None  # Disable OCSP cache
            )
            logger.info(f"Connected to Snowflake database: {self.database}")
            
            # Create database and tables if they don't exist
            self._initialize_database()
        except OperationalError as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            logger.warning("Continuing without Snowflake connection. Some features may be limited.")
            self.conn = None
        except Exception as e:
            logger.error(f"Unexpected error connecting to Snowflake: {str(e)}")
            logger.warning("Continuing without Snowflake connection. Some features may be limited.")
            self.conn = None
    
    def _initialize_database(self):
        """Create database and tables if they don't exist."""
        if not self.conn:
            logger.warning("Cannot initialize Snowflake database: No connection")
            return
            
        try:
            cursor = self.conn.cursor()
            
            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE DATABASE {self.database}")
            
            # Create schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            # Create tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS VIEWERSHIP (
                    id VARCHAR(255) PRIMARY KEY,
                    timestamp TIMESTAMP_NTZ,
                    match_id VARCHAR(255),
                    total_viewers INTEGER,
                    region VARCHAR(255),
                    platform VARCHAR(255),
                    avg_watch_time FLOAT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS SENTIMENT (
                    id VARCHAR(255) PRIMARY KEY,
                    timestamp TIMESTAMP_NTZ,
                    match_id VARCHAR(255),
                    source VARCHAR(255),
                    text TEXT,
                    compound FLOAT,
                    positive FLOAT,
                    neutral FLOAT,
                    negative FLOAT,
                    region VARCHAR(255)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS AD_PERFORMANCE (
                    id VARCHAR(255) PRIMARY KEY,
                    timestamp TIMESTAMP_NTZ,
                    match_id VARCHAR(255),
                    ad_id VARCHAR(255),
                    type VARCHAR(255),
                    impressions INTEGER,
                    clicks INTEGER,
                    conversions INTEGER,
                    cost FLOAT,
                    ctr FLOAT
                )
            """)
            
            cursor.close()
            logger.info("Snowflake database initialized successfully")
        except (OperationalError, DatabaseError) as e:
            logger.error(f"Failed to initialize Snowflake database: {str(e)}")
    
    def store_viewership_data(self, data):
        """
        Store viewership data in Snowflake.
        
        Args:
            data: Dictionary containing viewership data
        """
        if not self.conn:
            logger.warning("Cannot store viewership data: No Snowflake connection")
            return False
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO VIEWERSHIP (id, timestamp, match_id, total_viewers, region, platform, avg_watch_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                data['id'],
                data['timestamp'],
                data['match_id'],
                data['total_viewers'],
                data['region'],
                data['platform'],
                data['avg_watch_time']
            ))
            cursor.close()
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to store viewership data in Snowflake: {str(e)}")
            return False
    
    def store_sentiment_data(self, data):
        """
        Store sentiment analysis data in Snowflake.
        
        Args:
            data: Dictionary containing sentiment data
        """
        if not self.conn:
            logger.warning("Cannot store sentiment data: No Snowflake connection")
            return False
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO SENTIMENT (id, timestamp, match_id, source, text, compound, positive, neutral, negative, region)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['id'],
                data['timestamp'],
                data['match_id'],
                data['source'],
                data['text'],
                data['compound'],
                data['positive'],
                data['neutral'],
                data['negative'],
                data['region']
            ))
            cursor.close()
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to store sentiment data in Snowflake: {str(e)}")
            return False
    
    def store_ad_performance_data(self, data):
        """
        Store ad performance data in Snowflake.
        
        Args:
            data: Dictionary containing ad performance data
        """
        if not self.conn:
            logger.warning("Cannot store ad performance data: No Snowflake connection")
            return False
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO AD_PERFORMANCE (id, timestamp, match_id, ad_id, type, impressions, clicks, conversions, cost, ctr)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['id'],
                data['timestamp'],
                data['match_id'],
                data['ad_id'],
                data['type'],
                data['impressions'],
                data['clicks'],
                data['conversions'],
                data['cost'],
                data['ctr']
            ))
            cursor.close()
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to store ad performance data in Snowflake: {str(e)}")
            return False
    
    def query_data(self, query):
        """
        Execute a query on Snowflake.
        
        Args:
            query: SQL query string
            
        Returns:
            Pandas DataFrame with query results or None if query fails
        """
        if not self.conn:
            logger.warning("Cannot execute query: No Snowflake connection")
            return None
            
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            logger.error(f"Failed to execute query on Snowflake: {str(e)}")
            return None
    
    def close(self):
        """Close the Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")
