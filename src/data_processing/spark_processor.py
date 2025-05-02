# Apache Spark processor for data processing

import logging
import threading
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, MapType, BooleanType

logger = logging.getLogger(__name__)

class SparkProcessor:
    """Processor for streaming data using Apache Spark"""
    
    def __init__(self, mongodb_client, snowflake_client, firebase_client, sentiment_analyzer):
        """
        Initialize Spark processor
        
        Args:
            mongodb_client: MongoDB client
            snowflake_client: Snowflake client
            firebase_client: Firebase client
            sentiment_analyzer: Sentiment analyzer
        """
        self.mongodb_client = mongodb_client
        self.snowflake_client = snowflake_client
        self.firebase_client = firebase_client
        self.sentiment_analyzer = sentiment_analyzer
        
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("Cricket Analytics") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .config("spark.mongodb.input.uri", mongodb_client.connection_string) \
            .config("spark.mongodb.output.uri", mongodb_client.connection_string) \
            .getOrCreate()
        
        logger.info("Initialized Spark session")
        
        # Define schemas for Kafka messages
        self._define_schemas()
        
        # Start processing threads
        self._start_processing_threads()
    
    def _define_schemas(self):
        """Define schemas for Kafka messages"""
        # Twitter data schema
        self.twitter_schema = StructType([
            StructField("id", StringType()),
            StructField("text", StringType()),
            StructField("created_at", StringType()),
            StructField("lang", StringType()),
            StructField("metrics", MapType(StringType(), IntegerType())),
            StructField("user", StructType([
                StructField("id", StringType()),
                StructField("name", StringType()),
                StructField("username", StringType()),
                StructField("location", StringType()),
                StructField("verified", BooleanType()),
                StructField("profile_image_url", StringType())
            ])),
            StructField("source", StringType()),
            StructField("keywords", ArrayType(StringType())),
            StructField("processed_at", StringType())
        ])
        
        # Viewership data schema
        self.viewership_schema = StructType([
            StructField("timestamp", StringType()),
            StructField("total_viewers", IntegerType()),
            StructField("regional_data", ArrayType(StructType([
                StructField("region", StringType()),
                StructField("viewers", IntegerType()),
                StructField("engagement_rate", FloatType())
            ]))),
            StructField("platform_data", ArrayType(StructType([
                StructField("platform", StringType()),
                StructField("viewers", IntegerType()),
                StructField("buffer_rate", FloatType()),
                StructField("avg_resolution", StringType())
            ])))
        ])
        
        # Ad data schema
        self.ad_schema = StructType([
            StructField("timestamp", StringType()),
            StructField("ads", ArrayType(StructType([
                StructField("type", StringType()),
                StructField("impressions", IntegerType()),
                StructField("clicks", IntegerType()),
                StructField("ctr", FloatType()),
                StructField("cost", FloatType()),
                StructField("conversions", IntegerType()),
                StructField("cost_per_conversion", FloatType()),
                StructField("regions", ArrayType(StructType([
                    StructField("region", StringType()),
                    StructField("impressions", IntegerType()),
                    StructField("clicks", IntegerType()),
                    StructField("ctr", FloatType())
                ])))
            ])))
        ])
    
    def _start_processing_threads(self):
        """Start threads for processing different data streams"""
        # Start Twitter data processing thread
        twitter_thread = threading.Thread(
            target=self._process_twitter_data,
            daemon=True
        )
        twitter_thread.start()
        
        # Start viewership data processing thread
        viewership_thread = threading.Thread(
            target=self._process_viewership_data,
            daemon=True
        )
        viewership_thread.start()
        
        # Start ad data processing thread
        ad_thread = threading.Thread(
            target=self._process_ad_data,
            daemon=True
        )
        ad_thread.start()
        
        logger.info("Started all processing threads")
    
    def _process_twitter_data(self):
        """Process Twitter data stream"""
        logger.info("Starting Twitter data processing")
        
        try:
            # Read from Kafka
            twitter_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "twitter-data") \
                .option("startingOffsets", "latest") \
                .load() \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), self.twitter_schema).alias("data")) \
                .select("data.*")
            
            # Process tweets
            processed_df = twitter_df.withColumn(
                "sentiment", self.sentiment_analyzer.analyze_sentiment_udf(col("text"))
            )
            
            # Write to MongoDB
            query1 = processed_df \
                .writeStream \
                .foreachBatch(self._write_tweets_to_mongodb) \
                .outputMode("append") \
                .start()
            
            # Aggregate sentiment by time window
            sentiment_agg = processed_df \
                .withWatermark("created_at", "10 minutes") \
                .groupBy(window(col("created_at"), "5 minutes")) \
                .agg({
                    "sentiment.positive": "avg",
                    "sentiment.neutral": "avg",
                    "sentiment.negative": "avg",
                    "sentiment.compound": "avg"
                }) \
                .withColumnRenamed("avg(sentiment.positive)", "avg_positive") \
                .withColumnRenamed("avg(sentiment.neutral)", "avg_neutral") \
                .withColumnRenamed("avg(sentiment.negative)", "avg_negative") \
                .withColumnRenamed("avg(sentiment.compound)", "avg_compound")
            
            # Write aggregated sentiment to Snowflake
            query2 = sentiment_agg \
                .writeStream \
                .foreachBatch(self._write_sentiment_to_snowflake) \
                .outputMode("append") \
                .start()
            
            # Write latest sentiment to Firebase for real-time updates
            query3 = sentiment_agg \
                .writeStream \
                .foreachBatch(self._write_sentiment_to_firebase) \
                .outputMode("complete") \
                .start()
            
            # Keep the queries running
            query1.awaitTermination()
            query2.awaitTermination()
            query3.awaitTermination()
        
        except Exception as e:
            logger.error(f"Error processing Twitter data: {e}")
    
    def _process_viewership_data(self):
        """Process viewership data stream"""
        logger.info("Starting viewership data processing")
        
        try:
            # Read from Kafka
            viewership_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "viewership-data") \
                .option("startingOffsets", "latest") \
                .load() \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), self.viewership_schema).alias("data")) \
                .select("data.*")
            
            # Write raw viewership data to MongoDB
            query1 = viewership_df \
                .writeStream \
                .foreachBatch(self._write_viewership_to_mongodb) \
                .outputMode("append") \
                .start()
            
            # Process regional data
            regional_df = viewership_df \
                .select(col("timestamp"), explode(col("regional_data")).alias("region_data")) \
                .select(
                    col("timestamp"),
                    col("region_data.region"),
                    col("region_data.viewers"),
                    col("region_data.engagement_rate")
                )
            
            # Write regional data to Snowflake
            query2 = regional_df \
                .writeStream \
                .foreachBatch(self._write_regional_to_snowflake) \
                .outputMode("append") \
                .start()
            
            # Process platform data
            platform_df = viewership_df \
                .select(col("timestamp"), explode(col("platform_data")).alias("platform_data")) \
                .select(
                    col("timestamp"),
                    col("platform_data.platform"),
                    col("platform_data.viewers"),
                    col("platform_data.buffer_rate"),
                    col("platform_data.avg_resolution")
                )
            
            # Write platform data to Snowflake
            query3 = platform_df \
                .writeStream \
                .foreachBatch(self._write_platform_to_snowflake) \
                .outputMode("append") \
                .start()
            
            # Write latest viewership data to Firebase for real-time updates
            query4 = viewership_df \
                .writeStream \
                .foreachBatch(self._write_viewership_to_firebase) \
                .outputMode("complete") \
                .start()
            
            # Keep the queries running
            query1.awaitTermination()
            query2.awaitTermination()
            query3.awaitTermination()
            query4.awaitTermination()
        
        except Exception as e:
            logger.error(f"Error processing viewership data: {e}")
    
    def _process_ad_data(self):
        """Process ad data stream"""
        logger.info("Starting ad data processing")
        
        try:
            # Read from Kafka
            ad_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "ad-data") \
                .option("startingOffsets", "latest") \
                .load() \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), self.ad_schema).alias("data")) \
                .select("data.*")
            
            # Write raw ad data to MongoDB
            query1 = ad_df \
                .writeStream \
                .foreachBatch(self._write_ad_data_to_mongodb) \
                .outputMode("append") \
                .start()
            
            # Process ad data
            processed_ad_df = ad_df \
                .select(col("timestamp"), explode(col("ads")).alias("ad")) \
                .select(
                    col("timestamp"),
                    col("ad.type"),
                    col("ad.impressions"),
                    col("ad.clicks"),
                    col("ad.ctr"),
                    col("ad.cost"),
                    col("ad.conversions"),
                    col("ad.cost_per_conversion")
                )
            
            # Write processed ad data to Snowflake
            query2 = processed_ad_df \
                .writeStream \
                .foreachBatch(self._write_ad_data_to_snowflake) \
                .outputMode("append") \
                .start()
            
            # Process regional ad data
            regional_ad_df = ad_df \
                .select(col("timestamp"), explode(col("ads")).alias("ad")) \
                .select(
                    col("timestamp"),
                    col("ad.type"),
                    explode(col("ad.regions")).alias("region_data")
                ) \
                .select(
                    col("timestamp"),
                    col("type"),
                    col("region_data.region"),
                    col("region_data.impressions"),
                    col("region_data.clicks"),
                    col("region_data.ctr")
                )
            
            # Write regional ad data to Snowflake
            query3 = regional_ad_df \
                .writeStream \
                .foreachBatch(self._write_regional_ad_data_to_snowflake) \
                .outputMode("append") \
                .start()
            
            # Write latest ad data to Firebase for real-time updates
            query4 = processed_ad_df \
                .writeStream \
                .foreachBatch(self._write_ad_data_to_firebase) \
                .outputMode("complete") \
                .start()
            
            # Keep the queries running
            query1.awaitTermination()
            query2.awaitTermination()
            query3.awaitTermination()
            query4.awaitTermination()
        
        except Exception as e:
            logger.error(f"Error processing ad data: {e}")
    
    def _write_tweets_to_mongodb(self, batch_df, batch_id):
        """Write tweets to MongoDB"""
        try:
            # Convert DataFrame to list of dictionaries
            tweets = batch_df.toPandas().to_dict("records")
            
            # Write to MongoDB
            if tweets:
                self.mongodb_client.insert_many("tweets", tweets)
                logger.info(f"Wrote {len(tweets)} tweets to MongoDB")
        except Exception as e:
            logger.error(f"Error writing tweets to MongoDB: {e}")
    
    def _write_sentiment_to_snowflake(self, batch_df, batch_id):
        """Write sentiment data to Snowflake"""
        try:
            # Convert DataFrame to list of dictionaries
            sentiment_data = batch_df.toPandas().to_dict("records")
            
            # Write to Snowflake
            if sentiment_data:
                self.snowflake_client.insert_sentiment_data(sentiment_data)
                logger.info(f"Wrote {len(sentiment_data)} sentiment records to Snowflake")
        except Exception as e:
            logger.error(f"Error writing sentiment to Snowflake: {e}")
    
    def _write_sentiment_to_firebase(self, batch_df, batch_id):
        """Write latest sentiment to Firebase"""
        try:
            # Get the latest sentiment data
            if not batch_df.isEmpty():
                latest = batch_df.orderBy(col("window.end").desc()).first()
                
                if latest:
                    # Convert to dictionary
                    sentiment_data = {
                        "timestamp": latest["window.end"].isoformat(),
                        "positive": float(latest["avg_positive"]),
                        "neutral": float(latest["avg_neutral"]),
                        "negative": float(latest["avg_negative"]),
                        "compound": float(latest["avg_compound"])
                    }
                    
                    # Write to Firebase
                    self.firebase_client.update("sentiment", sentiment_data)
                    logger.info("Wrote latest sentiment to Firebase")
        except Exception as e:
            logger.error(f"Error writing sentiment to Firebase: {e}")
    
    def _write_viewership_to_mongodb(self, batch_df, batch_id):
        """Write viewership data to MongoDB"""
        try:
            # Convert DataFrame to list of dictionaries
            viewership_data = batch_df.toPandas().to_dict("records")
            
            # Write to MongoDB
            if viewership_data:
                self.mongodb_client.insert_many("viewership", viewership_data)
                logger.info(f"Wrote {len(viewership_data)} viewership records to MongoDB")
        except Exception as e:
            logger.error(f"Error writing viewership to MongoDB: {e}")
    
    def _write_regional_to_snowflake(self, batch_df, batch_id):
        """Write regional viewership data to Snowflake"""
        try:
            # Convert DataFrame to list of dictionaries
            regional_data = batch_df.toPandas().to_dict("records")
            
            # Write to Snowflake
            if regional_data:
                self.snowflake_client.insert_regional_data(regional_data)
                logger.info(f"Wrote {len(regional_data)} regional records to Snowflake")
        except Exception as e:
            logger.error(f"Error writing regional data to Snowflake: {e}")
    
    def _write_platform_to_snowflake(self, batch_df, batch_id):
        """Write platform viewership data to Snowflake"""
        try:
            # Convert DataFrame to list of dictionaries
            platform_data = batch_df.toPandas().to_dict("records")
            
            # Write to Snowflake
            if platform_data:
                self.snowflake_client.insert_platform_data(platform_data)
                logger.info(f"Wrote {len(platform_data)} platform records to Snowflake")
        except Exception as e:
            logger.error(f"Error writing platform data to Snowflake: {e}")
    
    def _write_viewership_to_firebase(self, batch_df, batch_id):
        """Write latest viewership data to Firebase"""
        try:
            # Get the latest viewership data
            if not batch_df.isEmpty():
                latest = batch_df.orderBy(col("timestamp").desc()).first()
                
                if latest:
                    # Convert to dictionary
                    viewership_data = {
                        "timestamp": latest["timestamp"],
                        "total_viewers": int(latest["total_viewers"]),
                        "regional_data": [
                            {
                                "region": region["region"],
                                "viewers": int(region["viewers"]),
                                "engagement_rate": float(region["engagement_rate"])
                            }
                            for region in latest["regional_data"]
                        ],
                        "platform_data": [
                            {
                                "platform": platform["platform"],
                                "viewers": int(platform["viewers"]),
                                "buffer_rate": float(platform["buffer_rate"]),
                                "avg_resolution": platform["avg_resolution"]
                            }
                            for platform in latest["platform_data"]
                        ]
                    }
                    
                    # Write to Firebase
                    self.firebase_client.update("viewership", viewership_data)
                    logger.info("Wrote latest viewership to Firebase")
        except Exception as e:
            logger.error(f"Error writing viewership to Firebase: {e}")
    
    def _write_ad_data_to_mongodb(self, batch_df, batch_id):
        """Write ad data to MongoDB"""
        try:
            # Convert DataFrame to list of dictionaries
            ad_data = batch_df.toPandas().to_dict("records")
            
            # Write to MongoDB
            if ad_data:
                self.mongodb_client.insert_many("ads", ad_data)
                logger.info(f"Wrote {len(ad_data)} ad records to MongoDB")
        except Exception as e:
            logger.error(f"Error writing ad data to MongoDB: {e}")
    
    def _write_ad_data_to_snowflake(self, batch_df, batch_id):
        """Write ad data to Snowflake"""
        try:
            # Convert DataFrame to list of dictionaries
            ad_data = batch_df.toPandas().to_dict("records")
            
            # Write to Snowflake
            if ad_data:
                self.snowflake_client.insert_ad_data(ad_data)
                logger.info(f"Wrote {len(ad_data)} ad records to Snowflake")
        except Exception as e:
            logger.error(f"Error writing ad data to Snowflake: {e}")
    
    def _write_regional_ad_data_to_snowflake(self, batch_df, batch_id):
        """Write regional ad data to Snowflake"""
        try:
            # Convert DataFrame to list of dictionaries
            regional_ad_data = batch_df.toPandas().to_dict("records")
            
            # Write to Snowflake
            if regional_ad_data:
                self.snowflake_client.insert_regional_ad_data(regional_ad_data)
                logger.info(f"Wrote {len(regional_ad_data)} regional ad records to Snowflake")
        except Exception as e:
            logger.error(f"Error writing regional ad data to Snowflake: {e}")
    
    def _write_ad_data_to_firebase(self, batch_df, batch_id):
        """Write latest ad data to Firebase"""
        try:
            # Get the latest ad data
            if not batch_df.isEmpty():
                # Group by ad type and get the latest for each type
                latest_by_type = batch_df.groupBy("type").agg({
                    "timestamp": "max",
                    "impressions": "sum",
                    "clicks": "sum",
                    "ctr": "avg",
                    "cost": "sum",
                    "conversions": "sum",
                    "cost_per_conversion": "avg"
                })
                
                if not latest_by_type.isEmpty():
                    # Convert to dictionary
                    ad_data = {
                        "timestamp": latest_by_type.first()["max(timestamp)"],
                        "ads": [
                            {
                                "type": row["type"],
                                "impressions": int(row["sum(impressions)"]),
                                "clicks": int(row["sum(clicks)"]),
                                "ctr": float(row["avg(ctr)"]),
                                "cost": float(row["sum(cost)"]),
                                "conversions": int(row["sum(conversions)"]),
                                "cost_per_conversion": float(row["avg(cost_per_conversion)"])
                            }
                            for row in latest_by_type.collect()
                        ]
                    }
                    
                    # Write to Firebase
                    self.firebase_client.update("ads", ad_data)
                    logger.info("Wrote latest ad data to Firebase")
        except Exception as e:
            logger.error(f"Error writing ad data to Firebase: {e}")
    
    def stop(self):
        """Stop the Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Stopped Spark session")